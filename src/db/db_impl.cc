#include "db_impl.h"
#include <unistd.h>
#include <chrono>

namespace cruzdb {

DBImpl::DBImpl(zlog::Log *log, const RestorePoint& point) :
  log_(log),
  cache_(log, this),
  stop_(false),
  entry_service_(log, point.replay_start_pos, &lock_),
  intention_queue_(entry_service_.NewIntentionQueue(point.replay_start_pos)),
  in_flight_txn_rid_(-1),
  root_(Node::Nil(), this)
{
  auto root = cache_.CacheAfterImage(point.after_image, point.after_image_pos);
  root_.replace(root);

  root_snapshot_ = point.after_image.intention();
  last_intention_processed_ = root_snapshot_;

  transaction_processor_thread_ = std::thread(&DBImpl::TransactionProcessorEntry, this);
  afterimage_writer_thread_ = std::thread(&DBImpl::AfterImageWriterEntry, this);
  afterimage_finalizer_thread_ = std::thread(&DBImpl::AfterImageFinalizerEntry, this);

  janitor_thread_ = std::thread(&DBImpl::JanitorEntry, this);
}

DBImpl::~DBImpl()
{
  {
    std::lock_guard<std::mutex> l(lock_);
    stop_ = true;
  }

  janitor_cond_.notify_one();
  janitor_thread_.join();

  entry_service_.Stop();

  unwritten_roots_cond_.notify_one();

  transaction_processor_thread_.join();
  afterimage_writer_thread_.join();
  afterimage_finalizer_thread_.join();

  cache_.Stop();
}

Snapshot *DBImpl::GetSnapshot()
{
  std::lock_guard<std::mutex> l(lock_);
  return new Snapshot(this, root_);
}

void DBImpl::ReleaseSnapshot(Snapshot *snapshot)
{
  delete snapshot;
}

Iterator *DBImpl::NewIterator(Snapshot *snapshot)
{
  return new IteratorImpl(snapshot);
}

int DBImpl::FindRestorePoint(zlog::Log *log, RestorePoint& point,
    uint64_t& latest_intention)
{
  uint64_t pos;
  int ret = log->CheckTail(&pos);
  assert(ret == 0);

  // log is empty?
  if (pos == 0) {
    return -EINVAL;
  }

  // intention_pos -> earliest (ai_pos, ai_blob)
  std::unordered_map<uint64_t,
    std::pair<uint64_t, cruzdb_proto::AfterImage>> after_images;

  bool set_latest_intention = false;

  // scan log in reverse order
  while (true) {
    std::string data;
    ret = log->Read(pos, &data);
    if (ret < 0) {
      if (ret == -ENOENT) {
        // see issue github #33
        assert(pos > 0);
        pos--;
        continue;
      }
      assert(0);
    }

    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());

    switch (entry.type()) {
      case cruzdb_proto::LogEntry::INTENTION:
       {
         if (!set_latest_intention) {
           latest_intention = pos;
           set_latest_intention = true;
         }

         auto it = after_images.find(pos);
         if (it != after_images.end()) {
           // found a starting point, but still need to guarantee that the
           // decision will remain valid: see github #33.
           point.replay_start_pos = pos + 1;
           point.after_image_pos = it->second.first;
           point.after_image = it->second.second;
           assert(it->first == it->second.second.intention());
           return 0;
         }
       }
       break;

       // find the oldest version of each after image
      case cruzdb_proto::LogEntry::AFTER_IMAGE:
        {
          assert(pos > 0);
          auto ai = entry.after_image();
          auto it = after_images.find(ai.intention());
          if (it != after_images.end()) {
            after_images.erase(it);
          }
          auto ret = after_images.emplace(ai.intention(), std::make_pair(pos, ai));
          assert(ret.second);
        }
        break;

      default:
        assert(0);
        exit(1);
    }

    assert(pos > 0);
    pos--;
  }
  assert(0);
  exit(1);
}

int DBImpl::Validate(const SharedNodeRef root)
{
  assert(root != nullptr);

  if (root == Node::Nil())
    return 1;

  assert(root->left.ref_notrace());
  assert(root->right.ref_notrace());

  SharedNodeRef ln = root->left.ref_notrace();
  SharedNodeRef rn = root->right.ref_notrace();

  if (root->red() && (ln->red() || rn->red()))
    return 0;

  int lh = Validate(ln);
  int rh = Validate(rn);

  if ((ln != Node::Nil() && ln->key().compare(root->key()) >= 0) ||
      (rn != Node::Nil() && rn->key().compare(root->key()) <= 0))
    return 0;

  if (lh != 0 && rh != 0 && lh != rh)
    return 0;

  if (lh != 0 && rh != 0)
    return root->red() ? lh : lh + 1;

  return 0;
}

void DBImpl::Validate()
{
  auto snapshot = root_;
  bool valid = Validate(snapshot.ref_notrace()) != 0;
  assert(valid);
}

void DBImpl::UpdateLRU(std::vector<NodeAddress>& trace)
{
  cache_.UpdateLRU(trace);
}

boost::optional<uint64_t> DBImpl::IntentionToAfterImage(uint64_t intention_pos)
{
  return cache_.IntentionToAfterImage(intention_pos);
}

SharedNodeRef DBImpl::fetch(std::vector<NodeAddress>& trace,
    boost::optional<NodeAddress>& address)
{
  return cache_.fetch(trace, address);
}

int DBImpl::Get(const zlog::Slice& key, std::string *value)
{
  std::vector<NodeAddress> trace;
  auto snap = GetSnapshot();
  auto root = snap->root;

  auto cur = root.ref(trace);
  while (cur != Node::Nil()) {
    int cmp = key.compare(zlog::Slice(cur->key().data(),
          cur->key().size()));
    if (cmp == 0) {
      value->assign(cur->val().data(), cur->val().size());
      UpdateLRU(trace);
      return 0;
    }
    cur = cmp < 0 ? cur->left.ref(trace) :
      cur->right.ref(trace);
  }
  UpdateLRU(trace);
  return -ENOENT;
}

Transaction *DBImpl::BeginTransaction()
{
  std::lock_guard<std::mutex> lk(lock_);
  return new TransactionImpl(this,
      root_,
      root_snapshot_,
      in_flight_txn_rid_--,
      txn_finder_.NewToken());
}

void DBImpl::WaitOnIntention(uint64_t pos)
{
  bool done = false;
  std::condition_variable cond;
  std::unique_lock<std::mutex> lk(lock_);
  if (pos <= last_intention_processed_) {
    return;
  }
  waiting_on_log_entry_.emplace(pos, std::make_pair(&cond, &done));
  cond.wait(lk, [&] { return done; });
}

void DBImpl::NotifyIntention(uint64_t pos)
{
  auto first = waiting_on_log_entry_.begin();
  auto last = waiting_on_log_entry_.upper_bound(pos);
  std::for_each(first, last, [pos](auto& w) {
    assert(w.first <= pos);
    *w.second.second = true;
    w.second.first->notify_one();
  });
  waiting_on_log_entry_.erase(first, last);
}

bool DBImpl::ProcessConcurrentIntention(const Intention& intention)
{
  // set of keys read or written by the intention
  auto intention_keys = intention.OpKeys();

  // retrieve all of the intentions in the conflict zone.
  std::vector<uint64_t> intentions;
  {
    auto snapshot = intention.Snapshot();
    assert(snapshot < root_snapshot_);

    auto first = intention_map_.find(snapshot);
    assert(first != intention_map_.end());

    auto last = intention_map_.find(root_snapshot_);
    assert(last != intention_map_.end());

    // converts [first, last) to (first, last] since the snapshot is not in the
    // conflict zone, and root_snapshot_ is.
    std::copy(std::next(first),
        std::next(last), std::back_inserter(intentions));
  }

  auto other_intentions = entry_service_.ReadIntentions(intentions);

  for (auto& other_intention : other_intentions) {
    // set of keys modified by the intention in the conflict zone
    auto other_intention_keys = other_intention.UpdateOpKeys();

    // return abort=true if the set of keys intersect
    for (auto k0 : intention_keys) {
      if (other_intention_keys.find(k0) !=
          other_intention_keys.end()) {
        return true;
      }
    }
  }

  return false;
}

void DBImpl::NotifyTransaction(int64_t token, uint64_t intention_pos,
    bool committed)
{
  NotifyIntention(intention_pos);
  txn_finder_.Notify(token, intention_pos, committed);
}

void DBImpl::ReplayIntention(PersistentTree *tree, const Intention& intention)
{
  for (auto op : intention) {
    switch (op.op()) {
      case cruzdb_proto::TransactionOp::GET:
        assert(!op.has_val());
        break;

      case cruzdb_proto::TransactionOp::PUT:
        assert(op.has_val());
        tree->Put(op.key(), op.val());
        break;

      case cruzdb_proto::TransactionOp::DELETE:
        assert(!op.has_val());
        tree->Delete(op.key());
        break;

      default:
        assert(0);
        exit(1);
    }
  }
}

void DBImpl::TransactionProcessorEntry()
{
  while (true) {
    // next intention to process
    const auto intention = intention_queue_->Wait();
    if (!intention) {
      break;
    }
    const auto intention_pos = intention->Position();

    // serial intention?
    assert(root_snapshot_ < intention_pos);
    const auto serial = root_snapshot_ == intention->Snapshot();

    // check for conflicts
    bool abort;
    if (serial) {
      abort = false;
    } else {
      abort = ProcessConcurrentIntention(*intention);
    }

    // abort: notify waiters before moving on
    if (abort) {
      std::lock_guard<std::mutex> lk(lock_);
      NotifyTransaction(intention->Token(), intention_pos, false);
      assert(last_intention_processed_ < intention_pos);
      last_intention_processed_ = intention_pos;
      continue;
    }

    // TODO: this records the intentions that have committed. during conflict
    // resolution we need access to arbitrary ranges of committed intention
    // positions, but we also can't let this grow unbounded. so we'll need to
    // store this out-of-core at some point.
    {
      auto ret = intention_map_.emplace(intention_pos);
      assert(ret.second);
    }

    bool need_replay;
    std::unique_ptr<PersistentTree> next_root;
    if (serial) {
      auto tmp = finished_txns_.Find(intention_pos);
      if (tmp) {
        next_root = std::move(tmp);
        assert(next_root);
        need_replay = false;
      } else {
        need_replay = true;
      }
    } else {
      need_replay = true;
    }

    boost::optional<int> root_offset;
    if (need_replay) {
      // really need this lock? it's really only here for the read of root_, but
      // that shouldn't need a lock since this thread is the only thread that
      // can write to root_.
      std::unique_lock<std::mutex> lk(lock_);
      next_root = std::make_unique<PersistentTree>(this, root_,
          static_cast<int64_t>(intention_pos),
          intention_pos);
      lk.unlock();
      ReplayIntention(next_root.get(), *intention);
      root_offset = next_root->infect_self_pointers(intention_pos, true);
    } else {
      // this also fixes up the rid. see method for details
      root_offset = next_root->infect_self_pointers(intention_pos, false);
    }
    assert(root_offset);

    assert(next_root->Root() != nullptr);
    NodePtr root(next_root->Root(), this);
    if (root_offset) {
      assert(next_root->Root() != Node::Nil());
      root.SetIntentionAddress(intention_pos, *root_offset);
    }

    std::unique_lock<std::mutex> lk(lock_);

    root_.replace(root);
    root_snapshot_ = intention_pos;

    assert(last_intention_processed_ < intention_pos);
    last_intention_processed_ = intention_pos;

    lcs_trees_.emplace_back(std::move(next_root));
    unwritten_roots_cond_.notify_one();

    NotifyTransaction(intention->Token(), intention_pos, true);
  }

  std::cerr << "transaction processor exiting" << std::endl;
}

void DBImpl::AfterImageWriterEntry()
{
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    std::unique_lock<std::mutex> lk(lock_);

    if (!unwritten_roots_cond_.wait_for(lk, std::chrono::seconds(10),
          [&] { return !lcs_trees_.empty() || stop_; })) {
      std::cout << "tw: no unwritten root produced for 2 seconds" << std::endl;
      continue;
    }

    if (stop_)
      break;

    std::vector<std::pair<std::string, std::unique_ptr<PersistentTree>>> trees;
    for (auto& tree : lcs_trees_) {
      trees.emplace_back("", std::move(tree));
    }
    lcs_trees_.clear();

    lk.unlock();

    for (auto& tree_info : trees) {
      auto tree = std::move(tree_info.second);
      const auto intention_pos = tree->Intention();

      std::vector<SharedNodeRef> delta;
      cruzdb_proto::AfterImage after_image;
      tree->SerializeAfterImage(after_image, intention_pos, delta);
      assert(after_image.intention() == intention_pos);

      std::string blob;
      cruzdb_proto::LogEntry entry;
      entry.set_type(cruzdb_proto::LogEntry::AFTER_IMAGE);
      entry.set_allocated_after_image(&after_image);
      assert(entry.IsInitialized());
      assert(entry.SerializeToString(&blob));
      entry.release_after_image();

      tree_info.first = blob;

      entry_service_.ai_matcher.watch(std::move(tree));
    }

    // this whole copy, shuffle, loop thing is mostly to just stress out the
    // rendezvous mechanism between this thread and the thread that adds the
    // afterimage to the cache. once we have actual aio here, this can all be
    // simplified / removed.
    //
    // note that above they are registered in log order with entry service, but
    // then the io is random. this is consistent with how we will register
    // directly as they pop off the queue, then dispatch aio
    std::random_shuffle(trees.begin(), trees.end());

    // ok, the serialization is done again down here to reproduce the blob. it
    // isn't removed from above, or move here, because we need to go through and
    // make sure that there isn't some side effects from serialization (like
    // setting the intention on the tree before registering) as that method
    // isn't const. in any case, it should be idempotent, so no harm, just
    // wasteful and silly.
    for (auto& tree_info : trees) {
      uint64_t afterimage_pos;
      int ret = log_->Append(tree_info.first, &afterimage_pos);
      assert(ret == 0);
    }
  }
}

void DBImpl::AfterImageFinalizerEntry()
{
  while (true) {
    auto tree = entry_service_.ai_matcher.match();
    if (!tree)
      break;

    auto ipos = tree->Intention();
    auto ai_pos = tree->AfterImage();

    std::vector<SharedNodeRef> delta;
    {
      cruzdb_proto::AfterImage after_image;
      tree->SerializeAfterImage(after_image, ipos, delta);
      assert(after_image.intention() == ipos);
    }

    std::unique_lock<std::mutex> lk(lock_);
    if (stop_)
      break;

    assert(ipos < ai_pos);
    tree->SetDeltaPosition(delta, ai_pos);
    cache_.SetIntentionMapping(ipos, ai_pos);
    cache_.ApplyAfterImageDelta(delta, ai_pos);
  }
}

bool DBImpl::CompleteTransaction(TransactionImpl *txn)
{
  // setup transaction rendezvous under this token
  const auto token = txn->Token();
  TransactionFinder::WaiterHandle waiter;
  txn_finder_.AddTokenWaiter(waiter, token);

  // MOVE txn's intention to the append io service
  uint64_t pos;
  auto ret = entry_service_.AppendIntention(
      std::move(txn->GetIntention()), &pos);
  assert(ret == 0);

  // MOVE txn's tree into index for txn processor
  auto tree = std::move(txn->Tree());
  tree->SetIntention(pos);
  finished_txns_.Insert(pos, std::move(tree));

  bool committed = txn_finder_.WaitOnTransaction(waiter, pos);

  return committed;
}

void DBImpl::TransactionFinder::AddTokenWaiter(
    WaiterHandle& whandle, uint64_t token)
{
  std::lock_guard<std::mutex> lk(lock_);

  auto& waiter = whandle.waiter;

  // register this waiter under the given token
  auto ret = token_waiters_.emplace(token, &waiter);
  if (!ret.second) {
    assert(ret.first->first == token);
    ret.first->second.waiters.emplace_back(&waiter);
  }

  // for quickly removing token entry
  whandle.token_it = ret.first;

  // for quickly removing waiter from rendezvous list
  whandle.waiter_it = ret.first->second.waiters.end();
  whandle.waiter_it--;
}

bool DBImpl::TransactionFinder::WaitOnTransaction(
    WaiterHandle& whandle, uint64_t intention_pos)
{
  std::unique_lock<std::mutex> lk(lock_);

  auto& waiter = whandle.waiter;

  // prepare to wait for the intention to be processed. we'll use the token to
  // rendezvous with the transaction processor. first enable fast path in which
  // the transaction processor finds the correct waiter, as the first waiter,
  // registered under this token.
  assert(!waiter.pos);
  waiter.pos = intention_pos;

  bool committed;
  while (true) {
    // successful fast path
    if (waiter.complete) {
      // tw_it is invalidated; removed during notification
      committed = waiter.committed;
      break;
    }

    // transaction processor woke up all waiters on this token because it
    // processed an intention and couldn't find a waiter with the target
    // intention position. this could also be a spurious wake-up.
    auto& results = whandle.token_it->second.results;
    auto it = results.find(intention_pos);
    if (it != results.end()) {
      committed = it->second;
      whandle.token_it->second.waiters.erase(whandle.waiter_it);
      results.erase(it);
      break;
    }

    waiter.cond.wait(lk);
  }

  // erase token entry if there are no associations
  if (whandle.token_it->second.waiters.empty() &&
      whandle.token_it->second.results.empty()) {
    token_waiters_.erase(whandle.token_it);
  }

  return committed;
}

void DBImpl::TransactionFinder::Notify(int64_t token,
    uint64_t intention_pos, bool committed)
{
  std::lock_guard<std::mutex> lk(lock_);

  // if a token is not found, then a different instance of the database produced
  // the transaction. in this case there are no waiters to notify.
  auto it = token_waiters_.find(token);
  if (it == token_waiters_.end())
    return;

  // at least one transaction is waiting under this token
  auto& tx = it->second;
  assert(!tx.waiters.empty());

  // fast path: first waiter is for this intention position
  auto fw = tx.waiters.front();
  if (fw->pos && fw->pos == intention_pos) {
    tx.waiters.pop_front();
    fw->complete = true;
    fw->committed = committed;
    fw->cond.notify_one();
  } else {
    // slow path. waiter didn't set the intention position before the
    // intention was processed, or wasn't first in line. record the result,
    // and let the waiters know they need to look for themselves. intentions
    // without waiters under this token should be periodically purged.
    auto ret = tx.results.emplace(intention_pos, committed);
    assert(ret.second);
    std::for_each(tx.waiters.begin(), tx.waiters.end(), [](auto w) {
      w->cond.notify_one();
    });
  }
}

std::unique_ptr<PersistentTree>
DBImpl::FinishedTransactions::Find(uint64_t ipos)
{
  std::lock_guard<std::mutex> lk(lock_);
  auto it = txns_.find(ipos);
  if (it != txns_.end()) {
    return std::move(it->second);
  }
  return nullptr;
}

void DBImpl::FinishedTransactions::Insert(uint64_t ipos,
    std::unique_ptr<PersistentTree> tree)
{
  std::lock_guard<std::mutex> lk(lock_);
  auto ret = txns_.emplace(ipos, std::move(tree));
  assert(ret.second);
}

void DBImpl::FinishedTransactions::Clean(uint64_t last_ipos)
{
  std::vector<std::unique_ptr<PersistentTree>> unused_trees;
  std::lock_guard<std::mutex> lk(lock_);
  auto it = txns_.begin();
  while (it != txns_.end()) {
    if (it->first <= last_ipos) {
      unused_trees.emplace_back(std::move(it->second));
      it = txns_.erase(it);
    } else {
      it++;
    }
  }
  // unused_trees destructor called after lock is released
}

void DBImpl::JanitorEntry()
{
  while (!stop_) {
    std::unique_lock<std::mutex> lk(lock_);
    janitor_cond_.wait_for(lk, std::chrono::seconds(1));
    lk.unlock();

    finished_txns_.Clean(last_intention_processed_);
  }
}

}
