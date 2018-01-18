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

  txn_writer_ = std::thread(&DBImpl::TransactionWriter, this);
  ai_writer_thread_ = std::thread(&DBImpl::AfterImageWriterEntry, this);
  txn_processor_ = std::thread(&DBImpl::TransactionProcessor, this);
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


  txn_processor_.join();
  txn_writer_.join();
  ai_writer_thread_.join();
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
  auto snapshot = intention.Snapshot();
  assert(snapshot < root_snapshot_); // concurrent property

  auto it = intention_map_.find(snapshot);
  assert(it != intention_map_.end());
  it++; // start immediately after snapshot

  // set of keys read or written by the intention
  std::set<std::string> intention_keys;
  for (auto op : intention) {
    intention_keys.insert(op.key());
  }

  while (true) {
    // next intention to examine for conflicts
    assert(it != intention_map_.end());
    uint64_t intent_pos = *it;

    // read intention from the log
    std::string data;
    int ret = log_->Read(intent_pos, &data);
    assert(ret == 0);
    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());
    assert(entry.type() == cruzdb_proto::LogEntry::INTENTION);

    // set of keys modified by the intention in the conflict zone
    const auto& other_intention = entry.intention();
    std::set<std::string> other_intention_keys;
    for (int i = 0; i < other_intention.ops_size(); i++) {
      auto& op = other_intention.ops(i);
      if (op.op() == cruzdb_proto::TransactionOp::PUT ||
          op.op() == cruzdb_proto::TransactionOp::DELETE) {
        other_intention_keys.insert(op.key());
      }
    }

    // return abort=true if the set of keys intersect
    for (auto k0 : intention_keys) {
      if (other_intention_keys.find(k0) !=
          other_intention_keys.end()) {
        return true;
      }
    }

    if (intent_pos == root_snapshot_)
      break;

    it++;
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

void DBImpl::TransactionProcessor()
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
    auto serial = root_snapshot_ == intention->Snapshot();

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

    // save the location of the next intention that committed. note for the
    // future: we are not holding any locks here because this thread has
    // execlusive access to this index.
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
          static_cast<int64_t>(intention_pos));
      lk.unlock();
      ReplayIntention(next_root.get(), *intention);
      root_offset = next_root->infect_self_pointers(intention_pos, true);
    } else {
      // this also fixes up the rid. see method for details
      root_offset = next_root->infect_self_pointers(intention_pos, false);
    }
    assert(root_offset);

    assert(next_root->root_ != nullptr);
    NodePtr root(next_root->root_, this);
    if (root_offset) {
      assert(next_root->root_ != Node::Nil());
      root.SetIntentionAddress(intention_pos, *root_offset);
    }

    std::unique_lock<std::mutex> lk(lock_);

    root_.replace(root);
    root_snapshot_ = intention_pos;

    assert(last_intention_processed_ < intention_pos);
    last_intention_processed_ = intention_pos;

    unwritten_roots_.emplace_back(intention_pos, std::move(next_root));
    unwritten_roots_cond_.notify_one();

    NotifyTransaction(intention->Token(), intention_pos, true);
  }

  std::cerr << "transaction processor exiting" << std::endl;
}

// PROBLEM: in order to write a after image we need to fill in the physical
// address for all the pointers in the after image. but that means that we
// need to have also written out the images that this image depends on. so
// in the end we basically  just have a serial read-write-update loop to be
// able to get these images out to the log.
//
// 1) this might be an OK intermediate step, but in general, that process of
// serializing, while it doesn't block new transactions, it does mean that
// memory will grow because we can't flush the uncached roots.
//
// 2) another option is to do something a little more exoct.
//
// 2.1) we could batch up a lot of after images into a single after image
// and do big writes that amatorizes that io cost over lots of after images.
//
// 2.2) we could write out of a bunch of after images in parallel without
// the phsyical address specified, and then write out an index, for a batch
// of them. but then how would random access work, perhaps the index is
// indexed somehow. maybe w back pointeres etc...
//
// 2.3) perhaps we ask the sequencer to reserve a bunch of spots for us and
// then we do the assignments assuming that it will succeed, and deal with
// io failures (that should be rare...) when they occur.
//
// 3) some hybrid combo of these where we select and combine strategies
//
// DECISION: some of these seem fairly reasonble. we should START with (1),
// the naive one, because it lets us make faster progress AND it also lets
// us play around with the new transaction creation throttling when memory
// pressure in the uncached roots gets high.
//
// ====== OLD NOTES BELOW =====
//
// when an after image is logged, we tag it with the position of the intention
// that was used to produce that after image.
//
// since an after image has to come after the intention, and an intention
// produces the in-memory uncached after image (from which the serialized
// verion we are processing was created), then the intention in question must
// be in this uncached_roots data structure.
//
// when we process an after image we can update the physical address of hte
// SELF nodes, mark that we know the address now, and then toss away the after
// image---it was only used to connect together an (intention, after image
// physicla address, in-memory after image). then we can keep processing.
//
// (note: this will also probably be sufficient for dealing with duplicates:
// if the pos isn't present, then its a duplicate and we just drop it).
//
// now the physical position can be used to update the node pointers in that
// in-memory after image (probably like SetDeltaPosition).
//
// BUT.... since those nodes without positions set were copied into new
// intentions, then we need to find those pointers as well and update them...,
// or at least create a structure that cna be used to find them like during
// serialization.
//
// take avantage of the fact that pointers only point backwards?
//
// 1. serialize uncached after image, and move to a new list to keep track of
// work completed.
//
// 2. when the log reader eventually processes the after image, we'll have a
// blob that need to be handled.
//
// 1
// when a new uncached root is created, we seialize it into the log. a policy
// must be established in the multi node case about who serializes which
// transactions, and when do other nodes try to take over for a slow node for
// a particular transaction in order to make progress in freeing uncached
// after images.
//
// 2
// when we encounter an after image in the log, we look to see if any uncached
// root (s) can be updated to reflect a new physical location.
//
// 3
// when an uncached after image has a physical location, we can fold it into
// the cache and release those resources from the uncached set.



void DBImpl::AfterImageWriterEntry()
{
  while (true) {
    std::unique_lock<std::mutex> lk(lock_);

    if (!unwritten_roots_cond_.wait_for(lk, std::chrono::seconds(10),
          [&] { return !unwritten_roots_.empty() || stop_; })) {
      std::cout << "tw: no unwritten root produced for 2 seconds" << std::endl;
      continue;
    }

    if (stop_)
      break;

    auto root_info = std::move(unwritten_roots_.front());
    unwritten_roots_.pop_front();
    lk.unlock();

    auto intention_pos = root_info.first;
    auto& tree = root_info.second;

    // serialization
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
    // cannot use entry after release...
    entry.release_after_image();

    // write it
    uint64_t afterimage_pos;
    int ret = log_->Append(blob, &afterimage_pos);
    assert(ret == 0);
    assert(intention_pos < afterimage_pos);

    lk.lock();
    unresolved_roots_.emplace_back(intention_pos, std::move(tree));
  }
}

// 1. after image cache needs to be structured in a way that it can be trimmed.
// currently it just bloats up.
void DBImpl::TransactionWriter()
{
  std::unordered_map<uint64_t,
    std::pair<uint64_t, cruzdb_proto::AfterImage>> after_images_cache;

  while (true) {
    std::unique_lock<std::mutex> lk(lock_);

    entry_service_.pending_after_images_cond_.wait(lk, [&] {
      return !entry_service_.pending_after_images_.empty() || stop_; });

    if (stop_)
      break;

    for (auto ai : entry_service_.pending_after_images_) {
      auto key = ai.second.intention();
      if (after_images_cache.find(key) == after_images_cache.end()) {
        assert(key >= 0);
        assert((uint64_t)key < ai.first);
        after_images_cache[key] = ai;
      }
    }
    entry_service_.pending_after_images_.clear();

    std::list<std::pair<uint64_t,
      std::unique_ptr<PersistentTree>>> roots;
    roots.swap(unresolved_roots_);
    lk.unlock();

    auto it = roots.begin();
    while (it != roots.end()) {
      auto intention_pos = it->first;

      auto it2 = after_images_cache.find(intention_pos);
      if (it2 == after_images_cache.end()) {
        it++;
        continue;
      }

      auto tree = std::move(it->second);
      it = roots.erase(it);

      // just a hack... should reuse previous calc of delta...
      std::vector<SharedNodeRef> delta;
      {
        cruzdb_proto::AfterImage after_image;
        tree->SerializeAfterImage(after_image, intention_pos, delta);
        assert(after_image.intention() == intention_pos);
      }

      lk.lock();
      assert(intention_pos < it2->second.first);
      tree->SetDeltaPosition(delta, it2->second.first);
      cache_.SetIntentionMapping(intention_pos, it2->second.first);
      cache_.ApplyAfterImageDelta(delta, it2->second.first);
      lk.unlock();
    }

    lk.lock();
    unresolved_roots_.splice(unresolved_roots_.end(), roots);
    lk.unlock();
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
  finished_txns_.Insert(pos, std::move(txn->Tree()));

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
