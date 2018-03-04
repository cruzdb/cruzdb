#include "db_impl.h"
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <boost/lexical_cast.hpp>
#include <spdlog/spdlog.h>

namespace cruzdb {

DBImpl::DBImpl(zlog::Log *log, const RestorePoint& point,
    std::unique_ptr<EntryService> entry_service,
    std::shared_ptr<spdlog::logger> logger) :
  cache_(log, this),
  stop_(false),
  entry_service_(std::move(entry_service)),
  intention_iterator_(entry_service_->NewIntentionIterator(point.replay_start_pos)),
  in_flight_txn_rid_(-1),
  root_(Node::Nil(), this),
#if 0
  metrics_http_server_({"listening_ports", "0.0.0.0:8080", "num_threads", "1"}),
#endif
  metrics_handler_(this),
  logger_(logger)
{
  entry_service_->Start(point.replay_start_pos);

  auto root = cache_.CacheAfterImage(*point.after_image, point.after_image_pos);
  root_ = root;

  root_snapshot_ = point.after_image->intention();
  last_intention_processed_ = root_snapshot_;

  if (logger_)
    logger_->info("db init i_pos {} ai_pos {}", root_snapshot_, point.after_image_pos);

  transaction_processor_thread_ = std::thread(&DBImpl::TransactionProcessorEntry, this);
  afterimage_writer_thread_ = std::thread(&DBImpl::AfterImageWriterEntry, this);
  afterimage_finalizer_thread_ = std::thread(&DBImpl::AfterImageFinalizerEntry, this);

  janitor_thread_ = std::thread(&DBImpl::JanitorEntry, this);

#if 0
  metrics_http_server_.addHandler("/metrics", &metrics_handler_);
#endif
}

DBImpl::~DBImpl()
{
  {
    std::lock_guard<std::mutex> l(lock_);
    stop_ = true;
  }

  janitor_cond_.notify_one();
  janitor_thread_.join();

  entry_service_->Stop();

  lcs_trees_cond_.notify_one();

  transaction_processor_thread_.join();
  afterimage_writer_thread_.join();
  afterimage_finalizer_thread_.join();

  cache_.Stop();
#if 0
  metrics_http_server_.removeHandler("/metrics");
  metrics_http_server_.close();
#endif
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
  return new FilteredPrefixIteratorImpl(PREFIX_USER, snapshot);
}

int DBImpl::FindRestorePoint(EntryService *entry_service, RestorePoint& point,
    uint64_t& latest_intention)
{
  // the true parameter tells the entry service to set max_pos according to the
  // tail returned. this is important, because if this is a new log then tail
  // will be zero pos, but the initialized log entries are present. since the
  // log scanner hasn't been started, the entry service is in an idle state.
  auto tail = entry_service->CheckTail(true);

  // log is empty?
  if (tail == 0) {
    return -EINVAL;
  }

  // intention_pos -> earliest (ai_pos, ai_blob)
  std::unordered_map<uint64_t,
    std::pair<uint64_t, std::shared_ptr<cruzdb_proto::AfterImage>>> after_images;

  bool set_latest_intention = false;

  auto it = entry_service->NewReverseIterator(tail);
  while (true) {
    auto entry = it.NextEntry(true);
    // if hole, skip. see github issue #33

    if (!entry)
      break;

    switch (entry->second.type) {
      case EntryService::CacheEntry::EntryType::INTENTION:
       {
         if (!set_latest_intention) {
           latest_intention = entry->first;
           set_latest_intention = true;
         }

         auto it = after_images.find(entry->first);
         if (it != after_images.end()) {
           // found a starting point, but still need to guarantee that the
           // decision will remain valid: see github #33.
           point.replay_start_pos = entry->first + 1;
           point.after_image_pos = it->second.first;
           point.after_image = it->second.second;
           assert(it->first == it->second.second->intention());
           return 0;
         }
       }
       break;

       // find the oldest version of each after image
      case EntryService::CacheEntry::EntryType::AFTERIMAGE:
        {
          assert(entry->first > 0);
          auto ai = entry->second.after_image;
          auto it = after_images.find(ai->intention());
          if (it != after_images.end()) {
            after_images.erase(it);
          }
          auto ret = after_images.emplace(ai->intention(),
              std::make_pair(entry->first, ai));
          assert(ret.second);
        }
        break;

      case EntryService::CacheEntry::EntryType::FILLED:
        break;

      default:
        assert(0);
        exit(1);
    }
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

  // FIXME: this string/slice/prefix append conversion can be more efficient.
  // probably a lot more efficient.
  auto pskey = prefix_string(PREFIX_USER, key.ToString());
  const zlog::Slice pkey(pskey);

  auto cur = root.ref(trace);
  while (cur != Node::Nil()) {
    int cmp = pkey.compare(zlog::Slice(cur->key().data(),
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
  db_stats_.transactions_started++;
  auto txn = new TransactionImpl(this,
      root_,
      root_snapshot_,
      in_flight_txn_rid_--,
      txn_finder_.NewToken());
  if (logger_)
    logger_->info("begin-txn snap {}", root_snapshot_);
  return txn;
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

  const auto snapshot = intention.Snapshot();
  auto irange = committed_intentions_.range(snapshot, root_snapshot_);
  if (!irange.second) {
    Snapshot snap(this, root_); // TODO: lock to read root_?
    FilteredPrefixIteratorImpl it(PREFIX_COMMITTED_INTENTION, &snap);

    std::stringstream key;
    key << std::setw(20) << std::setfill('0') << snapshot;

    it.Seek(key.str());
    assert(it.Valid());
    assert(boost::lexical_cast<uint64_t>(it.key().ToString()) == snapshot);

    it.Next();
    assert(it.Valid());

    while (it.Valid()) {
      auto key = it.key();
      auto pos = boost::lexical_cast<uint64_t>(key.ToString());
      if (pos == irange.first.front())
        break;
      irange.first.emplace_back(pos);
      it.Next();
    }
  }

  auto other_intentions = entry_service_->ReadIntentions(irange.first);

  for (auto& other_intention : other_intentions) {
    // set of keys modified by the intention in the conflict zone
    auto other_intention_keys = other_intention->UpdateOpKeys();

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
        tree->Delete(PREFIX_USER, op.key());
        break;

      case cruzdb_proto::TransactionOp::COPY:
        assert(!op.has_val());
        tree->Copy(op.key());
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
    const auto opt_intention = intention_iterator_.Next();
    if (!opt_intention) {
      break;
    }
    const auto intention = *opt_intention;
    const auto intention_pos = intention->Position();

    if (logger_)
      logger_->info("txn-proc: ipos {}", intention_pos);

    // serial intention? a flush intention is also treated like serial in that
    // it has no conflicts. be careful that serial doesn't examine anything in
    // the flush intention that might not be set given the flush intention's
    // special cases.
    assert(root_snapshot_ < intention_pos);
    const auto serial = root_snapshot_ == intention->Snapshot() ||
      intention->Flush();

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

    committed_intentions_.push(intention_pos);

    // committed intention key
    std::stringstream ci_key;
    ci_key << std::setw(20) << std::setfill('0') << intention_pos;

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
      next_root->Put(PREFIX_COMMITTED_INTENTION, ci_key.str(), "");
      root_offset = next_root->infect_self_pointers(intention_pos, true);
    } else {
      // first impressions are that this Put here really kills performance.
      // Persumably because its jsut overhead in a strictly serial process. It's
      // possible that we could store this information in a different way: for
      // example as just a bunch of backpointers. This would be slower for
      // finding than in the after image, but in both cases we are going to try
      // to aggressively cache these entries. We could optimize the LRU for this
      // index to keep the info around a long time and/or give preference to
      // this part of the tree in the node cache.
      next_root->Put(PREFIX_COMMITTED_INTENTION, ci_key.str(), "");
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

    root_ = root;
    root_snapshot_ = intention_pos;

    assert(last_intention_processed_ < intention_pos);
    last_intention_processed_ = intention_pos;

    lcs_trees_.emplace_back(std::move(next_root));
    lcs_trees_cond_.notify_one();

    NotifyTransaction(intention->Token(), intention_pos, true);
  }
}

// asynchronously dispatch after image serializations to the log
// TODO:
//  - throttle
//  - parallel serialization
//  - no multi-client writer policy
void DBImpl::AfterImageWriterEntry()
{
  std::unique_lock<std::mutex> lk(lock_);
  while (true) {
    lcs_trees_cond_.wait(lk, [&] {
        return !lcs_trees_.empty() || stop_; });

    if (stop_)
      break;

    std::list<std::unique_ptr<PersistentTree>> trees;
    trees.swap(lcs_trees_);
    lk.unlock();

    for (auto& tree : trees) {
      const auto intention_pos = tree->Intention();

      std::vector<SharedNodeRef> delta;
      cruzdb_proto::AfterImage after_image;
      tree->SerializeAfterImage(after_image, intention_pos, delta);
      assert(after_image.intention() == intention_pos);

      entry_service_->ai_matcher.watch(std::move(delta), std::move(tree));

      // in its current form, this isn't actually async because there is very
      // little, if any, benefit from actual AIO with the LMDB/RAM backends. for
      // for other backends the benefit is huge. other optimizations like not
      // writing the after image if we already know about it by looking at the
      // dedup index.
      entry_service_->Append(after_image);
    }

    lk.lock();
  }
}

void DBImpl::AfterImageFinalizerEntry()
{
  while (true) {
    auto tree_info = entry_service_->ai_matcher.match();
    if (!tree_info.second)
      break;

    auto& delta = tree_info.first;
    auto& tree = tree_info.second;

    auto ipos = tree->Intention();
    auto ai_pos = tree->AfterImage();

    if (logger_)
      logger_->info("ai-fini: ai_pos {}", ai_pos);

    assert(ipos < ai_pos);
    tree->SetDeltaPosition(delta, ai_pos);
    cache_.SetIntentionMapping(ipos, ai_pos);
    cache_.ApplyAfterImageDelta(delta, ai_pos);

    std::unique_lock<std::mutex> lk(lock_);
    if (stop_)
      break;
  }
}

bool DBImpl::CompleteTransaction(TransactionImpl *txn)
{
  // setup transaction rendezvous under this token
  const auto token = txn->Token();
  TransactionFinder::WaiterHandle waiter;
  txn_finder_.AddTokenWaiter(waiter, token);

  // MOVE txn's intention to the append io service
  auto pos = entry_service_->Append(std::move(txn->GetIntention()));

  // MOVE txn's tree into index for txn processor
  auto tree = std::move(txn->Tree());
  tree->SetIntention(pos);
  finished_txns_.Insert(pos, std::move(tree));

  bool committed = txn_finder_.WaitOnTransaction(waiter, pos);

  return committed;
}

// 1. when we do gc, we should also probably be removing entries from the
// committed intention index
//
// 2. it might be smart to use metadata to avoid copying nodes that were already
// moved forward through some other process after they were identified for gc.
//
// 3. the order of gc might actually matter depending on if we end up creating
// node copies where the children are further back in the log...
void DBImpl::gc()
{
  auto node = root_.ref_notrace();

  std::map<NodeAddress, std::string> addrs;
  std::stack<SharedNodeRef> stack;
  while (!stack.empty() || node != Node::Nil()) {
    if (node != Node::Nil()) {
      stack.push(node);
      node = node->left.ref_notrace();
    } else {
      node = stack.top();
      stack.pop();

      auto left_node = node->left.ref_notrace();
      if (left_node != Node::Nil()) {
        auto addr = node->left.Address();
        assert(addr);
        addrs.emplace(*addr, left_node->key().ToString());
        if (addrs.size() > 5) {
          addrs.erase(--addrs.end());
        }
      }

      auto right_node = node->right.ref_notrace();
      if (right_node != Node::Nil()) {
        auto addr = node->right.Address();
        assert(addr);
        addrs.emplace(*addr, right_node->key().ToString());
        if (addrs.size() > 5) {
          addrs.erase(--addrs.end());
        }
      }

      node = node->right.ref_notrace();
    }
  }

  auto flush = std::unique_ptr<Intention>(
      new Intention(0, 0));

  for (auto& addr : addrs) {
    std::cout << addr.first << "/" << addr.second << " ";
    flush->Copy(addr.second);
  }
  std::cout << std::endl;

  entry_service_->Append(std::move(flush));
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

void DBImpl::CommittedIntentionIndex::push(uint64_t pos)
{
  auto ret = index_.emplace(pos);
  assert(ret.second);
  assert(++ret.first == index_.end());
  if (index_.size() > limit_) {
    index_.erase(index_.begin());
  }
}

std::pair<std::vector<uint64_t>, bool>
DBImpl::CommittedIntentionIndex::range(uint64_t first, uint64_t last) const
{
  assert(first < last);

  if (index_.empty()) {
    return std::make_pair(std::vector<uint64_t>{{last}}, false);
  }

  // oldest position >= first
  auto it = index_.lower_bound(first);
  assert(it != index_.end());

  bool complete;
  if (*it == first) {
    std::next(it);
    complete = true;
  } else {
    // the range (first, *it] is unknown
    complete = false;
  }

  auto it2 = index_.find(last);
  assert(it2 != index_.end());

  std::vector<uint64_t> res;
  std::copy(it, std::next(it2), std::back_inserter(res));

  return std::make_pair(res, complete);
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
