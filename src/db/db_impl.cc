#include "db_impl.h"
#include <unistd.h>
#include <chrono>

namespace cruzdb {

DBImpl::DBImpl(zlog::Log *log, const RestorePoint& point) :
  log_(log),
  cache_(this),
  stop_(false),
  in_flight_txn_rid_(-1),
  root_(Node::Nil(), this)
{
  auto root = cache_.CacheAfterImage(point.after_image, point.after_image_pos);
  root_.replace(root);

  root_intention_ = point.after_image.intention();
  log_reader_pos = point.replay_start_pos;
  last_intention_processed = root_intention_;

  txn_writer_ = std::thread(&DBImpl::TransactionWriter, this);
  txn_processor_ = std::thread(&DBImpl::TransactionProcessor, this);
  txn_finisher_ = std::thread(&DBImpl::TransactionFinisher, this);
  log_reader_ = std::thread(&DBImpl::LogReader, this);
}

DBImpl::~DBImpl()
{
  {
    std::lock_guard<std::mutex> l(lock_);
    stop_ = true;
  }

  pending_intentions_cond_.notify_one();
  unwritten_roots_cond_.notify_one();
  pending_after_images_cond_.notify_one();

  log_reader_.join();
  txn_finisher_.join();
  txn_processor_.join();
  txn_writer_.join();
  cache_.Stop();
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

    switch (entry.msg_case()) {
      case cruzdb_proto::LogEntry::kIntention:
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
           assert((int64_t)it->first == it->second.second.intention());
           return 0;
         }
       }
       break;

       // find the oldest version of each after image
      case cruzdb_proto::LogEntry::kAfterImage:
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

/*
 *
 */
int DBImpl::_validate_rb_tree(const SharedNodeRef root)
{
  assert(root != nullptr);

  assert(root->read_only());
  if (!root->read_only())
    return 0;

  if (root == Node::Nil())
    return 1;

  assert(root->left.ref_notrace());
  assert(root->right.ref_notrace());

  SharedNodeRef ln = root->left.ref_notrace();
  SharedNodeRef rn = root->right.ref_notrace();

  if (root->red() && (ln->red() || rn->red()))
    return 0;

  int lh = _validate_rb_tree(ln);
  int rh = _validate_rb_tree(rn);

  if ((ln != Node::Nil() && ln->key().compare(root->key()) >= 0) ||
      (rn != Node::Nil() && rn->key().compare(root->key()) <= 0))
    return 0;

  if (lh != 0 && rh != 0 && lh != rh)
    return 0;

  if (lh != 0 && rh != 0)
    return root->red() ? lh : lh + 1;

  return 0;
}

void DBImpl::validate_rb_tree(NodePtr root)
{
  assert(_validate_rb_tree(root.ref_notrace()) != 0);
}

int DBImpl::Get(const zlog::Slice& key, std::string *value)
{
  auto root = root_;
  std::vector<NodeAddress> trace;

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
  std::unique_lock<std::mutex> lk(lock_);
  return new TransactionImpl(this,
      root_,
      root_intention_,
      in_flight_txn_rid_--,
      txn_finder_.Token());
}

void DBImpl::WaitOnIntention(uint64_t pos)
{
  bool done = false;
  std::condition_variable cond;
  std::unique_lock<std::mutex> lk(lock_);
  if (pos <= last_intention_processed) {
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

// TODO:
//  - where does the log reader start from, and how does it progress. currently
//  we are going to start from 0 because we are only considering new databases.
//  we progress by two methods: locally we know when a new intention or after
//  image has been appened, but other nodes may also append, so we need to make
//  sure to be checking with the sequencer. currently we are single node, so we
//  are only dealing with the simple case...
//
//  create new log reader service
void DBImpl::LogReader()
{
  while (true) {
    {
      std::lock_guard<std::mutex> l(lock_);
      if (stop_)
        return;
    }

    std::string data;

    // need to fill log positions. this is because it is important that any
    // after image that is currently the first occurence following its
    // intention, remains that way.
    int ret = log_->Read(log_reader_pos, &data);
    if (ret) {
      // TODO: be smart about reading. we shouldn't wait one second, and we
      // should sometimes fill holes. the current infrastructure won't generate
      // holes in testing, so this will work for now.
      if (ret == -ENOENT) {
        /*
         * TODO: currently we can run into a soft lockup where the log reader is
         * spinning on a position that hasn't been written. that's weird, since
         * we aren't observing any failed clients or sequencer or anything, so
         * every position should be written.
         *
         * what might be happening.. is that there is a hole, but the entity
         * assigned to fill that hole is waiting on something a bit further
         * ahead in the log, so no progress is being made...
         *
         * lets get a confirmation about the state here so we can record this
         * case. it would be an interesting case.
         *
         * do timeout waits so we can see with print statements...
         */
        continue;
      }
      assert(0);
    }

    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());

    // TODO: look into using Arena allocation in protobufs, or moving to
    // flatbuffers. we basically want to avoid all the copying here, by doing
    // something like pushing a pointer onto these lists, or using move
    // semantics.
    switch (entry.msg_case()) {
      case cruzdb_proto::LogEntry::kIntention:
        {
          std::lock_guard<std::mutex> lk(lock_);
          pending_intentions_.emplace_back(log_reader_pos, entry.intention());
        }
        pending_intentions_cond_.notify_one();
        break;

      case cruzdb_proto::LogEntry::kAfterImage:
        {
          std::lock_guard<std::mutex> lk(lock_);
          pending_after_images_.emplace_back(log_reader_pos, entry.after_image());
        }
        pending_after_images_cond_.notify_one();
        break;

      case cruzdb_proto::LogEntry::MSG_NOT_SET:
      default:
        assert(0);
        exit(1);
    }

    log_reader_pos++;
  }
}

bool DBImpl::ProcessConcurrentIntention(const Intention& intention)
{
  auto snapshot = intention.Snapshot();
  assert(snapshot < root_intention_); // concurrent property

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
    assert(entry.msg_case() == cruzdb_proto::LogEntry::kIntention);

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

    if (intent_pos == (uint64_t)root_intention_)
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
    std::unique_lock<std::mutex> lk(lock_);

    if (!pending_intentions_cond_.wait_for(lk, std::chrono::seconds(10),
          [&] { return !pending_intentions_.empty() || stop_; })) {
      std::cout << "tp: no pending intentions for 1 second" << std::endl;
#if 0
      for (auto& txn : pending_txns_) {
        std::cout << "tp: pending txn at pos " << txn.first << std::endl;
      }
#endif
      continue;
    }

    if (stop_)
      return;

    // the intentions are processed in strict fifo order
    const auto i_info = pending_intentions_.front();
    pending_intentions_.pop_front();

    const auto intention_pos = i_info.first;
    const auto& intention = i_info.second;

    // the next root starts with the current root as its snapshot
    auto next_root = std::make_unique<PersistentTree>(this, root_,
        static_cast<int64_t>(intention_pos));

    lk.unlock();

    assert(root_intention_ < (int64_t)intention_pos);
    auto serial = root_intention_ == intention.Snapshot();

    // check for transaction conflicts
    bool abort;
    if (serial) {
      abort = false;
    } else {
      abort = ProcessConcurrentIntention(intention);
    }

    // if aborting, notify waiters, then move on to next txn
    if (abort) {
      lk.lock();
      NotifyTransaction(intention.Token(), intention_pos, false);
      last_intention_processed = intention_pos;
      continue;
    }

    // save the location of the next intention that committed. note for the
    // future: we are not holding any locks here because this thread has
    // execlusive access to this index.
    {
      auto ret = intention_map_.emplace(intention_pos);
      assert(ret.second);
    }

    ReplayIntention(next_root.get(), intention);

    auto root_offset = next_root->infect_self_pointers(intention_pos);

    assert(next_root->root_ != nullptr);
    NodePtr root(next_root->root_, this);
    if (root_offset) {
      assert(next_root->root_ != Node::Nil());
      root.SetIntentionAddress(intention_pos, *root_offset);
    }

    lk.lock();
    NotifyTransaction(intention.Token(), intention_pos, true);
    root_.replace(root);
    root_intention_ = intention_pos;
    last_intention_processed = intention_pos;

    unwritten_roots_.emplace_back(intention_pos, std::move(next_root));
    unwritten_roots_cond_.notify_one();
  }
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
void DBImpl::TransactionWriter()
{
  std::unordered_map<uint64_t,
    std::pair<uint64_t, cruzdb_proto::AfterImage>> after_images_cache;

  while (true) {
    std::unique_lock<std::mutex> lk(lock_);

    /*
     * since this thread is handling both unwritten roots, as well as pending
     * after images, there might be a lot of superfolous wake-ups arriving.
     * we'll need to think about this as we get things more stable and are ready
     * to refactor.
     */
    if (!unwritten_roots_cond_.wait_for(lk, std::chrono::seconds(10),
          [&] { return !unwritten_roots_.empty() || stop_; })) {
      std::cout << "tw: no unwritten root produced for 2 seconds" << std::endl;
      continue;
    }

    if (stop_)
      return;

    // TODO: rid. rid also needs to go in the pointers so we can search for their
    // physical address later..

    /*
     * step one to being able to free this root from memory (or more precisely
     * put it under control of the caching system), we need to make sure it is
     * saved to storage so that we can provide physical addresses of its nodes.
     *
     * we start with the oldest node...
     */
    auto root_info = std::move(unwritten_roots_.front());
    unwritten_roots_.pop_front();
    auto root = std::move(root_info.second);
    lk.unlock();

    // serialize the after image to the end of the log. note that by design, any
    // pointer contained in this after image either points to a node in its
    // delta, or has a fully defined physical address.
    cruzdb_proto::AfterImage after_image;
    std::vector<SharedNodeRef> delta;

    // 1. look-up the physical address for any pointers marked as pointing to
    // volatile nodes. 2. assert the property that all nodes are either pointing
    // internally, OR are fully defined physically.
    //
    // NOTE: here we don't alter the 
    //
    // After the serialization I/O complets we can then update the node pointers
    // and probably also trim the index..
    root->SerializeAfterImage(after_image, root_info.first, delta);

    // set during serialization, provided when the intention was processed to
    // produce this after image.
    uint64_t intention_pos = after_image.intention();
    assert(intention_pos == root_info.first);

    std::string blob;
    cruzdb_proto::LogEntry entry;
    entry.set_allocated_after_image(&after_image);
    assert(entry.IsInitialized());
    assert(entry.SerializeToString(&blob));
    // cannot use entry after release...
    entry.release_after_image();

    uint64_t ai_pos;
    int ret = log_->Append(blob, &ai_pos);
    assert(ret == 0);
    assert(intention_pos < ai_pos);

    //std::cout << "serialized intention @ " << intention_pos <<
    //  " waiting on after image from log" << std::endl;

    // to make sure everyone is using the same physical version, everyone should
    // use the first instance in the log. since we may not have been the first
    // to append, now we wait for this after image to show up in the log.
    lk.lock();
    while (true) {

      pending_after_images_cond_.wait(lk, [&] {
        return !pending_after_images_.empty() || stop_;
      });

      if (stop_)
        return;

      //std::cout << "adding " << pending_after_images_.size()
      //  << " pending after images" << std::endl;

      // TODO: this drop duplicates on the floor, which it should, but the cache
      // is never trimmed. so we need to add some type of low water mark here
      // that we can purge.
      //
      // TODO: make sure to consider the case that an after image arrives here
      // for processing before its made it through transaction processing. since
      // io reading and dispatch are asynchonous, this will likely occur often.
      // after first glance it seems totally fine. we are driven by transactions
      // that finish (above), and then just wait on the corresponding after
      // image to arrive...
      for (auto ai : pending_after_images_) {
        auto key = ai.second.intention();
        if (after_images_cache.find(key) == after_images_cache.end()) {
          //std::cout << "caching after image for intention @ " << key << std::endl;
          assert(key >= 0);
          assert((uint64_t)key < ai.first);
          after_images_cache[key] = ai;
        }
      }
      pending_after_images_.clear();

      // are we still waiting on the after image from above...?
      auto it = after_images_cache.find(intention_pos);
      if (it == after_images_cache.end()) {
        //std::cout << "NOT FOUND cached after image for intention @ " << intention_pos << std::endl;
        continue;
      }

      //std::cout << "FOUND cached after image for intention @ " << intention_pos << std::endl;

      // this sets the csn on all the self referencing nodes to be the actual
      // physical position that they are stored at in the log.
      //
      // deal with races...
      //root->SetDeltaPosition(delta, it->first);

      // update the index that allows node pointers being copied to resolve
      // their physical log location. TODO: use some sort of threshold for
      // expiring entries out of this index. TODO: how does node copy know that
      // the info can be found in this idnex?

      assert(intention_pos < it->second.first);
      //auto r = intention_to_after_image_pos_.emplace(intention_pos, it->second.first);
      //assert(r.second);

      // TODO: so... this is a contention point. there are readers of the state
      // we are now changing... this will need to be a big part of the refactor.
      //
      // TODO: setting the delta position only updates new self pointer in new
      // nodes. it might be a new node contains a pointer that was copied which
      // points into the volatile space. so, this needs to be taken into account
      // when determing when to trim the index.
      root->SetDeltaPosition(delta, it->second.first);
      cache_.SetIntentionMapping(intention_pos, it->second.first);

      // TODO: we could also add some assertions here that all pointers in the
      // delta being added to the cache have been converted to after image
      // pointers. that should be the case...
      cache_.ApplyAfterImageDelta(delta, it->second.first);

      break;
    }
  }
}

void DBImpl::TransactionFinisher()
{
}

bool DBImpl::CompleteTransaction(TransactionImpl *txn)
{
  const auto token = txn->Token();

  auto blob = txn->GetIntention().Serialize(token);

  TransactionFinder::WaiterHandle waiter;
  txn_finder_.AddTokenWaiter(waiter, token);

  // append intention to log
  uint64_t pos;
  int ret = log_->Append(blob, &pos);
  assert(ret == 0);

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

}
