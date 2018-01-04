#include "db_impl.h"
#include <unistd.h>
#include <sstream>
#include <chrono>

namespace cruzdb {

DBImpl::DBImpl(zlog::Log *log) :
  log_(log),
  cache_(this),
  stop_(false),
  in_flight_txn_rid_(-1),
  txn_token(0),
  root_(Node::Nil(), this),
  root_intention_(-1),
  txn_writer_(std::thread(&DBImpl::TransactionWriter, this)),
  txn_processor_(std::thread(&DBImpl::TransactionProcessor, this)),
  txn_finisher_(std::thread(&DBImpl::TransactionFinisher, this))
{
  validate_rb_tree(root_);
}

int DB::Open(zlog::Log *log, bool create_if_empty, DB **db)
{
  uint64_t tail;
  int ret = log->CheckTail(&tail);
  assert(ret == 0);

  // empty log
  if (tail == 0) {
    if (!create_if_empty) {
      assert(0);
      return -EINVAL;
    }

    // TODO: this initialization might be a race when we assume that multiple
    // nodes exist that all start-up at the same time and interact with an
    // initially empty log...
    //
    // initialization uses an after image without a corresponding intention.
    // that is, there is no transaction that initialized the log... it just is.
#if 0
    std::string blob;
    cruzdb_proto::LogEntry entry;
    auto ai = entry.mutable_after_image();
    ai->set_snapshot(-1);
    ai->set_intention(-1);
    assert(entry.IsInitialized());
    assert(entry.SerializeToString(&blob));

    std::string blob;
    cruzdb_proto::LogEntry entry;
    auto intention = entry.mutable_intention();
    intention->set_snapshot(-1);
    assert(entry.IsInitialized());
    assert(entry.SerializeToString(&blob));

    ret = log->Append(blob, &tail);
    assert(ret == 0);
#endif
  }

  DBImpl *impl = new DBImpl(log);

  // TODO: in the short term we might want to explicitly initialize the log
  // here, and figure out proper recovery later. for now we are going to just
  // let the empty/init log entry stay in the log and we'll ignore it during
  // processing.

  ret = impl->RestoreFromLog();
  if (ret)
    return ret;

  *db = impl;

  return 0;
}

DB::~DB()
{
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

/*
 * Log recovery:
 *   - scan the log backwards until we find the latest intention with a
 *   corresponding after image. then roll the log forward from that state.
 *
 *   - FIXME: the after image needs to be the first instance of the after image
 *   produced by the corresponding intention. so to make this robust, we should
 *   ensure that all log positions between the intention and the after image are
 *   filled.
 */
int DBImpl::RestoreFromLog()
{
  std::unordered_map<uint64_t,
    std::pair<uint64_t, cruzdb_proto::AfterImage>> after_images;

  uint64_t pos;
  int ret = log_->CheckTail(&pos);
  assert(ret == 0);

  // TODO: this initialization process and transaction commit/abort redeznvous
  // is so messed up :/
  txn_token = pos;

  const uint64_t real_tail = pos;

  // currently an empty log is an ok starting point for a new database, but i
  // think we'll want to add some explicit log entries for initialization later.
  if (pos == 0) {
    reader_initialized = true;
    log_reader_pos = 0;
    start_reader();
    return 0;
  }

  int64_t newest_intention = -1;
  int64_t oldest_intention = -1;

  // start reading the log in reverse...
  while (true) {
    std::string data;
    ret = log_->Read(pos, &data);
    if (ret < 0 && ret != -ENOENT) {
      // see comment on method about missing entries and correctness
      assert(0);
    }

    if (ret == 0) {
      cruzdb_proto::LogEntry entry;
      assert(entry.ParseFromString(data));
      assert(entry.IsInitialized());

      switch (entry.msg_case()) {
        case cruzdb_proto::LogEntry::kIntention:
          {
            oldest_intention = pos;
            if (newest_intention == -1)
              newest_intention = (int64_t)pos;

            auto it = after_images.find(pos);
            if (it != after_images.end()) {
              std::cout << "recovery intention pos: " <<
                pos << " newest intention " << newest_intention
                << " tail " << real_tail << std::endl;

              // the after image will be the new root
              auto root = cache_.CacheAfterImage(it->second.second, it->second.first);
              root_.replace(root);
              // TODO: we should be either keeping this intention address in the
              // pointer, or keeping a separate data structure for specific use
              // cases that contains these mappings, or facilities for resolving
              // it.
              root_intention_ = it->second.second.intention();
              assert(root_intention_ >= 0);
              assert((int64_t)it->first == root_intention_);

              // start replaying log from pos+1
              reader_initialized = true;
              log_reader_pos = pos + 1;

              start_reader();

              // wait until the log rolls forward. this just good to do (not in
              // this way, of course we neeed better synchronization mechanisms
              // here). this isn't strictly needed as we can always submit
              // transactions against older snapshots and wait for
              // commit/abort...however..we haven't implemented that yet...
              assert((int64_t)pos <= newest_intention);
              if ((int64_t)pos < newest_intention) { // already up to date?
                while (last_intention_processed < newest_intention) {
                  usleep(2000);
                }
              }

              return 0;
            }
          }
          break;

        case cruzdb_proto::LogEntry::kAfterImage:
          {
            auto ai = entry.after_image();
            //std::cout << "after image for intention " << ai.intention() << " at " << pos << std::endl;
            after_images.emplace(ai.intention(), std::make_pair(pos, ai));
          }
          break;

        default:
          assert(0);
          exit(1);
      }
    }

    // made it pos 0 without recovering
    if (pos == 0) {
      // startup edge cases we'll need to think about. unlikely to happen
      // anytime soon.
      assert(oldest_intention >= 0);

      // if we had an after image, it could only have been produced by an
      // intention in the log that proceeded it.
      assert(after_images.empty());

      // so we'll start now with the oldest intention and roll forward. TODO..
      // this is one reason we probably want a specific start transaction so
      // that we can distinguish between weird crashes at start-up causing this
      // cenario in which a bunch of txns ran, but their after images never got
      // serialized.

      // ok, this is just the same thing that happens above when we end up
      // finding a after image / intention pair, except the root is just the
      // already setup empty tree.
      //auto root = cache_.CacheAfterImage(it->second.second, it->second.first);
      //root_.replace(root);

      // start replaying log from pos+1
      reader_initialized = true;

      // important: need to replay from here to produce a new starting after image!
      log_reader_pos = oldest_intention;

      start_reader();

      // wait until the log rolls forward. this just good to do (not in
      // this way, of course we neeed better synchronization mechanisms
      // here). this isn't strictly needed as we can always submit
      // transactions against older snapshots and wait for
      // commit/abort...however..we haven't implemented that yet...
      assert((int64_t)pos <= newest_intention);
      if ((int64_t)pos < newest_intention) { // already up to date?
        while (last_intention_processed < newest_intention) {
          usleep(2000);
        }
      }

      return 0;

      //std::cout << "real tail " << real_tail << " oldest int " << oldest_intention << std::endl;
      //assert(0);
      //return -EINVAL;
    }

    pos--;
  }
}

std::ostream& operator<<(std::ostream& out, const SharedNodeRef& n)
{
  out << "node(" << n.get() << "):" << n->key().ToString() << ": ";
#if 0 // TODO: fixup for new addressing
  out << (n->red() ? "red " : "blk ");
  //out << "fi " << n->field_index() << " ";
  out << "left=[p" << n->left.csn() << ",o" << n->left.offset() << ",";
  if (n->left.ref_notrace() == Node::Nil())
    out << "nil";
  else
    out << n->left.ref_notrace().get();
  out << "] ";
  out << "right=[p" << n->right.csn() << ",o" << n->right.offset() << ",";
  if (n->right.ref_notrace() == Node::Nil())
    out << "nil";
  else
    out << n->right.ref_notrace().get();
  out << "] ";
#endif
  return out;
}

std::ostream& operator<<(std::ostream& out, const cruzdb_proto::NodePtr& p)
{
  out << "[n" << p.nil() << ",s" << p.self()
    << ",p" << p.csn() << ",o" << p.off() << "]";
  return out;
}

std::ostream& operator<<(std::ostream& out, const cruzdb_proto::Node& n)
{
  out << "key " << n.key() << " val " << n.val() << " ";
  out << (n.red() ? "red" : "blk") << " ";
  out << "left " << n.left() << " right " << n.right();
  return out;
}

std::ostream& operator<<(std::ostream& out, const cruzdb_proto::AfterImage& i)
{
  out << "- intention tree_size = " << i.tree_size() << std::endl;
  for (int idx = 0; idx < i.tree_size(); idx++) {
    out << "  " << idx << ": " << i.tree(idx) << std::endl;
  }
  return out;
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

/*
 * TODO:
 *  - we will likely want to bound/throttle the number of inflight transactions
 *  so that slow transaction processing doesn't result in a massive back log of
 *  transactions that could create memory pressure or result in pathological
 *  conditions with processing extremely large conflict zones.
 *
 *  - add an assertion check here that the database state as been initialized.
 *  users shouldn't be able to interact with the instance until it is
 *  initialized, but we hit some issues during development related to this and
 *  this would be a good defensive check.
 */
Transaction *DBImpl::BeginTransaction()
{
  std::unique_lock<std::mutex> lk(lock_);
  return new TransactionImpl(this,
      root_,
      root_intention_,
      in_flight_txn_rid_--);
}

// TODO:
//  - where does the log reader start from, and how does it progress. currently
//  we are going to start from 0 because we are only considering new databases.
//  we progress by two methods: locally we know when a new intention or after
//  image has been appened, but other nodes may also append, so we need to make
//  sure to be checking with the sequencer. currently we are single node, so we
//  are only dealing with the simple case...
void DBImpl::LogReader()
{
  assert(reader_initialized);

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
#if 0
        if (++count % 1000000 == 0) {
          uint64_t tail;
          int ret = log_->CheckTail(&tail);
          assert(ret == 0);
          std::cout << "log reader no entry count " << count 
            << " latest pos " << log_reader_pos << " tail " << tail << std::endl;
        }
#endif
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
        //std::cout << "log_reader: pos " << pos << " intention" << std::endl;
        {
          std::lock_guard<std::mutex> lk(lock_);
          pending_intentions_.emplace_back(log_reader_pos, entry.intention());
        }
        pending_intentions_cond_.notify_one();
        break;

      case cruzdb_proto::LogEntry::kAfterImage:
        //std::cout << "log_reader: pos " << pos << " after image" << std::endl;
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

bool DBImpl::ProcessConcurrentIntention(const cruzdb_proto::Intention& intention)
{
  auto snapshot = intention.snapshot_intention();
  assert(snapshot < root_intention_); // concurrent property

  auto it = intention_map_.find(snapshot);
  assert(it != intention_map_.end());
  it++; // start immediately after snapshot

  // set of keys read or written by the intention
  std::set<std::string> intention_keys;
  for (int i = 0; i < intention.ops_size(); i++) {
    auto& op = intention.ops(i);
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
  auto it = pending_txns_.find(token);
  if (it != pending_txns_.end()) {
    auto waiter = it->second;
    pending_txns_.erase(it);
    waiter->pos = intention_pos;
    waiter->committed = committed;
    waiter->complete = true;
    waiter->cond.notify_one();
  }
}

void DBImpl::ReplayIntention(PersistentTree *tree,
    const cruzdb_proto::Intention& intention)
{
  bool read_only = true;
  for (int idx = 0; idx < intention.ops_size(); idx++) {
    auto& op = intention.ops(idx);
    switch (op.op()) {
      case cruzdb_proto::TransactionOp::GET:
        assert(!op.has_val());
        break;

      case cruzdb_proto::TransactionOp::PUT:
        assert(op.has_val());
        tree->Put(op.key(), op.val());
        read_only = false;
        break;

      case cruzdb_proto::TransactionOp::DELETE:
        assert(!op.has_val());
        tree->Delete(op.key());
        read_only = false;
        break;

      default:
        assert(0);
        exit(1);
    }
  }
  // nothing wrong with these, but haven't yet considered if handling them is a
  // special case or deserves an optimization.
  assert(intention.ops_size());
  assert(!read_only);
}

void DBImpl::TransactionProcessor()
{
  while (true) {
    std::unique_lock<std::mutex> lk(lock_);

    if (!pending_intentions_cond_.wait_for(lk, std::chrono::seconds(10),
          [&] { return !pending_intentions_.empty() || stop_; })) {
      std::cout << "tp: no pending intentions for 1 second" << std::endl;
      for (auto& txn : pending_txns_) {
        std::cout << "tp: pending txn at pos " << txn.first << std::endl;
      }
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

    // TODO: get rid of root_intention_. how is initialization handled?
    assert(root_intention_ < (int64_t)i_info.first);
    auto serial = root_intention_ == intention.snapshot_intention();

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
      NotifyTransaction(intention.token(), intention_pos, false);
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

    // mark all pointers in this delta that do not have a physical address with
    // the address of the intention. we can't use the after image because it
    // hasn't been created yet. instead the nodes are marked with the intention
    // address. as more transactions execute, this address is propogated and
    // fixed up later.
    int root_offset = next_root->infect_self_pointers(intention_pos);

    lk.lock();

    NotifyTransaction(intention.token(), intention_pos, true);

    // TODO: filling in csn during after image replay races with whatever the
    // current root is which copies of nodes are being made during intention
    // replay.
    NodePtr root(next_root->root_, this);

    // TODO: this may be Nil... in which case the asserts below are wrong
    assert(next_root->root_ != Node::Nil());
    // TODO: watch out for scenarios in which the delta is empty and we are now
    // referencing a root node i the after image that is unchanged, yet we treat
    // here like it is coming from a new intention. we shouldn't ever encounter
    // this in the current implementation cause we bail on empty transactions
    // but in general we need be careful and check for different conditions.
    root.SetIntentionAddress(i_info.first, root_offset);
    root_intention_ = i_info.first;

    root_.replace(root);

    last_intention_processed = i_info.first;

    // TODO: uncached_roots is a better name, and we don't need to have multiple
    // queus, so get rid of the shared pointer.
    unwritten_roots_.emplace_back(i_info.first, std::move(next_root));
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

// TODO: this may not be necessary any more. for instance, from the db's
// perspective, there is an extremely narrow window in which a transaction could
// be aborted, and those are all after txn commit has been requested. so abort
// should be able to be a strictly transaction side operation.
void DBImpl::AbortTransaction(TransactionImpl *txn)
{
  assert(0);
  //assert(!txn->Committed());
  //assert(!txn->Completed());
}

/**
 * TODO: when we get around to allowing the transaction after image to be
 * re-used to avoid de-serialization from the log, then we need to make sure we
 * properly convert the in-memory representation (e.g. updating rid).
 */
bool DBImpl::CompleteTransaction(TransactionImpl *txn)
{
  // TODO: are these flags really necessary?
  //assert(!txn->Committed());
  //assert(!txn->Completed());

  /*
   * TODO: create a unique value for thet ransaction for rendezvous. we really
   * need to be carefully checking based on `pos`, and then only use the token
   * for (1) recovery by clients that want to inspect if a txn completed or not,
   * and (2) its primary purpose is to allow this txn to rendezvous with the
   * commit process.
   *
   * in a distributed environment, we also should try to make these tokens
   * unique, but not rely on them for correctness.
   */
  auto token = txn_token++;

  // transaction intention -> binary blob
  std::string blob;
  {
    cruzdb_proto::LogEntry entry;
    auto& intent = txn->GetIntention();
    intent.set_token(token);
    entry.set_allocated_intention(&intent);
    assert(entry.IsInitialized());
    assert(entry.SerializeToString(&blob));
    // cannot use entry after release...
    entry.release_intention();
  }

  std::unique_lock<std::mutex> lk(lock_);
  TransactionWaiter waiter;
  auto insert_ret = pending_txns_.emplace(token, &waiter);
  // wait for transaction to be processed
  assert(insert_ret.second);

  lk.unlock();

  // intention is appended to the log
  // TODO:
  //   - this should be smarter about I/O errors
  //   - how to handle potential duplicate appends
  uint64_t pos;
  int ret = log_->Append(blob, &pos);
  assert(ret == 0);

  lk.lock();

  max_pending_txn_pos_ = std::max(max_pending_txn_pos_, pos);

  // TODO: wake up processor

  // wait on result
  //std::cout << "txn added waiting on " << pos << std::endl;
  waiter.cond.wait(lk, [&]{ return waiter.complete; });
  //std::cout << "woke up for pos " << pos << std::endl;

  assert(waiter.pos >= 0);
  assert((uint64_t)waiter.pos == pos);

  // the caller took care of removing the waiter structure from the set of
  // pending transaction so there is no shared state to update.
  lk.unlock();

  return waiter.committed;
}

}
