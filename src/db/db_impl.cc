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
  root_(Node::Nil(), this, true),
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

void DBImpl::write_dot_null(std::ostream& out,
    SharedNodeRef node, uint64_t& nullcount)
{
  nullcount++;
  out << "null" << nullcount << " [shape=point];"
    << std::endl;
  out << "\"" << node.get() << "\" -> " << "null"
    << nullcount << " [label=\"nil\"];" << std::endl;
}

void DBImpl::write_dot_node(std::ostream& out,
    SharedNodeRef parent, NodePtr& child, const std::string& dir)
{
  out << "\"" << parent.get() << "\":" << dir << " -> ";
  out << "\"" << child.ref_notrace().get() << "\"";
  out << " [label=\"" << child.csn() << ":"
    << child.offset() << "\"];" << std::endl;
}

void DBImpl::write_dot_recursive(std::ostream& out, int64_t rid,
    SharedNodeRef node, uint64_t& nullcount, bool scoped)
{
  if (scoped && node->rid() != rid)
    return;

  out << "\"" << node.get() << "\" ["
    << "label=\"" << node->key().ToString() << "_" << node->val().ToString() << "\",style=filled,"
    << "fillcolor=" << (node->red() ? "red" :
        "black,fontcolor=white")
    << "]" << std::endl;

  assert(node->left.ref_notrace() != nullptr);
  if (node->left.ref_notrace() == Node::Nil())
    write_dot_null(out, node, nullcount);
  else {
    write_dot_node(out, node, node->left, "sw");
    write_dot_recursive(out, rid, node->left.ref_notrace(), nullcount, scoped);
  }

  assert(node->right.ref_notrace() != nullptr);
  if (node->right.ref_notrace() == Node::Nil())
    write_dot_null(out, node, nullcount);
  else {
    write_dot_node(out, node, node->right, "se");
    write_dot_recursive(out, rid, node->right.ref_notrace(), nullcount, scoped);
  }
}

void DBImpl::_write_dot(std::ostream& out, SharedNodeRef root,
    uint64_t& nullcount, bool scoped)
{
  assert(root != nullptr);
  write_dot_recursive(out, root->rid(),
      root, nullcount, scoped);
}

void DBImpl::write_dot(std::ostream& out, bool scoped)
{
  auto root = root_;
  uint64_t nullcount = 0;
  out << "digraph ptree {" << std::endl;
  _write_dot(out, root.ref_notrace(), nullcount, scoped);
  out << "}" << std::endl;
}

void DBImpl::write_dot_history(std::ostream& out,
    std::vector<Snapshot*>& snapshots)
{
  uint64_t trees = 0;
  uint64_t nullcount = 0;
  out << "digraph ptree {" << std::endl;
  std::string prev_root = "";

  for (auto it = snapshots.cbegin(); it != snapshots.end(); it++) {

    // build sub-graph label
    std::stringstream label;
    label << "label = \"root: " << (*it)->root.csn();
    label << "\"";

    out << "subgraph cluster_" << trees++ << " {" << std::endl;
    auto ref = (*it)->root.ref_notrace();
    if (ref == Node::Nil()) {
      out << "null" << ++nullcount << " [label=nil];" << std::endl;
    } else {
      _write_dot(out, ref, nullcount, true);
    }

#if 0
    if (prev_root != "")
      out << "\"" << prev_root << "\" -> \"" << it->root.get() << "\" [style=invis];" << std::endl;
    std::stringstream ss;
    ss << it->root.get();
    prev_root = ss.str();
#endif
    out << label.str() << std::endl;
    out << "}" << std::endl;
  }
  out << "}" << std::endl;
}

void DBImpl::print_node(SharedNodeRef node)
{
  if (node == Node::Nil())
    std::cout << "nil:" << (node->red() ? "r" : "b");
  else
    std::cout << node->key().ToString() << ":" << (node->red() ? "r" : "b");
}

void DBImpl::print_path(std::ostream& out, std::deque<SharedNodeRef>& path)
{
  out << "path: ";
  if (path.empty()) {
    out << "<empty>";
  } else {
    out << "[";
    for (auto node : path) {
      if (node == Node::Nil())
        out << "nil:" << (node->red() ? "r " : "b ");
      else
        out << node->key().ToString() << ":" << (node->red() ? "r " : "b ");
    }
    out << "]";
  }
  out << std::endl;
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
  std::vector<std::pair<int64_t, int>> trace;

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
  int64_t max_intention_resolvable = -1;
  auto it = intention_to_after_image_pos_.rbegin();
  if (it != intention_to_after_image_pos_.rend()) {
    max_intention_resolvable = it->first;
  }
  return new TransactionImpl(this,
      root_,
      root_intention_,
      max_intention_resolvable,
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

    auto i_info = pending_intentions_.front();
    pending_intentions_.pop_front();

    // TODO: we create a transaction here that we will replay the operations
    // against. we end up throwing the transaction away at the end and only
    // keeping the resulting tree. there are probably other inefficiencies here
    // too, such as recording all the ops, again. so there is some optimization
    // that can be done here.
    //
    // NOTE: its important that we use the pos here for the root id so we can
    // assign some identity. we need to match up the serialization of the after
    // image with the intention, and also identify duplicates when processing
    // after images. this is also used to propogate the intention position
    // through to node pointers so that they can resolve back to physical
    // pointers later... it's like a virus that will affect future transactions.
    //
    // max intent resovl: this is for stopping propogation of volatile pointers
    uint64_t max_intention_resolvable = -1;
    {
      auto it = intention_to_after_image_pos_.rbegin();
      if (it != intention_to_after_image_pos_.rend()) {
        max_intention_resolvable = it->first;
      }
    }

#if 0
    is there any risk here of having max intent pointing at the same intent that
      we are procesing right now? i think not because any after image would have
      to come strictly after that and we process in log order... but... another
      node could have processed it and appened, and we end up reading it while
      we are doing this???

      does this pose any problem?

      i think it will be fine. basically wed just end up copying a full verison
      of a pointer. and infect self pointers method will: skip any nodes that
      are fully specified.

      hmm.. I THInk there is a scenario in which self-pointers would be fully
      spefieied and then infect-self below would overwrite. im tempted to say
      that that woudl be ok because it could be resolved naturally later, but
      perhaps we want to also check for the condition and skip it.
#endif

   // here we replay the transaction against a snapshot of the database. it
   // could be the original snapshot, or a future snapshot. if it is the same,
   // then, there is no need to actually do commit/abort processing.

   // std::cout << "txn-replay: intent @ " << i_info.first << std::endl;
    auto txn_wrapper = std::make_shared<PersistentTree>(this, root_,
        max_intention_resolvable,
        (int64_t)i_info.first);

    assert(txn_wrapper->rid() >= 0);

    lk.unlock();

    auto& intention = i_info.second;

    // TODO: if the intentions snapshot is the last committed state, then we
    // don't need to check for conflicts.
    //
    // TODO: it may be possible that we want to predict that there are very few
    // concurrent transactions, and in this case we go ahead and seralize the
    // after image with an intention. in these cases we could use that after
    // image for serial transactions, and avoid producing after images in these
    // cases.

    // commit/abort
    //
    // i_info.first: the address of the intention that we are replaying
    // root_/root_intention: the last commit state / intention that produced it
    // intention.snapshot_intention: the address of the snapshot/intention that i_info.first ran against

    // TODO: initialization is tricky. currently for an empty db we just
    // intialize against an in-memory root pointer to Nil, and root_intention_
    // is -1 initially, because, what else would it be? so i think this here
    // just shows a good case for making sure initialization includes some
    // on-disk data.
#if 0
    if (root_intention_ < 0) {
      std::cout << root_intention_ << std::endl;
      assert(root_intention_ >= 0);
    }
#endif
    assert(root_intention_ < (int64_t)i_info.first);
    auto serial = root_intention_ == intention.snapshot_intention();

    bool abort = false;
    if (serial) {
      // this is the case in which we can avoid replaying. if we have the
      // in-memory after image because the transaction executed locally, then
      // use that. otherwise, we can make a decision to either wait for an after
      // image to show up in the log, or replay here without checking for
      // commit/abort.
    } else {
      /*
       * TODO: we need to find a good way to manage the intention map index. we
       * can also search the log within a range to find the intentions, we could
       * embed backpointers in intentions, we could keep a small index in lmdb
       * of everything. some of this info can also be kept nodeptr and erived
       * from cached nodes. lots of options...
       */
      auto si = intention.snapshot_intention();
      assert(si < root_intention_);

      auto it = intention_map_.find(si);
      assert(it != intention_map_.end());
      it++; // start immediately after si

      int logical_cz_size = 0;
      // obviously this is crazy inefficient...
      while (true) {
        uint64_t intent_pos = *it;

        std::string data;
        int ret = log_->Read(intent_pos, &data);
        assert(ret == 0);
        cruzdb_proto::LogEntry entry;
        assert(entry.ParseFromString(data));
        assert(entry.IsInitialized());
        assert(entry.msg_case() == cruzdb_proto::LogEntry::kIntention);
        auto check_intention = entry.intention();

        logical_cz_size++;

        // TODO: this just looks at PUT/PUT for now. we'll need to expand to
        // many other types of conflicts...
        for (int i = 0; i < intention.ops_size(); i++) {
          auto& iop = intention.ops(i);
          if (iop.op() == cruzdb_proto::TransactionOp::PUT) {
            for (int j = 0; j < check_intention.ops_size(); j++) {
              auto& jop = check_intention.ops(j);
              if (jop.op() == cruzdb_proto::TransactionOp::PUT &&
                  iop.key() == jop.key()) {
                abort = true;
                // could just bail early here
              }
            }
          }
        }

        assert(root_intention_ >= 0);
        if (intent_pos == (uint64_t)root_intention_)
          break;

        it++;
        assert(it != intention_map_.end());
      }

      // this is printing out the distance in log positions. we also might want
      // to know how many intentions are in the conflict zone.
      // checkout std::distance
      assert(logical_cz_size > 0);
      std::cout << "conflict zone size: " <<
        (root_intention_ - si) << " logical size " <<
        logical_cz_size << std::endl;
    }

    if (abort) {
      lk.lock();

      // notify the transaction waiter?
      auto token = intention.token();
      auto it = pending_txns_.find(token);
      if (it != pending_txns_.end()) {
        auto waiter = it->second;
        pending_txns_.erase(it);
        waiter->pos = i_info.first;
        waiter->committed = false;
        waiter->complete = true;
        waiter->cond.notify_one();
      }

      last_intention_processed = i_info.first;
      
      continue;
    }

    // the txn for this intention commited. its the next in line
    auto imret = intention_map_.emplace(i_info.first);
    assert(imret.second);

    int idx;
    bool read_only = true;
    for (idx = 0; idx < intention.ops_size(); idx++) {
      auto& op = intention.ops(idx);
      switch (op.op()) {
        case cruzdb_proto::TransactionOp::GET:
          assert(!op.has_val());
          break;

        case cruzdb_proto::TransactionOp::PUT:
          assert(op.has_val());
          //std::cout << "   txn-replay: Put(" << op.key() << ")" << std::endl;
          //std::cout << "ServerPut " << txn_wrapper.get() << std::endl;
          txn_wrapper->Put(op.key(), op.val());
          read_only = false;
          break;

        case cruzdb_proto::TransactionOp::DELETE:
          assert(!op.has_val());
          txn_wrapper->Delete(op.key());
          read_only = false;
          break;

        default:
          assert(0);
          exit(1);
      }
    }

    if (idx == 0) {
      //std::cout << "warning: empty transaction" << std::endl;
      assert(0); // these may cause problem for the assumption below in which we have new root
    }

    // nothing wrong RO transactions, but we might want to handle them in a
    // special way later, or treat them differently depending on the isolation
    // level.
    if (read_only) {
      //std::cout << "info: read only transaction" << std::endl;
      assert(0); // these may cause problem for the assumption below in which we have new root
    }

    // before we let new transaction start running against this new after image,
    // we need to infect its pointers so that when copies of the nodes in this
    // after image are made, which may refer to nodes that do not yet have
    // physical addresses, that they contain information sufficient to resolves
    // those missing locations.
    //
    // issues: speed: we could probably do some of this work in parallel with
    // transaction processing if we are smart about it. we could also trade
    // memory for time, keep nodes cached for a longer period of time, and index
    // on node heap pointers instead of setting the offset/intention position
    // here.
    // 
    // also we need to do this without hte db lock since we might try to resolve
    // some pointers in here. ideally we do not need to resolve shit. so we'll
    // need to figure that out later.
    int root_offset = txn_wrapper->infect_self_pointers(i_info.first);

    lk.lock();

    // notify the transaction waiter?
    auto token = intention.token();
    auto it = pending_txns_.find(token);
    if (it != pending_txns_.end()) {
      auto waiter = it->second;
      pending_txns_.erase(it);
      waiter->pos = i_info.first;
      waiter->committed = true;
      waiter->complete = true;
      waiter->cond.notify_one();
    } else {
      // we rendezvous on the position of the intention. but its possible that
      // the intention was processed here before the client had a chance to
      // stick it in the pending index after performing the append. two options:
      //
      // 1.. put a unique value in the log entry and rendezvous on that, or,
      // cache the answers and have the client look too. this last option is
      // certainly robust, but we need to be careful that the index doesn't
      // become too large. that shouldn't be a problem. this case is fairly
      // narrow, and we can also trim this index aggressively (i think).

      // ok, so we probably want a nonce for clients. so we'll use that method
      // of rendezvous. we can also keep an index. lets also figure out we can
      // build an index so we can look it up from the log?
    }

    // TODO: uncached_roots is a better name, and we don't need to have multiple
    // queus, so get rid of the shared pointer.
    unwritten_roots_.emplace_back(i_info.first, txn_wrapper);
    unwritten_roots_cond_.notify_one();

    // TODO: filling in csn during after image replay races with whatever the
    // current root is which copies of nodes are being made during intention
    // replay.
    NodePtr root(txn_wrapper->root_, this, false);

    // TODO: this may be Nil... in which case the asserts below are wrong
    assert(txn_wrapper->root_ != Node::Nil());
    // TODO: watch out for scenarios in which the delta is empty and we are now
    // referencing a root node i the after image that is unchanged, yet we treat
    // here like it is coming from a new intention. we shouldn't ever encounter
    // this in the current implementation cause we bail on empty transactions
    // but in general we need be careful and check for different conditions.
    root.set_csn(i_info.first);
    root.set_csn_is_intention_pos();
    root.set_offset(root_offset); // last one in serialization order

    // this is the address of the intntion that produced this db state
    root_intention_ = root.csn();

    root_.replace(root);

    last_intention_processed = i_info.first;
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
    auto root_info = unwritten_roots_.front();
    unwritten_roots_.pop_front();
    auto root = root_info.second;
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
      //std::cout << "intention index insert " << intention_pos << " " << it->second.first << std::endl
      //  << std::flush;
      auto r = intention_to_after_image_pos_.emplace(intention_pos, it->second.first);
      assert(r.second);

      // TODO: so... this is a contention point. there are readers of the state
      // we are now changing... this will need to be a big part of the refactor.
      root->SetDeltaPosition(delta, it->second.first);
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
