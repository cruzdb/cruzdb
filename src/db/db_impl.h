#pragma once
#include <cassert>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <stack>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zlog/log.h>

#include "iterator_impl.h"
#include "db/cruzdb.pb.h"
#include "node.h"
#include "node_cache.h"
#include "snapshot.h"
#include "transaction_impl.h"
#include "cruzdb/db.h"

namespace cruzdb {

std::ostream& operator<<(std::ostream& out, const SharedNodeRef& n);
std::ostream& operator<<(std::ostream& out, const cruzdb_proto::NodePtr& p);
std::ostream& operator<<(std::ostream& out, const cruzdb_proto::Node& n);
std::ostream& operator<<(std::ostream& out, const cruzdb_proto::AfterImage& i);

class DBImpl : public DB {
 public:
  explicit DBImpl(zlog::Log *log);
  ~DBImpl();

  // TODO: return unique ptr?
  Transaction *BeginTransaction() override;

  Snapshot *GetSnapshot() override {
    std::lock_guard<std::mutex> l(lock_);
    //std::cout << "get snapshot: " << root_.csn() << std::endl;
    return new Snapshot(this, root_);
  }

  void ReleaseSnapshot(Snapshot *snapshot) override {
    delete snapshot;
  }

  Iterator *NewIterator(Snapshot *snapshot) override {
    return new IteratorImpl(snapshot);
  }

  int Get(const zlog::Slice& key, std::string *value) override;

  int RestoreFromLog();

 private:
  friend class PersistentTree; // TODO: get rid of this! its only for updatelru
  friend class NodeCache;
  friend class NodePtr;
  friend class IteratorTraceApplier;

  int _validate_rb_tree(SharedNodeRef root);
  void validate_rb_tree(NodePtr root);

 public:

  void validate() {
    const auto snapshot = root_;
    validate_rb_tree(snapshot);
  }

 public:
  bool CompleteTransaction(TransactionImpl *txn);
  void AbortTransaction(TransactionImpl *txn);

  void start_reader() {
    log_reader_ = std::thread(&DBImpl::LogReader, this);
  }

  uint64_t IntentionToAfterImage(uint64_t intention_pos) {
    return cache_.IntentionToAfterImage(intention_pos);
  }

 private:
  struct TransactionWaiter {
    TransactionWaiter() :
      pos(-1),
      committed(false),
      complete(false)
    {}

    int64_t pos;
    bool committed;
    bool complete;
    std::condition_variable cond;
  };

  SharedNodeRef fetch(std::vector<NodeAddress>& trace,
      boost::optional<NodeAddress>& address) {
    return cache_.fetch(trace, address);
  }

  void UpdateLRU(std::vector<NodeAddress>& trace) {
    cache_.UpdateLRU(trace);
  }

  std::mutex lock_;
  zlog::Log *log_;
  NodeCache cache_;
  bool stop_;

  // transaction handling
 private:
  // handles a transaction intention by making a commit/abort decision, and
  // producing a new last-committed-state for the database. after images are
  // queued for handling by the transaction writer.
  void TransactionProcessor();

  // schedules the serialization and writing of in-memory after images produced
  // by the transaction processor when committing an intention from the log.
  void TransactionWriter();

  // handle transaction after images. it applies log position updates to pending
  // after images, writes completed after images to the log, and folds completed
  // images into the node cache.
  void TransactionFinisher();

  // process the log in order. dispatch entries to handlers.
  void LogReader();

  // fifo queue of transaction intentions read from the log. these are processed
  // in order by the transaction processor.
  std::list<std::pair<uint64_t, cruzdb_proto::Intention>> pending_intentions_;
  std::condition_variable pending_intentions_cond_;

  // the set of transactions, initiated by this database instance, indexed by
  // their log position that are waiting on a commit/abort decision. note that
  // other nodes in a system may submit transactions to the log, so when
  // processing transaction intentions, it isn't required that the transaction
  // be found in this data structure.
  uint64_t max_pending_txn_pos_ = 0;
  std::unordered_map<uint64_t, TransactionWaiter*> pending_txns_;

  uint64_t log_reader_pos;
  bool reader_initialized = false;
  int64_t last_intention_processed = -1;

  int64_t in_flight_txn_rid_;
  std::atomic<uint64_t> txn_token;

  // the set of after image roots produced by committed transactions, but which
  // do not yet have a known serialization location in the log. they remain
  // uncached until their physical location is known and set.
  //
  // TODO: why is this a transaction? we use the txn as a container for
  // the root which replayed the tranaction actions against. it also serves to
  // keep the nodes in the after image alive since the after image is not added
  // to the cache. we definitely want to get rid of this use of the transaciton
  // object sooner rather than later.
  //
  // TODO: remove... i thought we wanted multiple queues, but we got away with
  // just the unwritten_roots_ queue, below...
  //std::list<std::shared_ptr<TransactionImpl>> uncached_roots_;

  // the set of after image roots produced by processing transactions that
  // commit. the transaction writer uses this to drive after image writing.
  // TODO: scheduling (when to write, if to write) is a big part of the work the
  // transaction writer does. also, it should be possible in principle to unify
  // this set with the uncached_roots_ set...
  std::list<std::pair<uint64_t, std::shared_ptr<PersistentTree>>> unwritten_roots_;
  std::condition_variable unwritten_roots_cond_;

  std::list<std::pair<uint64_t, cruzdb_proto::AfterImage>> pending_after_images_;
  std::condition_variable pending_after_images_cond_;

  // after image tree for last committed transaction. this may be a node that
  // has a fully specified phsyical address in which case if it has a node
  // reference it will point into the cache. otherwise, its node reference will
  // be at one of the roots in the uncached_roots_ set.
  //
  // TODO: there is only a basic check for these conditions in NodePtr, but its
  // a lot of overhead. we need to completely revamp this infrastructure to
  // handle all the cases we've encountered since development began and resulted
  // in a bit of a mess.
  NodePtr root_;
  int64_t root_intention_;

  std::set<uint64_t> intention_map_;

  // from the spec "Then, nonstatic data members shall be initialized in the
  // order they were declared in the class definition (again regardless of the
  // order of the mem-initializers)."
  //
  // since the thread will immediately start interacting with this class,
  // everything needs to be initialized. in particular the condition
  // variables.
  std::thread txn_writer_;
  std::thread txn_processor_;
  std::thread txn_finisher_;
  std::thread log_reader_;
};

}
