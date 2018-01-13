#pragma once
#include <cassert>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <memory>
#include <random>
#include <mutex>
#include <set>
#include <stack>
#include <thread>
#include <unordered_map>
#include <vector>
#include <boost/optional.hpp>

#include <zlog/log.h>

#include "iterator_impl.h"
#include "db/cruzdb.pb.h"
#include "node.h"
#include "node_cache.h"
#include "snapshot.h"
#include "transaction_impl.h"
#include "cruzdb/db.h"
#include "db/entry_service.h"

namespace cruzdb {

class DBImpl : public DB {
 public:
  // point in log from which to restore a database instance
  struct RestorePoint {
    uint64_t replay_start_pos;
    uint64_t after_image_pos;
    cruzdb_proto::AfterImage after_image;
  };

  // find the latest point in the log that can be used to restore a database
  // instance. the returned RestorePoint can be passed to the DBImpl
  // constructor. then use WaitOnIntention to wait until the database has rolled
  // the log forward.
  static int FindRestorePoint(zlog::Log *log, RestorePoint& point,
      uint64_t& latest_intention);

  DBImpl(zlog::Log *log, const RestorePoint& point);

  ~DBImpl();

  void WaitOnIntention(uint64_t pos);

  // exported DB interface
 public:
  Transaction *BeginTransaction() override;
  Snapshot *GetSnapshot() override;
  void ReleaseSnapshot(Snapshot *snapshot) override;
  Iterator *NewIterator(Snapshot *snapshot) override;
  int Get(const zlog::Slice& key, std::string *value) override;


  // verify that the root tree is a red-black tree
 public:
  void Validate();

 private:
  int Validate(SharedNodeRef root);

  // caching
 public:
  void UpdateLRU(std::vector<NodeAddress>& trace);
  uint64_t IntentionToAfterImage(uint64_t intention_pos);
  SharedNodeRef fetch(std::vector<NodeAddress>& trace,
      boost::optional<NodeAddress>& address);

  // transaction processing
 public:
  bool CompleteTransaction(TransactionImpl *txn);

 private:
  class TransactionFinder {
   private:
    struct Waiter {
      Waiter() :
        complete(false),
        pos(boost::none)
      {}

      bool complete;
      bool committed;
      boost::optional<uint64_t> pos;
      std::condition_variable cond;
    };

    struct Rendezvous {
      Rendezvous(Waiter *w) :
        waiters{w}
      {}

      std::list<Waiter*> waiters;
      std::unordered_map<uint64_t, bool> results;
    };

   public:
    class WaiterHandle {
      Waiter waiter;
      std::list<Waiter*>::iterator waiter_it;
      std::unordered_map<uint64_t, Rendezvous>::iterator token_it;
      friend class TransactionFinder;
    };

    TransactionFinder() {
      std::random_device seeder;
      txn_token_engine_ = std::mt19937_64(seeder());
      txn_token_dist_ = std::uniform_int_distribution<uint64_t>(
          std::numeric_limits<uint64_t>::min(),
          std::numeric_limits<uint64_t>::max());
    }

    uint64_t NewToken() {
      std::lock_guard<std::mutex> lk(lock_);
      return txn_token_dist_(txn_token_engine_);
    }

    // register token waiter before appending intention to log
    void AddTokenWaiter(WaiterHandle& waiter, uint64_t token);

    // wait on the intention to be processed
    bool WaitOnTransaction(WaiterHandle& waiter, uint64_t intention_pos);

    // the transaction processor notifies the commit/abort decision
    void Notify(int64_t token, uint64_t intention_pos, bool committed);

   private:
    std::mutex lock_;
    std::unordered_map<uint64_t, Rendezvous> token_waiters_;
    std::mt19937_64 txn_token_engine_;
    std::uniform_int_distribution<uint64_t> txn_token_dist_;
  };

  void NotifyIntention(uint64_t pos);
  void TransactionProcessor();
  bool ProcessConcurrentIntention(const Intention& intention);
  void NotifyTransaction(int64_t token, uint64_t intention_pos, bool committed);
  void ReplayIntention(PersistentTree *tree, const Intention& intention);

 private:
  void TransactionWriter();

 private:
  std::mutex lock_;
  zlog::Log *log_;
  NodeCache cache_;
  bool stop_;

  EntryService entry_service_;

  std::list<std::pair<uint64_t, std::unique_ptr<PersistentTree>>> unwritten_roots_;
  std::condition_variable unwritten_roots_cond_;

  TransactionFinder txn_finder_;
  std::map<uint64_t, std::pair<std::condition_variable*, bool*>> waiting_on_log_entry_;
  EntryService::IntentionQueue *intention_queue_;
  uint64_t last_intention_processed_;
  int64_t in_flight_txn_rid_;
  std::set<uint64_t> intention_map_;

 private:
  // finished transactions indexed by their intention position and used by the
  // transaction processor to avoid replaying serial intentions.
  class FinishedTransactions {
   public:
    std::unique_ptr<PersistentTree> Find(uint64_t ipos);
    void Insert(uint64_t ipos, std::unique_ptr<PersistentTree> tree);
    void Clean(uint64_t last_ipos);

   private:
    mutable std::mutex lock_;
    std::unordered_map<uint64_t,
      std::unique_ptr<PersistentTree>> txns_;
  };

  FinishedTransactions finished_txns_;

  NodePtr root_;
  uint64_t root_snapshot_;

  std::thread txn_writer_;
  std::thread txn_processor_;

  // handles various clean-up tasks. useful for absorbing potentially high
  // latency memory freeing events.
  void JanitorEntry();
  std::condition_variable janitor_cond_;
  std::thread janitor_thread_;
};

}
