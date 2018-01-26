#pragma once
#include <cassert>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <memory>
#include <random>
#include <cstring>
#include <mutex>
#include <set>
#include <stack>
#include <thread>
#include <unordered_map>
#include <vector>
#include <boost/optional.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <CivetServer.h>

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

  struct DBStats {
    uint64_t transactions_started = 0;
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
  boost::optional<uint64_t> IntentionToAfterImage(uint64_t intention_pos);
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
  bool ProcessConcurrentIntention(const Intention& intention);
  void NotifyTransaction(int64_t token, uint64_t intention_pos, bool committed);
  void ReplayIntention(PersistentTree *tree, const Intention& intention);

  // committed intention position cache. this is used by the transaction
  // processor to look-up the position of intentions in a conflict zone. for
  // zones that begin "not too far" in the past, a cache hit is expected. for
  // zones that begin outside the range of the cache, query the database
  // catalog. an lru approach can be taken to integrate intention positions, but
  // the granularity of eviction should be complete ranges rather than indvidual
  // positions that would create a sparse index.
  class CommittedIntentionIndex {
   public:
    void push(uint64_t pos);

    // (first, last] or [X<first, last]
    // ret.second is true if returned range is complete
    std::pair<std::vector<uint64_t>, bool> range(uint64_t first,
        uint64_t last) const;

   private:
    const size_t limit_ = 1000;
    std::set<uint64_t> index_;
  };

  CommittedIntentionIndex committed_intentions_;

 private:
  static std::string prefix_string(const std::string& prefix,
      const std::string& value) {
    auto out = prefix;
    out.push_back(0);
    out.append(value);
    return out;
  }

  mutable std::mutex lock_;
  zlog::Log *log_;
  NodeCache cache_;
  bool stop_;

  EntryService entry_service_;

  std::list<std::unique_ptr<PersistentTree>> lcs_trees_;
  std::condition_variable lcs_trees_cond_;

  TransactionFinder txn_finder_;
  std::map<uint64_t, std::pair<std::condition_variable*, bool*>> waiting_on_log_entry_;
  EntryService::IntentionQueue *intention_queue_;
  uint64_t last_intention_processed_;
  int64_t in_flight_txn_rid_;

 private:
  class MetricsHandler : public CivetHandler {
   public:
    explicit MetricsHandler(DBImpl *db) :
      db_(db)
    {}

    bool handleGet(CivetServer *server, struct mg_connection *conn) {

      DBStats stats = db_->stats();

      std::stringstream out;

      writeCounter(out, "transactions_started",
          stats.transactions_started);

      std::string body = out.str();
      std::string content_type = "text/plain";

      mg_printf(conn,
	  "HTTP/1.1 200 OK\r\n"
	  "Content-Type: %s\r\n",
	  content_type.c_str());
      mg_printf(conn, "Content-Length: %lu\r\n\r\n",
	  static_cast<unsigned long>(body.size()));
      mg_write(conn, body.data(), body.size());

      return true;
    }

   private:
    void writeCounter(std::ostream& out, const std::string& name,
        uint64_t value) {
      out << "# TYPE " << name << " counter" << std::endl;
      out << name << " " << value << std::endl;
    }

    DBImpl *db_;
  };

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

  DBStats stats() const {
    std::lock_guard<std::mutex> lk(lock_);
    return db_stats_;
  }

  FinishedTransactions finished_txns_;

  NodePtr root_;
  uint64_t root_snapshot_;

  void TransactionProcessorEntry();
  std::thread transaction_processor_thread_;

  void AfterImageWriterEntry();
  std::thread afterimage_writer_thread_;

  void AfterImageFinalizerEntry();
  std::thread afterimage_finalizer_thread_;

  void JanitorEntry();
  std::condition_variable janitor_cond_;
  std::thread janitor_thread_;

  CivetServer metrics_http_server_;
  MetricsHandler metrics_handler_;
  struct DBStats db_stats_;
};

}
