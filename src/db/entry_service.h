#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <boost/optional.hpp>
#include <zlog/log.h>
#include "db/intention.h"

namespace cruzdb {

// one critical method for increasing performance is to eliminate as much
// blocking on io as possible. for example, when a transaction finishes it
// writes an intention to the log. once that intention is written it becomes
// immutable. so during replay, it doesn't matter if the source of an intention
// is the log or an already existing, identical version in memory. the same
// holds true for any other type of entry in the log. the entry cache is a write
// through cache that can be consulted before going to the log for a particular
// log entry.
//
// there is potential for some very interesting enhancements to the entry cache.
// for example, in addition to an in-memory cache being a single layer above the
// log, we can introduce a cache on local devices using something like lmdb.
// another enhancement is to create a distributed cache connected to peer nodes.
// when multiple nodes are submitting transactions, every other node needs to
// read intentions in log order. however, it may be orders of magnitude faster
// to retreive / push entries across the network using rdma than go to the log.
class EntryCache {
 public:
  void Insert(std::unique_ptr<Intention> intention);
  boost::optional<Intention> FindIntention(uint64_t pos);

 private:
  std::mutex lock_;
  std::map<uint64_t, std::unique_ptr<Intention>> intentions_;
};

class EntryService {
 public:
  EntryService(zlog::Log *log, uint64_t pos,
      std::mutex *db_lock);

  void Stop();

 public:
  int AppendIntention(std::unique_ptr<Intention> intention, uint64_t *pos);

  class IntentionQueue {
   public:
    explicit IntentionQueue(uint64_t pos);

    void Stop();
    boost::optional<Intention> Wait();

   private:
    friend class EntryService;

    uint64_t Position() const;
    void Push(Intention intention);

    mutable std::mutex lock_;
    uint64_t pos_;
    bool stop_;
    std::queue<Intention> q_;
    std::condition_variable cond_;
  };

  // create a non-owning intention queue. the queue can be used to access all
  // intentions in their log order, starting at the provided position. the queue
  // will remain valid until the entry service is destroyed.
  IntentionQueue *NewIntentionQueue(uint64_t pos);

  std::list<std::pair<uint64_t, cruzdb_proto::AfterImage>> pending_after_images_;
  std::condition_variable pending_after_images_cond_;

 private:
  void Run();
  void IntentionReader();

  zlog::Log *log_;
  uint64_t pos_;
  bool stop_;
  std::mutex lock_;

  std::mutex *db_lock_;

  std::list<std::unique_ptr<IntentionQueue>> intention_queues_;

  EntryCache cache_;

  std::thread log_reader_;
  std::thread intention_reader_;
};

}
