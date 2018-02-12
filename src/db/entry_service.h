#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <boost/optional.hpp>
#include "db/persistent_tree.h"
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
  explicit EntryService(zlog::Log *log);

  void Start(uint64_t pos);
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

  // read the intentions in the provided set. this interface should be
  // asychronous: the caller doesn't need the results in order, nor as a
  // complete result set.
  std::list<Intention> ReadIntentions(std::vector<uint64_t> addrs);

  // matches intentions with their primary afterimage in the log
  class PrimaryAfterImageMatcher {
   public:
    PrimaryAfterImageMatcher();

    // watch for an intention's afterimage.
    //
    // intention watches MUST be set strictly in the order that intentions
    // appear in the log, but many of inflight watches can be active at once.
    // the reason is related to how we garbage collect the de-duplication index.
    //
    // we need to identify two things. the first is a threshold below which the
    // de-duplication index can ignore new afterimages that show up in the log.
    // this point is the minimum position below which all intentions have been
    // matched up with their after images. the second is a threshold that lets
    // the GC process know that no new intention watches will be added. since we
    // are adding the intentions in strict log order, then for any point in the
    // index we know that below that point the index is complete.
    void watch(std::vector<SharedNodeRef> delta,
        std::unique_ptr<PersistentTree> intention);

    // add an afterimage from the log
    void push(const cruzdb_proto::AfterImage& ai, uint64_t pos);

    // get intention/afterimage match
    std::pair<
      std::vector<SharedNodeRef>,
      std::unique_ptr<PersistentTree>> match();

    // notify stream consumers
    void shutdown();

   private:
    // (pos, nullptr)  -> after image, no intention waiter
    // (none, set)     -> intention waiter, no after image
    // (none, nullptr) -> matched. can be removed from index
    struct PrimaryAfterImage {
      boost::optional<uint64_t> pos;
      std::unique_ptr<PersistentTree> tree;
      std::vector<SharedNodeRef> delta;
    };

    // gc the dedup index
    void gc();

    std::mutex lock_;
    bool shutdown_;
    uint64_t matched_watermark_;
    std::condition_variable cond_;

    // rendezvous point and de-duplication index
    // intention position --> primary after image
    std::map<uint64_t, PrimaryAfterImage> afterimages_;

    // intentions matched with primary after image
    std::list<std::pair<std::vector<SharedNodeRef>,
      std::unique_ptr<PersistentTree>>> matched_;
  };

  PrimaryAfterImageMatcher ai_matcher;

  void AppendAfterImageAsync(const std::string& blob);

 private:
  void Run();
  void IntentionReader();

  zlog::Log *log_;
  uint64_t pos_;
  bool stop_;
  std::mutex lock_;

  std::list<std::unique_ptr<IntentionQueue>> intention_queues_;

  EntryCache cache_;

  std::thread log_reader_;
  std::thread intention_reader_;
};

}
