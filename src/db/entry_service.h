#pragma once
#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <boost/optional.hpp>
#include <zlog/log.h>
#include "cruzdb/options.h"
#include "db/persistent_tree.h"
#include "db/intention.h"
#include "monitoring/statistics.h"

namespace cruzdb {

class EntryService {
 public:
  EntryService(const Options& options, Statistics *statistics, zlog::Log *log);

  // TODO: add a safety mechanism to ensure parts of the interface are not used
  // unless it has been started. Or actually make part of the service static
  // interfaces, and then create a constructor that requires the starting
  // position to be specified.
  void Start(uint64_t pos);
  void Stop();

 public:
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

  class CacheEntry {
   public:
    enum EntryType {
      INTENTION,
      AFTERIMAGE,
      FILLED
    };

    EntryType type;
    std::shared_ptr<Intention> intention;
    std::shared_ptr<cruzdb_proto::AfterImage> after_image;
  };

  class Iterator {
   public:
    Iterator(EntryService *entry_service, uint64_t pos,
        const std::string& name);

    virtual ~Iterator() {}

    virtual uint64_t advance() = 0;

    boost::optional<
      std::pair<uint64_t, EntryService::CacheEntry>>
      NextEntry(bool fill = false);

   protected:
    uint64_t pos_;

   private:
    EntryService *entry_service_;
    const std::string name_;
  };

  class ReverseIterator : public EntryService::Iterator {
   public:
    ReverseIterator(EntryService *entry_service, uint64_t pos,
        const std::string& name);
    virtual uint64_t advance() override;
  };

  class ForwardIterator : public EntryService::Iterator {
   public:
    ForwardIterator(EntryService *entry_service, uint64_t pos);
    virtual uint64_t advance() override;
  };

  class IntentionIterator : private EntryService::ForwardIterator {
   public:
    IntentionIterator(EntryService *entry_service, uint64_t pos);
    boost::optional<std::shared_ptr<Intention>> Next();
  };

  class AfterImageIterator : private EntryService::ForwardIterator {
   public:
    AfterImageIterator(EntryService *entry_service, uint64_t pos);
    boost::optional<std::pair<uint64_t,
      std::shared_ptr<cruzdb_proto::AfterImage>>> Next();
  };

  ReverseIterator NewReverseIterator(uint64_t pos, const std::string& name);
  IntentionIterator NewIntentionIterator(uint64_t pos);
  AfterImageIterator NewAfterImageIterator(uint64_t pos);

  uint64_t Append(cruzdb_proto::Intention& intention) const;
  uint64_t Append(cruzdb_proto::AfterImage& after_image) const;
  uint64_t Append(std::unique_ptr<Intention> intention);

  // Read an afterimage at the provided position. It is a fatal error if the log
  // does not contain an afterimage at the position.
  std::shared_ptr<cruzdb_proto::AfterImage>
    ReadAfterImage(const uint64_t pos);

  std::pair<uint64_t, std::map<int, cruzdb_proto::Node>>
    ReadAfterImageRandomNodes(uint64_t pos, int offset, float f) const;
  std::pair<uint64_t, std::map<int, cruzdb_proto::Node>>
    ReadAllAfterImageNodes(uint64_t pos) const;
  std::pair<uint64_t, std::map<int, cruzdb_proto::Node>>
    ReadAfterImageNode(uint64_t pos, int offset) const;

  // Read intentions at the provided positions. It is a fatal error if any
  // position does not contain an intention.
  std::vector<std::shared_ptr<Intention>> ReadIntentions(
      const std::vector<uint64_t>& positions);

  boost::optional<CacheEntry> Read(uint64_t pos, bool fill = false);

  uint64_t CheckTail(bool update_max_pos = false);

  void Fill(uint64_t pos) const;

  void ClearCaches() {
    std::unique_lock<std::mutex> lk(lock_);
    entry_cache_.clear();
  }

 private:
  Statistics *stats_;

  void IOEntry();
  uint64_t Append(const std::string& data) const;

  // this still needs a lot of work. we are just removing older log entries, but
  // this doesn't necessarily correspond to any sort of real lru policy just as
  // an exmaple.
  std::map<uint64_t, CacheEntry> entry_cache_;
  void entry_cache_gc();

  zlog::Log *log_;
  uint64_t pos_;
  bool stop_;
  std::mutex lock_;

  uint64_t max_pos_;
  std::list<std::condition_variable*> tail_waiters_;

  std::thread io_thread_;
  const size_t cache_size_;
};

}
