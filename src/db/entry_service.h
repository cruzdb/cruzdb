#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <functional>
#include <queue>
#include <thread>
#include <boost/optional.hpp>
#include "db/persistent_tree.h"
#include <zlog/log.h>
#include "db/intention.h"

namespace cruzdb {

class EntryService {
 public:
  explicit EntryService(zlog::Log *log);

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

  template<bool Forward>
  class Iterator {
   public:
    Iterator(EntryService *entry_service, uint64_t pos) :
      pos_(pos),
      stop_(false),
      entry_service_(entry_service)
    {}

    virtual boost::optional<
      std::pair<uint64_t, EntryService::CacheEntry>> NextEntry(bool fill = false)
    {
      const auto pos = advance();
      auto entry = entry_service_->Read(pos, fill);
      if (entry) {
        return std::make_pair(pos, *entry);
      }
      return boost::none;
    }

   private:
    template<bool F = Forward, typename std::enable_if<F>::type* = nullptr>
    inline uint64_t advance() {
      return pos_++;
    }

    template<bool F = Forward, typename std::enable_if<!F>::type* = nullptr>
    inline uint64_t advance() {
      // the way this is setup is that we prep for the next read. so if we read
      // pos 0, then pos goes to 2**64 with wrap around. need assertions in here
      // etc...
      return pos_--;
    }

    uint64_t pos_;
    bool stop_;
    EntryService *entry_service_;
  };

  typedef EntryService::Iterator<false> ReverseIterator;

  ReverseIterator NewReverseIterator(uint64_t pos) {
    return ReverseIterator(this, pos);
  }

  class IntentionIterator : private EntryService::Iterator<true> {
   public:
    IntentionIterator(EntryService *entry_service, uint64_t pos);
    boost::optional<std::shared_ptr<Intention>> Next();
  };

  IntentionIterator NewIntentionIterator(uint64_t pos);

  class AfterImageIterator : private EntryService::Iterator<true> {
   public:
    AfterImageIterator(EntryService *entry_service, uint64_t pos);
    boost::optional<std::pair<uint64_t,
      std::shared_ptr<cruzdb_proto::AfterImage>>> Next();
  };

  AfterImageIterator NewAfterImageIterator(uint64_t pos);

  uint64_t Append(cruzdb_proto::Intention& intention) const;
  uint64_t Append(cruzdb_proto::AfterImage& after_image) const;
  uint64_t Append(std::unique_ptr<Intention> intention);

  // Read an afterimage at the provided position. It is a fatal error if the log
  // does not contain an afterimage at the position.
  std::shared_ptr<cruzdb_proto::AfterImage>
    ReadAfterImage(const uint64_t pos);

  // Read intentions at the provided positions. It is a fatal error if any
  // position does not contain an intention.
  std::vector<std::shared_ptr<Intention>> ReadIntentions(
      const std::vector<uint64_t>& positions);

  boost::optional<CacheEntry> Read(uint64_t pos, bool fill = false);

  uint64_t CheckTail(bool update_max_pos = false);

  void Fill(uint64_t pos) const;

 private:
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
};

}
