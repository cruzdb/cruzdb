#pragma once
#include <atomic>
#include <unordered_map>
#include <mutex>
#include <utility>
#include <thread>
#include <list>
#include <condition_variable>
#include <zlog/log.h>
#include "cruzdb/options.h"
#include "node.h"
#include "db/cruzdb.pb.h"
#include "db/lru_cache.hpp"

namespace cruzdb {

class DBImpl;

template <class T>
inline void hash_combine(std::size_t& seed, const T& v)
{
  std::hash<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator () (const std::pair<T1,T2> &p) const {
    auto h1 = std::hash<T1>{}(p.first);
    auto h2 = std::hash<T2>{}(p.second);
    size_t hval = 0;
    hash_combine(hval, h1);
    hash_combine(hval, h2);
    return hval;
  }
};

class NodeCache {
 public:
  NodeCache(const Options& options, zlog::Log *log, DBImpl *db) :
    log_(log),
    db_(db),
    used_bytes_(0),
    stop_(false),
    num_slots_(8),
    cache_size_(options.node_cache_size),
    stats_(options.statistics.get()),
    imap_(options.imap_cache_size)
  {
    for (size_t i = 0; i < num_slots_; i++) {
      shards_.push_back(std::unique_ptr<shard>(new shard));
    }
    vaccum_ = std::thread(&NodeCache::do_vaccum_, this);
  }

  void CacheAfterImageNodes(std::map<int, cruzdb_proto::Node>& nodes,
      uint64_t intention, uint64_t pos);
  NodePtr CacheAfterImage(const cruzdb_proto::AfterImage& i,
      uint64_t pos);
  NodePtr ApplyAfterImageDelta(const std::vector<SharedNodeRef>& delta,
      uint64_t after_image_pos);

  uint64_t findAfterImagePosition(
      const boost::optional<NodeAddress>& address);

  SharedNodeRef fetch(std::vector<NodeAddress>& trace,
      boost::optional<NodeAddress>& address);

  boost::optional<uint64_t> IntentionToAfterImage(uint64_t intention_pos) {
    std::lock_guard<std::mutex> l(lock_);
    return imap_.get(intention_pos);
  }

  void SetIntentionMapping(uint64_t intention_pos,
      uint64_t after_image_pos) {
    std::lock_guard<std::mutex> l(lock_);
    imap_.insert(intention_pos, after_image_pos);
  }

  void Stop() {
    lock_.lock();
    stop_ = true;
    lock_.unlock();
    cond_.notify_one();
    vaccum_.join();
  }

  void UpdateLRU(std::vector<NodeAddress>& trace) {
    if (!trace.empty()) {
      std::lock_guard<std::mutex> l(lock_);
      traces_.emplace_front();
      traces_.front().swap(trace);
      cond_.notify_one();
    }
  }

  // drop everything in the cache. this also tries to clear out all of the
  // pending traces too, but that tough to guarantee if racing with the vaccum.
  void Clear() {
    cond_.notify_one();
    {
      std::lock_guard<std::mutex> l(lock_);
      imap_.clear();
      traces_.clear();
    }
    for (size_t slot = 0; slot < num_slots_; slot++) {
      auto& shard = shards_[slot];
      auto& nodes_ = shard->nodes;
      auto& nodes_lru_ = shard->lru;
      std::unique_lock<std::mutex> lk(shard->lock);
      while (!nodes_.empty()) {
        auto key = nodes_lru_.back();
        auto nit = nodes_.find(key);
        assert(nit != nodes_.end());
        used_bytes_ -= nit->second.node->ByteSize();
        nodes_.erase(nit);
        nodes_lru_.pop_back();
      }
    }
  }

 private:
  zlog::Log *log_;
  DBImpl *db_;
  std::mutex lock_;
  std::atomic_size_t used_bytes_;
  bool stop_;
  const size_t num_slots_;
  const size_t cache_size_;
  Statistics *stats_;

  struct entry {
    SharedNodeRef node;
    std::list<std::pair<uint64_t, int>>::iterator lru_iter;
  };

  struct shard {
    std::mutex lock;
    std::unordered_map<std::pair<uint64_t, int>, entry, pair_hash> nodes;
    std::list<std::pair<uint64_t, int>> lru;
  };

  std::vector<std::unique_ptr<shard>> shards_;

  size_t UsedBytes() const {
    return used_bytes_;
  }

  std::list<std::vector<NodeAddress>> traces_;

  lru_cache<uint64_t, uint64_t> imap_;

  SharedNodeRef deserialize_node(const cruzdb_proto::Node& n,
      uint64_t intention, uint64_t pos) const;

  std::thread vaccum_;
  std::condition_variable cond_;
  void do_vaccum_();
};

}
