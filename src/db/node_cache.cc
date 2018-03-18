#include "node_cache.h"
#include "db_impl.h"
#include <time.h>
#include <deque>
#include <condition_variable>

namespace cruzdb {

void NodeCache::do_vaccum_()
{
  while (true) {
    std::unique_lock<std::mutex> l(lock_);

    cond_.wait(l, [this]{
        return !traces_.empty() || UsedBytes() > cache_size_ || stop_;
    });

    if (stop_)
      return;

    std::list<std::vector<NodeAddress>> traces;
    traces_.swap(traces_);

    l.unlock();

    // apply lru updates
    for (auto trace : traces) {
      for (auto address : trace) {

        // TODO: we'll need to figure out a solution for converting addresses or
        // using another cache index. we don't want to take a lock for every
        // conversion. at the moment, it doesn't really matter: we can drop
        // anything from the cache and the system must still run correctly.
        auto key = std::make_pair<int64_t, int>(
            address.Position(), address.Offset());

        auto slot = pair_hash()(key) % num_slots_;
        auto& shard = shards_[slot];
        auto& nodes_ = shard->nodes;
        auto& nodes_lru_ = shard->lru;

        std::unique_lock<std::mutex> lk(shard->lock);

        auto node_it = nodes_.find(key);
        if (node_it == nodes_.end())
          continue;
        entry& e = node_it->second;
        nodes_lru_.erase(e.lru_iter);
        nodes_lru_.emplace_front(key);
        e.lru_iter = nodes_lru_.begin();
      }
    }

    if (UsedBytes() > cache_size_) {
      ssize_t target_bytes = (UsedBytes() - cache_size_) / num_slots_;
      for (size_t slot = 0; slot < num_slots_; slot++) {
        auto& shard = shards_[slot];
        auto& nodes_ = shard->nodes;
        auto& nodes_lru_ = shard->lru;

        std::unique_lock<std::mutex> lk(shard->lock);

        ssize_t left = target_bytes;
        while (left > 0) {
          if (nodes_.empty())
            break;
          auto key = nodes_lru_.back();
          auto nit = nodes_.find(key);
          assert(nit != nodes_.end());
          used_bytes_ -= nit->second.node->ByteSize();
          left -= nit->second.node->ByteSize();
          nodes_.erase(nit);
          nodes_lru_.pop_back();
          RecordTick(stats_, NODE_CACHE_FREE);
        }
      }
    }
  }
}

uint64_t NodeCache::findAfterImagePosition(
    const boost::optional<NodeAddress>& address)
{
  if (address->IsAfterImage()) {
    return address->Position();
  } else {
    auto tmp = IntentionToAfterImage(address->Position());
    if (tmp) {
      return *tmp;
    } else {
      const auto intention = address->Position();
      const auto pos = intention + 1;
      auto it = db_->entry_service_->NewAfterImageIterator(pos);
      while (true) {
        auto ai = it.Next();
        if (!ai) {
          std::cout << "this is not good. but maybe during shutdown" << std::endl;
          assert(0);
          exit(1);
        }
        // TODO: asynchronsly cache the nodes in any non-target afterimages that
        // are read?
        if (ai->second->intention() == intention) {
          return ai->first;
          break;
        }
      }
    }
  }
}

// when resolving a node we only resolve the single node. figuring out when to
// resolve an entire intention would be interesting.
//
// when fetching an address, it will either be an afterimage address, or it will
// be an intention that resolves to an afterimage. that resolution will always
// succeed because we don't let the in-memory pointer expire until after we've
// created an index entry. when reading nodes from the log, pointers with
// intentions are resolved before being allowed into memory.
SharedNodeRef NodeCache::fetch(std::vector<NodeAddress>& trace,
    boost::optional<NodeAddress>& address,
    const zlog::Slice *key_target)
{
  RecordTick(stats_, NODE_CACHE_FETCHES);

  const uint64_t afterimage = findAfterImagePosition(address);
  const auto offset = address->Offset();

  auto key = std::make_pair(afterimage, offset);

  auto slot = pair_hash()(key) % num_slots_;
  auto& shard = shards_[slot];
  auto& nodes_ = shard->nodes;
  auto& nodes_lru_ = shard->lru;

  std::unique_lock<std::mutex> lk(shard->lock);

  // is the node in the cache?
  auto it = nodes_.find(key);
  if (it != nodes_.end()) {
    RecordTick(stats_, NODE_CACHE_HIT);
    entry& e = it->second;
    nodes_lru_.erase(e.lru_iter);
    nodes_lru_.emplace_front(key);
    e.lru_iter = nodes_lru_.begin();
    return e.node;
  }

  // release lock for I/O
  lk.unlock();

  // publish the lru traces. we are doing this here because if the log read
  // blocks or takes a long time we don't want to reduce the quality of the
  // trace by having it be outdated. how important is this? is it over
  // optimization?
  {
    std::lock_guard<std::mutex> l(lock_);
    if (!trace.empty()) {
      traces_.emplace_front();
      traces_.front().swap(trace);
      cond_.notify_one();
    }
  }

#if 0
  // read all the nodes in the afterimage using the new fine-grained API
  auto ai = db_->entry_service_->ReadAllAfterImageNodes(afterimage);
#endif

#if 0
  // read only the node that is missing
  auto ai = db_->entry_service_->ReadAfterImageNode(afterimage, offset);
#endif

#if 0
  // read only the node that is missing
  auto ai = db_->entry_service_->ReadAfterImageRandomNodes(afterimage, offset, 0.5);
  //std::cout << ai.second.size() << std::endl;
#endif

#if 1
  // read only the node that is missing
  auto ai = db_->entry_service_->ReadAfterImageNodesWithTarget(afterimage, offset,
      key_target);
  //std::cout << ai.second.size() << std::endl;
#endif

#if 0
  // old way is to read the entire afterimage blob up and cache all the nodes
  // inside it. the new way is that we can still do that, but we can also
  // selectively read nodes.
  auto ai = db_->entry_service_->ReadAfterImage(afterimage);
#endif

  // cache all the nodes in the after image then return the one we care about.
  // it's technically possible that after node is cached its removed, but lru
  // should always prevent that. in any case, we handle that expliclty.
#if 1
  CacheAfterImageNodes(ai.second, ai.first, afterimage);
#else
  // this is the version used by reading the afterimage blob. the new version
  // just takes the nodes we read. NOTE that that CacheAfterImage is still used,
  // but just by the db initialization code. since that is done once and then we
  // clear cache for each Get(), it won't affect stats for our experiments.
  CacheAfterImage(*ai, afterimage);
#endif

  RecordTick(stats_, NODE_CACHE_NODES_READ, ai.second.size());

  // its probably there now
  lk.lock();
  it = nodes_.find(key);
  if (it != nodes_.end()) {
    entry& e = it->second;
    nodes_lru_.erase(e.lru_iter);
    nodes_lru_.emplace_front(key);
    e.lru_iter = nodes_lru_.begin();
    return e.node;
  }

  // this is a rare case where it was evicted after caching the nodes and when
  // we looked. its easy to handle, but we're skipping it here for now. blow up
  // if there is an issue.
  assert(0);
  exit(1);
}

// disabling resolution during node deserialization because currently when
// this is called we are holding a lock on a particular cache shard. allowing
// this would require us to take multiple locks at a time (deal with
// deadlock by ordering acquires etc...) or promote this resolution to a
// higher level in the call stack where we could iterate over the new nodes
// and acquire each shard lock without other locks. we'll just come back to
// this optimization later.
//
//void NodeCache::ResolveNodePtr(NodePtr& ptr)
//{
//  auto key = std::make_pair(ptr.csn(), ptr.offset());
//
//  auto slot = pair_hash()(key) % num_slots_;
//  auto& shard = shards_[slot];
//  auto& nodes_ = shard->nodes;
//  auto& nodes_lru_ = shard->lru;
//
//  auto node_it = nodes_.find(key);
//  if (node_it == nodes_.end())
//    return;
//
//  // lru update
//  entry& e = node_it->second;
//  nodes_lru_.erase(e.lru_iter);
//  nodes_lru_.emplace_front(key);
//  e.lru_iter = nodes_lru_.begin();
//
//  ptr.set_ref(e.node);
//}

void NodeCache::CacheAfterImageNodes(std::map<int, cruzdb_proto::Node>& nodes,
    uint64_t intention, uint64_t pos)
{
  if (nodes.empty()) {
    assert(0);
    return;
  }

  for (auto it : nodes) {
    auto nn = deserialize_node(it.second, intention, pos);
    auto key = std::make_pair(pos, it.first);

    auto slot = pair_hash()(key) % num_slots_;
    auto& shard = shards_[slot];
    auto& nodes_ = shard->nodes;
    auto& nodes_lru_ = shard->lru;

    std::unique_lock<std::mutex> lk(shard->lock);

    auto it2 = nodes_.find(key);
    if (it2 != nodes_.end()) {
      continue;
    }

    nodes_lru_.emplace_front(key);
    auto iter = nodes_lru_.begin();
    auto res = nodes_.insert(
        std::make_pair(key, entry{nn, iter}));
    assert(res.second);

    used_bytes_ += nn->ByteSize();
  }
}

NodePtr NodeCache::CacheAfterImage(const cruzdb_proto::AfterImage& i,
    uint64_t pos)
{
  if (i.tree_size() == 0) {
    NodePtr ret(Node::Nil(), nullptr);
    return ret;
  }

  int idx;
  SharedNodeRef nn = nullptr;
  for (idx = 0; idx < i.tree_size(); idx++) {

    // no locking on deserialize_node is OK
    nn = deserialize_node(i.tree(idx), i.intention(), pos);

    auto key = std::make_pair(pos, idx);

    auto slot = pair_hash()(key) % num_slots_;
    auto& shard = shards_[slot];
    auto& nodes_ = shard->nodes;
    auto& nodes_lru_ = shard->lru;

    std::unique_lock<std::mutex> lk(shard->lock);

    auto it = nodes_.find(key);
    if (it != nodes_.end()) {
      // if this was the last node, then make sure when we fall through to the
      // end of the routine that nn points to this node instead of the one
      // that was constructed above.
      nn = it->second.node;
      continue;
    }

    nodes_lru_.emplace_front(key);
    auto iter = nodes_lru_.begin();
    auto res = nodes_.insert(
        std::make_pair(key, entry{nn, iter}));
    assert(res.second);

    used_bytes_ += nn->ByteSize();
  }

  assert(nn != nullptr);
  NodePtr ret(nn, db_);
  ret.SetAfterImageAddress(pos, idx - 1);
  return ret;
}

SharedNodeRef NodeCache::deserialize_node(const cruzdb_proto::Node& n,
    uint64_t intention, uint64_t pos) const
{
  auto nn = std::make_shared<Node>(n.key(), n.val(), n.red(),
      nullptr, nullptr, intention, false, db_);

  if (!n.left().nil()) {
    uint16_t offset = n.left().off();
    if (n.left().self()) {
      nn->left.SetAfterImageAddress(pos, offset);
    } else if (n.left().has_afterimage()) {
      assert(!n.left().has_intention());
      nn->left.SetAfterImageAddress(n.left().afterimage(), offset);
    } else {
      assert(n.left().has_intention());
      nn->left.SetIntentionAddress(n.left().intention(), offset);
    }
  } else {
    nn->left.set_ref(Node::Nil());
  }

  if (!n.right().nil()) {
    uint16_t offset = n.right().off();
    if (n.right().self()) {
      nn->right.SetAfterImageAddress(pos, offset);
    } else if (n.right().has_afterimage()) {
      assert(!n.right().has_intention());
      nn->right.SetAfterImageAddress(n.right().afterimage(), offset);
    } else {
      assert(n.right().has_intention());
      nn->right.SetIntentionAddress(n.right().intention(), offset);
    }
  } else {
    nn->right.set_ref(Node::Nil());
  }

  return nn;
}

NodePtr NodeCache::ApplyAfterImageDelta(
    const std::vector<SharedNodeRef>& delta,
    uint64_t after_image_pos)
{
  if (delta.empty()) {
    NodePtr ret(Node::Nil(), nullptr);
    return ret;
  }

  int offset = 0;
  for (auto nn : delta) {
    nn->set_read_only();

    auto key = std::make_pair(after_image_pos, offset);

    auto slot = pair_hash()(key) % num_slots_;
    auto& shard = shards_[slot];
    auto& nodes_ = shard->nodes;
    auto& nodes_lru_ = shard->lru;

    std::unique_lock<std::mutex> lk(shard->lock);

    nodes_lru_.emplace_front(key);
    auto iter = nodes_lru_.begin();
    auto res = nodes_.insert(
        std::make_pair(key, entry{nn, iter}));
    assert(res.second);
    offset++;

    used_bytes_ += nn->ByteSize();
  }

  auto root = delta.back();
  NodePtr ret(root, db_);
  ret.SetAfterImageAddress(after_image_pos, offset - 1);
  return ret;
}

}
