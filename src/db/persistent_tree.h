#pragma once
#include "node.h"
#include "db/cruzdb.pb.h"
#include <deque>
#include <sstream>

namespace cruzdb {

class PersistentTree {
 public:
  PersistentTree(DBImpl *db,
      NodePtr root,
      int64_t root_intention,
      int64_t rid,
      uint64_t max_intention_resolvable) :
    db_(db),
    src_root_(root),
    root_intention_(root_intention), // need in tree?
    root_(nullptr),
    rid_(rid),
    max_intention_resolvable_(max_intention_resolvable)
  {}

 public:
  void Put(const zlog::Slice& key, const zlog::Slice& value);
  void Delete(const zlog::Slice& key);
  int Get(const zlog::Slice& key, std::string *value);

  bool EmptyDelta() {
    return root_ == nullptr;
  }

  // serialization and fix-up
 public:
  int infect_self_pointers();
  void SerializeAfterImage(cruzdb_proto::AfterImage& i,
      std::vector<SharedNodeRef>& delta);
  void SetDeltaPosition(std::vector<SharedNodeRef>& delta, uint64_t pos);

  // serialization and fix-up
 private:
  void infect_node_ptr(NodePtr& src, int maybe_offset);
  void infect_node(SharedNodeRef node, int maybe_left_offset, int maybe_right_offset);
  void infect_after_image(SharedNodeRef node, int& field_index);

  void serialize_node_ptr(cruzdb_proto::NodePtr *dst, NodePtr& src,
      int maybe_offset);
  void serialize_node(cruzdb_proto::Node *dst, SharedNodeRef node,
      int maybe_left_offset, int maybe_right_offset);
  void serialize_intention(cruzdb_proto::AfterImage& i,
      SharedNodeRef node, int& field_index,
      std::vector<SharedNodeRef>& delta);


  // tree management
 private:
  static inline NodePtr& left(SharedNodeRef n) { return n->left; };
  static inline NodePtr& right(SharedNodeRef n) { return n->right; };

  static inline SharedNodeRef pop_front(std::deque<SharedNodeRef>& d) {
    auto front = d.front();
    d.pop_front();
    return front;
  }

  SharedNodeRef insert_recursive(std::deque<SharedNodeRef>& path,
      const zlog::Slice& key, const zlog::Slice& value, const SharedNodeRef& node);

  template<typename ChildA, typename ChildB>
  void insert_balance(SharedNodeRef& parent, SharedNodeRef& nn,
      std::deque<SharedNodeRef>& path, ChildA, ChildB, SharedNodeRef& root);

  template <typename ChildA, typename ChildB >
  SharedNodeRef rotate(SharedNodeRef parent, SharedNodeRef child,
      ChildA child_a, ChildB child_b, SharedNodeRef& root);

  SharedNodeRef delete_recursive(std::deque<SharedNodeRef>& path,
      const zlog::Slice& key, const SharedNodeRef& node);

  void transplant(SharedNodeRef parent, SharedNodeRef removed,
      SharedNodeRef transplanted, SharedNodeRef& root);

  SharedNodeRef build_min_path(SharedNodeRef node, std::deque<SharedNodeRef>& path);

  void balance_delete(SharedNodeRef extra_black,
      std::deque<SharedNodeRef>& path, SharedNodeRef& root);

  template<typename ChildA, typename ChildB>
  void mirror_remove_balance(SharedNodeRef& extra_black, SharedNodeRef& parent,
      std::deque<SharedNodeRef>& path, ChildA child_a, ChildB child_b,
      SharedNodeRef& root);

 private:
  DBImpl *db_;

  // root of the tree. will change in future...
 private:
  // database snapshot
  NodePtr src_root_;

  // this is the address of the intention that produced src_root_. todo, make
  // sure this is encoded into src_root when we unify things. a src_root should
  // never exist if it wasn't produced by replaying some intention that does
  // have a physical address.
  int64_t root_intention_;

  // transaction after image
  SharedNodeRef root_;
  const int64_t rid_;


  // cache system related. will change in future...
 private:
  class TraceApplier {
   public:
    explicit TraceApplier(PersistentTree *tree) :
      tree_(tree)
    {}

    ~TraceApplier() {
      tree_->UpdateLRU();
    }

   private:
    PersistentTree *tree_;
  };

  int64_t max_intention_resolvable_;

  // access trace used to update lru cache. the trace is applied and reset
  // after each operation (e.g. get/put/etc) or if the transaction accesses
  // the cache to resolve a pointer (e.g. accessing the log).
  std::vector<std::pair<int64_t, int>> trace_;
  void UpdateLRU();

  // keep new nodes alive for the duration of the transaction until we
  // construct the intention. this is needed because NodePtr contains weak_ptr
  // so new NodeRef nodes (see: insert_recursive) just disappear, and we can't
  // let that happen because we don't store them in the the log or any other
  // type of cache. future options:
  //
  //   1. use a SharedNodePtr type in transactions
  //   2. probably better: integrate some sort of cache so that we can support
  //   transactions that are really large
  //
  std::vector<SharedNodeRef> fresh_nodes_;

  // TODO: so it can grab the root. this is only temporary for the parallel txn processing work.
  friend class DBImpl;
};

}
