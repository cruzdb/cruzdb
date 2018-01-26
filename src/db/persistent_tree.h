#pragma once
#include "node.h"
#include "db/cruzdb.pb.h"
#include <deque>
#include <sstream>
#include <atomic>

namespace cruzdb {

const std::string PREFIX_USER = "U";
const std::string PREFIX_COMMITTED_INTENTION = "C";

/**
 * rid: this value uniquely identifies a tree delta (a root plus any nodes
 * creates through tree modifications within a single context). tree deltas are
 * created in a few different scenarios which need to be taken into acocunt:
 *
 *   1. An executing txn builds a delta against the last committed state.
 *   2. The delta produced by replaying an intention record.
 *   3. Nodes created by loading them from disk in response to a cache miss.
 *
 * The delta produced in (2) and (3) can be uniquely represented by the
 * non-negative log address of its after image, or the intention. The after
 * image produced in (1) must not conflict with any other executing transaction,
 * or any node accessible via the last committed state.
 *
 * For (2) and (3), since each log position is by definition unique, any delta
 * fully represented by a log position can use the log position to uniqely
 * identify the nodes in the delta. To satisfy (1) let the rid value be a
 * negative integer, which by defintion, will not conflict with any state
 * reproduce by intentions from the log. Further, in-flight transactions always
 * start with a last committed state snapshot, so they are not visible to each
 * other. Thus, each in-flight transaction can simply use -1 as a rid, however,
 * we do produce unique negative values to make debugging easier.
 *
 * To use intention or after image position as the rid? Using the intention is
 * convenient because we don't need to wait for the after image to be written to
 * be able to assign the rid values. But when following a pointer during a cache
 * miss, we follow it to the after image. The intention is currently stored in
 * the serialization of the after image, so using the intention appears to be
 * the best choice.
 */
class PersistentTree {
 public:
  PersistentTree(DBImpl *db, NodePtr root, int64_t rid) :
    db_(db),
    src_root_(root),
    root_(nullptr),
    rid_(rid),
    intention_(boost::none),
    afterimage_(boost::none)
  {}

  PersistentTree(DBImpl *db, NodePtr root, int64_t rid, uint64_t intention) :
    db_(db),
    src_root_(root),
    root_(nullptr),
    rid_(rid),
    intention_(intention),
    afterimage_(boost::none)
  {}

  PersistentTree(const PersistentTree& other) = delete;
  PersistentTree(const PersistentTree&& other) = delete;
  PersistentTree& operator=(const PersistentTree& other) = delete;
  PersistentTree& operator=(const PersistentTree&& other) = delete;

 public:
  void Put(const std::string& prefix, const zlog::Slice& key,
      const zlog::Slice& value) {
    Put(prefix_string(prefix, key.ToString()), value);
  }

  void Delete(const std::string& prefix, const zlog::Slice& key) {
    Delete(prefix_string(prefix, key.ToString()));
  }

  int Get(const std::string& prefix, const zlog::Slice& key,
      std::string *value) {
    return Get(prefix_string(prefix, key.ToString()), value);
  }

  bool ReadOnly() const {
    return root_ == nullptr;
  }

  int64_t rid() const {
    return rid_;
  }

  const SharedNodeRef& Root() const {
    return root_;
  }

  void SetIntention(uint64_t pos) {
    assert(!intention_);
    intention_ = pos;
  }

  uint64_t Intention() const {
    assert(intention_);
    return *intention_;
  }

  void SetAfterImage(uint64_t pos) {
    assert(!afterimage_);
    afterimage_ = pos;
  }

  uint64_t AfterImage() const {
    assert(afterimage_);
    return *afterimage_;
  }

  // serialization and fix-up
 public:
  boost::optional<int> infect_self_pointers(uint64_t intention,
      bool expect_intention_rid);
  void SerializeAfterImage(cruzdb_proto::AfterImage& i,
      uint64_t intention,
      std::vector<SharedNodeRef>& delta);
  void SetDeltaPosition(std::vector<SharedNodeRef>& delta, uint64_t pos);

  // serialization and fix-up
 private:
  void infect_node_ptr(uint64_t intention, NodePtr& src, int maybe_offset);
  void infect_node(SharedNodeRef node, uint64_t intention, int maybe_left_offset, int maybe_right_offset);
  void infect_after_image(SharedNodeRef node, uint64_t intention, int& field_index);

  void serialize_node_ptr(cruzdb_proto::NodePtr *dst, NodePtr& src,
      int maybe_offset);
  void serialize_node(cruzdb_proto::Node *dst, SharedNodeRef node,
      int maybe_left_offset, int maybe_right_offset);
  void serialize_intention(cruzdb_proto::AfterImage& i,
      SharedNodeRef node, int& field_index,
      std::vector<SharedNodeRef>& delta);


  // tree management
 private:
  static std::string prefix_string(const std::string& prefix,
      const std::string& value) {
    auto out = prefix;
    out.push_back(0);
    out.append(value);
    return out;
  }

  void Put(const zlog::Slice& key, const zlog::Slice& value);
  void Delete(const zlog::Slice& key);
  int Get(const zlog::Slice& key, std::string *value);

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

  // transaction after image
  SharedNodeRef root_;
  int64_t rid_;

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

  boost::optional<uint64_t> intention_;
  boost::optional<uint64_t> afterimage_;

  // access trace used to update lru cache. the trace is applied and reset
  // after each operation (e.g. get/put/etc) or if the transaction accesses
  // the cache to resolve a pointer (e.g. accessing the log).
  std::vector<NodeAddress> trace_;
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

};

}
