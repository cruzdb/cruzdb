#pragma once
#include <cassert>
#include <memory>
#include <string>
#include <iostream>
#include <vector>
#include <zlog/slice.h>

namespace cruzdb {

class Node;
using SharedNodeRef = std::shared_ptr<Node>;
using WeakNodeRef = std::weak_ptr<Node>;

class DBImpl;

/*
 * The read-only flag is a temporary hack for enforcing read-only property on
 * the connected Node. What is really needed is a more sophisticated approach
 * that avoids duplicating the read-only flag as well as what is probably some
 * call overhead associated with this design. Overall, this isn't pretty but
 * lets us have confidence in the correctness which is the priority right now.
 * There is probably a lot of overhead always returning copies of the
 * shared_ptr NodeRef.
 *
 * The NodePtr can point to Nil, which is represented by a NodeRef singleton.
 * In this case ref() will resolve to the singleton heap address. Never let
 * the Nil object be freed!
 *
 * In order to handle I/O errors gracefully, including timeouts, etc... we
 * probably will want to allow NodePtr::ref return an error when resolving
 * pointers from storage.
 *
 * Copy assertion should check if we are copying a null pointer that the
 * physical address is defined.
 */
class NodePtr {
 public:

  NodePtr(const NodePtr& other) {
    ref_ = other.ref_;
    offset_ = other.offset_;
    csn_ = other.csn_;
    read_only_ = true;
    db_ = other.db_;
    csn_is_intention_pos_ = other.csn_is_intention_pos_;
  }

  NodePtr& operator=(const NodePtr& other) {
    assert(!read_only());
    ref_ = other.ref_;
    offset_ = other.offset_;
    csn_ = other.csn_;
    db_ = other.db_;
    csn_is_intention_pos_ = other.csn_is_intention_pos_;
    return *this;
  }

  // TODO: now we can copy nodeptr. implications?
  //NodePtr(NodePtr&& other) = delete;
  NodePtr& operator=(NodePtr&& other) & = delete;

  NodePtr(SharedNodeRef ref, DBImpl *db, bool read_only) :
    ref_(ref), csn_(-33), offset_(-44), db_(db), read_only_(read_only),
    csn_is_intention_pos_(false)
  {}

  void replace(const NodePtr& other) {
    ref_ = other.ref_;
    offset_ = other.offset_;
    csn_ = other.csn_;
    read_only_ = true;
    db_ = other.db_;
    csn_is_intention_pos_ = other.csn_is_intention_pos_;
  }

  inline bool read_only() const {
    return read_only_;
  }

  inline void set_read_only() {
    assert(!read_only());
    read_only_ = true;
  }

  inline SharedNodeRef ref(std::vector<std::pair<int64_t, int>>& trace) {
    trace.emplace_back(csn_, offset_);
    while (true) {
      // TODO: ugh... not thread safe with cache
      if (auto ret = ref_.lock()) {
        return ret;
      } else {
        // TODO: this is a check that we've added to catch errors in the
        // development of parallel txn processing. we need to be able to
        // interact with nodeptrs that aren't yet in the cache nor do they have
        // a physical address. so for these we use external control to keep the
        // nodes in memory that aren't physically resolvable so we should never
        // hit this case for those nodes.
        //std::cout << "csn " << csn_ << " off " << offset_ << std::endl;
        assert(csn_ >= 0);
        assert(offset_ >= 0);

        // when a new root is installed right after replaying an intention and
        // producing the in-memory after image, that after image might be GCd
        // following serialization etc... currently we don't touch any of the
        // pointers, and instead do a looksy into the volatile node index
        // here... there are several options. we could also keep nodes alive
        // longer... or fix them up before freeing them. doing this here makes a
        // lot of sense because its a fast index lookup, and in practice the
        // recently released nodes have a top spot in the LRU cache so are
        // unlikely to be freed anyway...
        uint64_t use_pos = csn_;
        if (csn_is_intention_pos_) {
          use_pos = resolve_intention_to_csn(csn_);
          //std::cout << csn_ << " " << use_pos << std::endl;
        }
        ref_ = fetch(use_pos, trace);
      }
    }
  }

  uint64_t resolve_intention_to_csn(uint64_t intention);

  // deference a node without providing a trace. this is used by the db that
  // doesn't maintain a trace. ideally we want to always (or nearly always)
  // have a trace. this is only for convenience in some routines that do
  // things like print the tree.
  inline SharedNodeRef ref_notrace() {
    std::vector<std::pair<int64_t, int>> trace;
    return ref(trace);
  }

  inline void set_ref(SharedNodeRef ref) {
    assert(!read_only());
    ref_ = ref;
  }

  inline int offset() const {
    return offset_;
  }

  inline void set_offset(int offset) {
    assert(!read_only());
    offset_ = offset;
  }

  inline int64_t csn() const {
    return csn_;
  }

  inline void set_csn(int64_t csn) {
    assert(!read_only());
    csn_ = csn;
    csn_is_intention_pos_ = false;
  }

  void set_csn_is_intention_pos() {
    csn_is_intention_pos_ = true;
  }

  // TODO: propogate through constructors and such above...
  bool csn_is_intention_pos() {
    return csn_is_intention_pos_;
  }

  void Print() {
    std::cout << "NodePtr: " << "csn " << csn_ << " offset " 
      << offset_ << " csn_is_intent " << csn_is_intention_pos_ << std::endl;
  }

 private:
  // heap pointer (optional), and log address
  WeakNodeRef ref_;
  int64_t csn_;
  int offset_;


  DBImpl *db_;

  bool read_only_;

  bool csn_is_intention_pos_;

  SharedNodeRef fetch(uint64_t pos, std::vector<std::pair<int64_t, int>>& trace);
};

/*
 * use signed types here and in protobuf so we can see the initial neg values
 */
class Node {
 public:
  NodePtr left;
  NodePtr right;

  // TODO: allow rid to have negative initialization value
  Node(const zlog::Slice& key, const zlog::Slice& val, bool red, SharedNodeRef lr, SharedNodeRef rr,
      uint64_t rid, bool read_only, DBImpl *db) :
    left(lr, db, read_only), right(rr, db, read_only),
    key_(key.data(), key.size()), val_(val.data(), val.size()),
    red_(red), rid_(rid), read_only_(read_only)
  {}

  static SharedNodeRef& Nil() {
    static SharedNodeRef node = std::make_shared<Node>("", "",
        false, nullptr, nullptr, (uint64_t)-22, true, nullptr);
    return node;
  }

  static uint64_t resolve_intention_to_csn(DBImpl *db, uint64_t intention);

  static SharedNodeRef Copy(SharedNodeRef src, DBImpl *db, uint64_t rid,
      int64_t max_intention_resolvable) {
    if (src == Nil())
      return Nil();

    // TODO: we don't need to use the version of ref() that resolves here
    // because the caller will likely only traverse down one side.
    auto node = std::make_shared<Node>(src->key(), src->val(), src->red(),
        src->left.ref_notrace(), src->right.ref_notrace(), rid, false, db);

    if (src->left.csn_is_intention_pos()) {
      assert(src->left.csn() >= 0);
      //std::cout << "Copy:Left:Resolved: intention: "
      //  << src->left.csn() << " max intent resolvable " <<
      //  max_intention_resolvable << std::endl << std::flush;
      if (src->left.csn() <= max_intention_resolvable) {
        //std::cout << "foo" << std::endl << std::flush;
        node->left.set_csn(resolve_intention_to_csn(db, src->left.csn()));
      } else {
        // keep propogating the intention pos
        node->left.set_csn(src->left.csn());
        node->left.set_csn_is_intention_pos();
      }
    } else {
      node->left.set_csn(src->left.csn());
    }
    node->left.set_offset(src->left.offset());

    if (src->right.csn_is_intention_pos()) {
      assert(src->right.csn() >= 0);
      //std::cout << "Copy:Right:Resolved: intention: "
      //  << src->right.csn() << " max intent resolvable " <<
      //  max_intention_resolvable << std::endl;
      if (src->right.csn() <= max_intention_resolvable) {
        //std::cout << "foo" << std::endl << std::flush;
        node->right.set_csn(resolve_intention_to_csn(db, src->right.csn()));
      } else {
        // keep propogating the intention pos
        node->right.set_csn(src->right.csn());
        node->right.set_csn_is_intention_pos();
      }
    } else {
      node->right.set_csn(src->right.csn());
    }
    node->right.set_offset(src->right.offset());

    return node;
  }

  inline bool read_only() const {
    return read_only_;
  }

  inline void set_read_only() {
    assert(!read_only());
    left.set_read_only();
    right.set_read_only();
    read_only_ = true;
  }

  inline bool red() const {
    return red_;
  }

  inline void set_red(bool red) {
    assert(!read_only());
    red_ = red;
  }

  inline void swap_color(SharedNodeRef other) {
    assert(!read_only());
    assert(!other->read_only());
    std::swap(red_, other->red_);
  }

  inline int64_t rid() const {
    return rid_;
  }

  inline void set_rid(int64_t rid) {
    assert(!read_only());
    assert(rid_ < 0);
    assert(rid >= 0);
    rid_ = rid;
  }

  // TODO: return const reference?
  inline zlog::Slice key() const {
    return zlog::Slice(key_.data(), key_.size());
  }

  // TODO: return const reference?
  inline zlog::Slice val() const {
    return zlog::Slice(val_.data(), val_.size());
  }

  inline void steal_payload(SharedNodeRef& other) {
    assert(!read_only());
    assert(!other->read_only());
    key_ = std::move(other->key_);
    val_ = std::move(other->val_);
  }

  size_t ByteSize() {
    return sizeof(*this) + key_.size() + val_.size();
  }

 private:
  std::string key_;
  std::string val_;
  bool red_;
  int64_t rid_;
  bool read_only_;
};

}
