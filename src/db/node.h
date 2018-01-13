#pragma once
#include <cassert>
#include <memory>
#include <string>
#include <iostream>
#include <mutex>
#include <vector>
#include <boost/optional.hpp>
#include <zlog/slice.h>

namespace cruzdb {

class Node;
using SharedNodeRef = std::shared_ptr<Node>;
using WeakNodeRef = std::weak_ptr<Node>;

class DBImpl;

// The physical address of a tree node in the log. All nodes are stored in a
// serialized form of an after image. The offset points to the location of the
// node in the serialized after image. The position points to an addressable
// location in the log. The position may point to an after image, or it may
// point to an intention. When pointing at an intention, the corresponding after
// image containing the target node is the first after image in the log,
// following the intention, that is associated with the intention. Multiple
// after images may exist for the same intention, and all except the first may
// be ignored.
class NodeAddress {
 public:
  NodeAddress(uint64_t position, uint16_t offset, bool is_afterimage) :
    position_(position), offset_(offset), is_afterimage_(is_afterimage)
  {}

  uint64_t Position() const {
    return position_;
  }

  uint16_t Offset() const {
    return offset_;
  }

  bool IsAfterImage() const {
    return is_afterimage_;
  }

 private:
  uint64_t position_;
  uint16_t offset_;
  bool is_afterimage_;
};

/*
 * TODO: add locking to various constructors
 * TODO: formalize rel between Nil and address = boost::none
 */
class NodePtr {
 public:
  NodePtr(const NodePtr& other) {
    ref_ = other.ref_;
    db_ = other.db_;
    address_ = other.address_;
  }

  NodePtr& operator=(const NodePtr& other) {
    ref_ = other.ref_;
    db_ = other.db_;
    address_ = other.address_;
    return *this;
  }

  // TODO: now we can copy nodeptr. implications?
  //NodePtr(NodePtr&& other) = delete;
  NodePtr& operator=(NodePtr&& other) & = delete;

  // TODO: remove read_only
  NodePtr(SharedNodeRef ref, DBImpl *db) :
    ref_(ref),
    address_(boost::none),
    db_(db)
  {}

  void replace(const NodePtr& other) {
    ref_ = other.ref_;
    db_ = other.db_;
    address_ = other.address_;
  }

  // TODO: tracing
  inline SharedNodeRef ref(std::vector<NodeAddress>& trace) {
    std::unique_lock<std::mutex> lk(lock_);
    if (address_) {
      trace.emplace_back(*address_);
    }
    while (true) {
      if (auto ret = ref_.lock()) {
        return ret;
      } else {
        assert(address_);
        auto address = address_;
        lk.unlock();
        auto node = fetch(address, trace);
        lk.lock();
        if (auto ret = ref_.lock()) {
          return ret;
        } else {
          ref_ = node;
        }
      }
    }
  }

  // deference a node without providing a trace. this is used by the db that
  // doesn't maintain a trace. ideally we want to always (or nearly always)
  // have a trace. this is only for convenience in some routines that do
  // things like print the tree.
  inline SharedNodeRef ref_notrace() {
    std::vector<NodeAddress> trace;
    return ref(trace);
  }

  inline void set_ref(SharedNodeRef ref) {
    std::lock_guard<std::mutex> l(lock_);
    ref_ = ref;
  }

  boost::optional<NodeAddress> Address() const {
    std::lock_guard<std::mutex> l(lock_);
    return address_;
  }

  void SetAddress(boost::optional<NodeAddress> address) {
    std::lock_guard<std::mutex> l(lock_);
    assert(!address_);
    address_ = address;
  }

  void SetIntentionAddress(uint64_t position, uint16_t offset) {
    std::lock_guard<std::mutex> l(lock_);
    address_ = NodeAddress(position, offset, false);
  }

  void SetAfterImageAddress(uint64_t position, uint16_t offset) {
    std::lock_guard<std::mutex> l(lock_);
    assert(!address_);
    address_ = NodeAddress(position, offset, true);
  }

  void ConvertToAfterImage(uint64_t position) {
    std::lock_guard<std::mutex> l(lock_);
    assert(address_);
    assert(!address_->IsAfterImage());
    assert(address_->Position() < position);
    address_ = NodeAddress(position, address_->Offset(), true);
  }

 private:
  mutable std::mutex lock_;

  WeakNodeRef ref_;
  boost::optional<NodeAddress> address_;

  DBImpl *db_;

  SharedNodeRef fetch(boost::optional<NodeAddress>& address,
      std::vector<NodeAddress>& trace);
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
    left(lr, db), right(rr, db),
    key_(key.data(), key.size()), val_(val.data(), val.size()),
    red_(red), rid_(rid), read_only_(read_only)
  {}

  static SharedNodeRef& Nil() {
    // TODO: in a redesign, it would be nice to get rid of Nil being represented
    // like this, especially the weird min rid value.
    static SharedNodeRef node = std::make_shared<Node>("", "",
        false, nullptr, nullptr, std::numeric_limits<int64_t>::min(), true, nullptr);
    return node;
  }

  static SharedNodeRef Copy(SharedNodeRef src, DBImpl *db, uint64_t rid) {
    if (src == Nil())
      return Nil();

    // TODO: we don't need to use the version of ref() that resolves here
    // because the caller will likely only traverse down one side.
    auto node = std::make_shared<Node>(src->key(), src->val(), src->red(),
        src->left.ref_notrace(), src->right.ref_notrace(), rid, false, db);

    // TODO: move this into the constructor
    node->left.SetAddress(src->left.Address());
    node->right.SetAddress(src->right.Address());

#if 0
    if (src->left.csn_is_intention_pos()) {
      assert(src->left.csn() >= 0);
      //std::cout << "Copy:Left:Resolved: intention: "
      //  << src->left.csn() << " max intent resolvable " <<
      //  max_intention_resolvable << std::endl << std::flush;
      if (src->left.csn() <= max_intention_resolvable) {
        //std::cout << "foo" << std::endl << std::flush;
        try {
          node->left.set_csn(resolve_intention_to_csn(db, src->left.csn()));
        } catch (const std::out_of_range& oor) {
          std::cout << "resolve left csn " <<
            src->left.csn() << " max intent resolv " <<
            max_intention_resolvable << std::endl;
          throw;
        }
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
        try {
          node->right.set_csn(resolve_intention_to_csn(db, src->right.csn()));
        } catch (const std::out_of_range& oor) {
          std::cout << "resolve right csn " <<
            src->right.csn() << " max intent resolv " <<
            max_intention_resolvable << std::endl;
          throw;
        }
      } else {
        // keep propogating the intention pos
        node->right.set_csn(src->right.csn());
        node->right.set_csn_is_intention_pos();
      }
    } else {
      node->right.set_csn(src->right.csn());
    }
    node->right.set_offset(src->right.offset());
#endif

    return node;
  }

  inline bool read_only() const {
    return read_only_;
  }

  inline void set_read_only() {
    assert(!read_only());
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
