/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#pragma once
#include <atomic>
#include <map>
#include <cassert>
#include <memory>
#include <stack>
#include <string>
#include <utility>
#include <cstddef>
#include <boost/optional.hpp>

struct OpContext {
  const uint64_t rid;
};

class Entry {
 public:
  Entry(const std::string& key, const std::string& value) :
    key(key),
    value(value),
    refcount_(1)
  {}

  inline void get() const {
    assert(refcount_ > 0);
    refcount_++;
  }

  inline void put() const {
    assert(refcount_ > 0);
    if (refcount_.fetch_sub(1) == 1) {
      delete this;
    }
  }

 public:
  const std::string key;
  const std::string value;

 private:
  mutable std::atomic<uint64_t> refcount_;
};

class Node;

class NodePointer {
 public:
  NodePointer() :
    ptr_(nullptr)
  {}

  NodePointer(const Node *ptr) :
    ptr_(ptr)
  {}

  auto operator()() const {
    return ptr_;
  }

  NodePointer copy() const;

 private:
  const Node *ptr_;
};

// even though everything is const, and copied instead of mutated do we ever
// need any memory barriers or enforcement of certain consistency?
//
// in som einstances like insert, we could relax copy-only and mutate to save
// some cycles.
class Node {
 private:
  template<typename RT>
    static inline auto get(RT item) {
      if (item) {
        item->get();
      }
      return item;
    }

  // caller ensures correct reference counts
  static inline const Node * _makeNode(const OpContext& ctx, const bool red,
      const Entry * const entry,
      const NodePointer& left,
      const NodePointer& right) {
    return new Node(red, entry, left, right, ctx.rid);
  }

  // bumps all reference counts
  static inline const Node * makeNode(const OpContext& ctx, const bool red,
      const Entry * const entry,
      const NodePointer& left,
      const NodePointer& right) {

    // bumping the reference on the underlying node, if it exists. this could
    // also be done in a copy constructor automatically, but that's later when
    // all the cases have been working through explicitly.
    if (auto lp = left(); lp) {
      lp->get();
    }

    if (auto rp = right(); rp) {
      rp->get();
    }

    assert(entry);
    entry->get();

    return _makeNode(ctx, red, entry, left, right);
  }

  // caller ensure correct left/right reference counts
  static inline auto makeNodeWithEntry(const OpContext& ctx, const bool red,
      const std::string& key, const std::string& value,
      const NodePointer& left, const NodePointer& right) {
    const auto entry = new Entry(key, value);
    return _makeNode(ctx, red, entry, left, right);
  }

 private:
  inline auto copyWithEntry(const OpContext& ctx, const std::string& key,
      const std::string& value) const {
    // copies of left and right are made. the original left/right might be
    // modified by a gc so we need ot makre the point where this thread takes
    // ownership of a copy which no gc can get to. actually maybe that isn't an
    // issue because the gc thread doesn't bother with ref counts. so if we make
    // bump the reference and then grab a copy, and then its gcd that's fine.
    return makeNodeWithEntry(ctx, red, key, value, left.copy(), right.copy());
  }

  // steals new_left reference
  inline auto __copyWithLeft(const OpContext& ctx,
      const Node * const new_left) const {
    return _makeNode(ctx, red, get(entry), new_left, right.copy());
  }

  // steals new_right reference
  inline auto __copyWithRight(const OpContext& ctx,
      const Node * const new_right) const {
    return _makeNode(ctx, red, get(entry), left.copy(), new_right);
  }

  inline auto copyAsRed(const OpContext& ctx) const {
    return makeNode(ctx, true, entry, left, right);
  }

 public:
  inline auto copyAsBlack(const OpContext& ctx) const {
    return makeNode(ctx, false, entry, left, right);
  }

 private:
  Node(const bool red,
      const Entry * const entry,
      const NodePointer& left,
      const NodePointer& right,
      const uint64_t rid) :
    red(red),
    entry(entry),
    left(left),
    right(right),
    rid(rid),
    refcount_(1)
  {}

  Node(const Node& other) = delete;
  Node(Node&& other) = delete;
  Node& operator=(const Node& other) = delete;
  Node& operator=(Node&& other) = delete;
  ~Node() = default;

 public:
  inline void get() const {
    assert(refcount_ > 0);
    refcount_++;
  }

  inline void put() const {
    assert(refcount_ > 0);
    if (refcount_.fetch_sub(1) == 1) {
      if (auto lp = left(); lp) {
        lp->put();
      }
      if (auto rp = right(); rp) {
        rp->put();
      }
      assert(entry);
      entry->put();
      delete this;
    }
  }

 public:
  static std::pair<const Node *, bool> insert(const OpContext& ctx,
      const NodePointer& root, const std::string& key,
      const std::string& value) {
    if (auto node = root(); node) {
      if (key < node->entry->key) {
        const auto [new_left, is_new_key] = insert(ctx, node->left, key, value);
        const auto new_node = node->__copyWithLeft(ctx, new_left); // steals new_left ref
        if (is_new_key) {
          const auto balanced = new_node->balance(ctx);
          new_node->put();
          return std::make_pair(balanced, is_new_key);
        } else {
          return std::make_pair(new_node, is_new_key);
        }

      } else if (key > node->entry->key) {
        const auto [new_right, is_new_key] = insert(ctx, node->right, key, value);
        const auto new_node = node->__copyWithRight(ctx, new_right); // steals new_right ref
        if (is_new_key) {
          const auto balanced = new_node->balance(ctx);
          new_node->put();
          return std::make_pair(balanced, is_new_key);
        } else {
          return std::make_pair(new_node, is_new_key);
        }

      } else {
        const auto new_node = node->copyWithEntry(ctx, key, value);
        return std::make_pair(new_node, false);
      }
    } else {
      const auto new_node = makeNodeWithEntry(ctx, true, key, value, nullptr, nullptr);
      return std::make_pair(new_node, true);
    }
  }

  static std::pair<NodePointer, bool> remove(const OpContext& ctx,
      const NodePointer& root, const std::string& key) {
    if (auto node = root(); node) {
      if (key < node->entry->key) {
        return remove_left(ctx, node, key);
      } else if (key > node->entry->key) {
        return remove_right(ctx, node, key);
      } else {
        const auto new_node = fuse(ctx, node->left, node->right);
        return std::make_pair(new_node, true);
      }
    } else {
      return std::make_pair(nullptr, false);
    }
  }

  // check that all nodes in the tree with the given rid are accessible via a
  // path from the root that is composed only of nodes with the same rid.
  static bool checkDenseDelta(const NodePointer& root, const uint64_t rid,
      const bool other_rid_above) {

    auto node = root();

    if (!node) {
      return true;
    }

    const auto same_rid = node->rid == rid;

    if (same_rid && other_rid_above) {
      return false;
    }

    const auto left_ok = checkDenseDelta(node->left, rid, !same_rid);
    const auto right_ok = checkDenseDelta(node->right, rid, !same_rid);

    return left_ok && right_ok;
  }

  // http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx
  static std::size_t checkConsistency(const NodePointer& root) {
    auto node = root();

    if (!node) {
      return 1;
    }

    const auto left = node->left();
    const auto right = node->right();

    if (node->red && ((left && left->red) || (right && right->red))) {
      return 0; // LCOV_EXCL_LINE
    }

    if ((left && left->entry->key >= node->entry->key) ||
        (right && right->entry->key <= node->entry->key)) {
      return 0; // LCOV_EXCL_LINE
    }

    const auto lh = checkConsistency(left);
    const auto rh = checkConsistency(right);

    if (lh != 0 && rh != 0 && lh != rh) {
      return 0; // LCOV_EXCL_LINE
    }

    if (lh != 0 && rh != 0) {
      return node->red ? lh : lh + 1;
    }

    return 0; // LCOV_EXCL_LINE
  }

 private:
  const Node * balance(const OpContext& ctx) const {
    if (!red) {
      // match: (color_l, color_l_l, color_l_r, color_r, color_r_l, color_r_r)
      if (auto l_node = left(); l_node && l_node->red) {
        // case: (Some(R), Some(R), ..)
        if (auto ll_node = l_node->left(); ll_node && ll_node->red) {
          const auto new_left = makeNode(ctx,
              false,
              ll_node->entry,
              ll_node->left,
              ll_node->right);

          const auto new_right = makeNode(ctx,
              false,
              entry,
              l_node->right,
              right);

          return _makeNode(ctx,
              true,
              get(l_node->entry),
              new_left,
              new_right);

          // case: (Some(R), _, Some(R), ..)
        } else if (auto lr_node = l_node->right(); lr_node && lr_node->red) {
          const auto new_left = makeNode(ctx,
              false,
              l_node->entry,
              l_node->left,
              lr_node->left);

          const auto new_right = makeNode(ctx,
              false,
              entry,
              lr_node->right,
              right);

          return _makeNode(ctx,
              true,
              get(lr_node->entry),
              new_left,
              new_right);
        }
      }

      // case: (.., Some(R), Some(R), _)
      if (auto r_node = right(); r_node && r_node->red) {
        if (auto rl_node = r_node->left(); rl_node && rl_node->red) {
          const auto new_left = makeNode(ctx,
              false,
              entry,
              left,
              rl_node->left);

          const auto new_right = makeNode(ctx,
              false,
              r_node->entry,
              rl_node->right,
              r_node->right);

          return _makeNode(ctx,
              true,
              get(rl_node->entry),
              new_left,
              new_right);

          // case: (.., Some(R), _, Some(R))
        } else if (auto rr_node = r_node->right(); rr_node && rr_node->red) {
          const auto new_left = makeNode(ctx,
              false,
              entry,
              left,
              r_node->left);

          const auto new_right = makeNode(ctx,
              false,
              rr_node->entry,
              rr_node->left,
              rr_node->right);

          return _makeNode(ctx,
              true,
              get(r_node->entry),
              new_left,
              new_right);
        }
      }
    }

    // red, or no matching case above
    get();
    return this;
  }

  // remove
 private:
  static const NodePointer fuse(const OpContext& ctx,
      const NodePointer& left,
      const NodePointer& right) {
    // match: (left, right)
    // case: (None, r)
    if (!left()) { // TODO: don't need to fully resolve to test
      return right.copy();
      // case: (l, None)
    } else if (!right()) {
      return left.copy(); // TODO: don't need to fully resolve to test
    }
    // case: (Some(l), Some(r))
    // fall through

    auto l_node = left();
    auto r_node = right();
    assert(l_node && r_node);

    // match: (left.color, right.color)
    // case: (B, R)
    if (!l_node->red && r_node->red) {
      return _makeNode(ctx,
          true,
          get(r_node->entry),
          fuse(ctx, left, r_node->left),
          r_node->right.copy());

      // case: (R, B)
    } else if (l_node->red && !r_node->red) {
      return _makeNode(ctx,
          true,
          get(l_node->entry),
          l_node->left.copy(),
          fuse(ctx, l_node->right, right));

      // case: (R, R)
    } else if (l_node->red && r_node->red) {
      const auto fused = fuse(ctx, l_node->right, r_node->left);
      auto f_node = fused();
      if (f_node && f_node->red) {
        const auto new_left = makeNode(ctx,
            true,
            l_node->entry,
            l_node->left,
            f_node->left);

        const auto new_right = makeNode(ctx,
            true,
            r_node->entry,
            f_node->right,
            r_node->right);

        const auto new_node = _makeNode(ctx,
            true,
            get(f_node->entry),
            new_left,
            new_right);

        //fused->put(); TODO: fused is owning. so destructor relase?
        return new_node;
      }

      const auto new_right = _makeNode(ctx,
          true,
          get(r_node->entry),
          fused, // TODO: i think is like stealing ownership
          r_node->right.copy());

      return _makeNode(ctx,
          true,
          get(l_node->entry),
          l_node->left.copy(),
          new_right);

      // case: (B, B)
    } else if (!l_node->red && !r_node->red) {
      const auto fused = fuse(ctx, l_node->right, r_node->left);
      auto f_node = fused();
      if (f_node && f_node->red) {
        const auto new_left = makeNode(ctx,
            false,
            l_node->entry,
            l_node->left,
            f_node->left);

        const auto new_right = makeNode(ctx,
            false,
            r_node->entry,
            f_node->right,
            r_node->right);

        const auto new_node = _makeNode(ctx,
            true,
            get(f_node->entry),
            new_left,
            new_right);

        //fused->put();
        return new_node;
      }

      const auto new_right = _makeNode(ctx,
          false,
          get(r_node->entry),
          fused,
          r_node->right.copy());

      const auto new_node = _makeNode(ctx,
          true,
          get(l_node->entry),
          l_node->left.copy(),
          new_right);

      const auto balanced = balance_left(ctx, new_node);

      new_node->put();
      return balanced;
    }

    assert(0); // LCOV_EXCL_LINE
  }

  static const Node * balance(const OpContext& ctx, const Node * const node) {
    if (auto left = node->left(); left && left->red) {
      if (auto right = node->right(); right && right->red) {

        const auto new_left = left->copyAsBlack(ctx);
        const auto new_right = right->copyAsBlack(ctx);

        return _makeNode(ctx,
            true,
            get(node->entry),
            new_left,
            new_right);
      }
    }

    assert(!node->red);
    return node->balance(ctx);
  }

  static const Node * balance_left(const OpContext& ctx, const Node * const node) {
    // match: (color_l, color_r, color_r_l)
    // case: (Some(R), ..)
    if (auto left = node->left(); left && left->red) {
      const auto new_left = makeNode(ctx,
          false,
          left->entry,
          left->left,
          left->right);

      return _makeNode(ctx,
          true,
          get(node->entry),
          new_left,
          node->right.copy());

      // case: (_, Some(B), _)
    } else if (auto right = node->right(); right && !right->red) {
      const auto new_right = makeNode(ctx,
          true,
          right->entry,
          right->left,
          right->right);

      const auto new_node = _makeNode(ctx,
          false,
          get(node->entry),
          node->left.copy(),
          new_right);

      const auto balanced = balance(ctx, new_node);
      new_node->put();

      return balanced;

      // case: (_, Some(R), Some(B))
    } else if (auto right = node->right(); right && right->red) { // TODO: resolved right, above
      if (auto rl_node = right->left(); rl_node && !rl_node->red) {

        auto rr_node = right->right(); // TODO: unchecked dereference

        const auto unbalanced_new_right = _makeNode(ctx,
            false,
            get(right->entry),
            rl_node->right.copy(),
            rr_node->copyAsRed(ctx));

        const auto new_right = balance(ctx, unbalanced_new_right);
        unbalanced_new_right->put();

        const auto new_left = makeNode(ctx,
            false,
            node->entry,
            node->left,
            rl_node->left.copy());

        return _makeNode(ctx,
            true,
            get(rl_node->entry),
            new_left,
            new_right);
      }
    }

    assert(0); // LCOV_EXCL_LINE
  }

  static const Node * balance_right(const OpContext& ctx,
      const Node * const node) {
    // match: (color_l, color_l_r, color_r)
    // case: (.., Some(R))
    if (auto right = node->right(); right && right->red) {
      const auto new_right = makeNode(ctx,
          false,
          right->entry,
          right->left,
          right->right);

      return _makeNode(ctx,
          true,
          get(node->entry),
          node->left.copy(),
          new_right);

      // case: (Some(B), ..)
    } else if (auto left = node->left(); left && !left->red) {
      const auto new_left = makeNode(ctx,
          true,
          left->entry,
          left->left,
          left->right);

      const auto new_node = _makeNode(ctx,
          false,
          get(node->entry),
          new_left,
          node->right.copy());

      const auto balanced = balance(ctx, new_node);
      new_node->put();

      return balanced;

      // case: (Some(R), Some(B), _)
    } else if (auto left = node->left(); left && left->red) {
      if (auto lr_node = left->right(); lr_node && !lr_node->red) {

        auto ll_node = left->left(); // TODO: unchecked

        const auto unbalanced_new_left = _makeNode(ctx,
            false,
            get(left->entry),
            ll_node->copyAsRed(ctx),
            lr_node->left.copy());

        const auto new_left = balance(ctx, unbalanced_new_left);
        unbalanced_new_left->put();

        const auto new_right = makeNode(ctx,
            false,
            node->entry,
            lr_node->right,
            node->right);

        return _makeNode(ctx,
            true,
            get(lr_node->entry),
            new_left,
            new_right);
      }
    }

    assert(0); // LCOV_EXCL_LINE
  }

  static std::pair<NodePointer, bool> remove_left(const OpContext& ctx,
      const NodePointer& root, const std::string& key) {

    const auto node = root();
    assert(node);

    const auto [new_left, removed] = remove(ctx, node->left, key);

    const auto new_node = _makeNode(ctx,
        true, // In case of rebalance the color does not matter
        get(node->entry),
        new_left,
        node->right.copy());

    if (auto left = node->left(); left && !left->red) {
      const auto balanced_new_node = balance_left(ctx, new_node);
      new_node->put();
      return std::make_pair(balanced_new_node, removed);
    } else {
      return std::make_pair(new_node, removed);
    }
  }

  static std::pair<NodePointer, bool> remove_right(const OpContext& ctx,
      const NodePointer& root, const std::string& key) {

    const auto node = root();
    assert(node);

    const auto [new_right, removed] = remove(ctx, node->right, key);

    const auto new_node = _makeNode(ctx,
        true, // In case of rebalance the color does not matter
        get(node->entry),
        node->left.copy(),
        new_right);

    if (auto right = node->right(); right && !right->red) {
      const auto balanced_new_node = balance_right(ctx, new_node);
      new_node->put();
      return std::make_pair(balanced_new_node, removed);
    } else {
      return std::make_pair(new_node, removed);
    }
  }

 public:
  const bool red;
  const Entry * const entry;
  NodePointer left;
  NodePointer right;
  const uint64_t rid;

 private:
  mutable std::atomic<uint64_t> refcount_;
};

class Tree {
 public:
  Tree() :
    root_(nullptr),
    size_(0)
  {}

  Tree(const Tree& other) :
    root_(other.root_),
    size_(other.size_)
  {
    if (root_)
      root_->get();
  }

  Tree(Tree&& other) :
    root_(other.root_),
    size_(other.size_)
  {
    other.root_ = nullptr;
    other.size_ = 0;
  }

  ~Tree() {
    if (root_)
      root_->put();
  }

  Tree& operator=(const Tree& other) {
    if (root_)
      root_->put();
    root_ = other.root_;
    size_ = other.size_;
    if (root_)
      root_->get();
    return *this;
  }

  Tree& operator=(Tree&& other) {
    if (root_)
      root_->put();
    root_ = other.root_;
    size_ = other.size_;
    other.root_ = nullptr;
    other.size_ = 0;
    return *this;
  }

 private:
  Tree(const Node *root, std::size_t size) :
    root_(root), size_(size)
  {}

 public:
  Tree insert(const OpContext& ctx, const std::string& key,
      const std::string& value) const {
    const auto [mb_new_root, is_new_key] = Node::insert(ctx, root_, key, value);
    const auto new_root = mb_new_root->copyAsBlack(ctx); // mb = maybe black
    mb_new_root->put();
    const auto new_size = size_ + (is_new_key ? 1 : 0);
    return Tree(new_root, new_size);
  }

  Tree remove(const OpContext& ctx, const std::string& key) const {
    const auto [mb_new_root, removed] = Node::remove(ctx, root_, key);
    if (removed) {
      auto mb_new_root_node = mb_new_root();
      if (mb_new_root_node) {
        // TODO: a simplification might be to proxy copyAsBlack through the node
        // pointer to catch the different cases.
        const auto new_root = mb_new_root_node->copyAsBlack(ctx);
        //mb_new_root->put(); // TODO: release in destructor?
        return Tree(new_root, size_ - 1);
      } else {
        return Tree(nullptr, size_ - 1);
      }
    } else {
      //if (mb_new_root) // TODO: relaese in destructor?
      //  mb_new_root->put();
      return *this; // copy constructor takes reference
    }
  }

  boost::optional<std::pair<std::string, std::string>> get(const std::string& key) const {
    auto cur = root_;
    while (cur) {
      if (key < cur->entry->key) {
        cur = cur->left();
      } else if (key > cur->entry->key) {
        cur = cur->right();
      } else {
        return std::make_pair(cur->entry->key, cur->entry->value);
      }
    }
    return boost::none;
  }

  std::map<std::string, std::string> items() const {
    std::map<std::string, std::string> out;
    auto node = root_;
    auto s = std::stack<const Node *>();
    while (!s.empty() || node) {
      if (node) {
        s.push(node);
        node = node->left();
      } else {
        node = s.top();
        s.pop();
        out.emplace(node->entry->key, node->entry->value);
        node = node->right();
      }
    }
    return out;
  }

  auto size() const {
    return size_;
  }

  void clear() {
    if (root_)
      root_->put();
    root_ = nullptr;
    size_ = 0;
  }

  bool consistent() const {
    if (root_) {
      const auto denseDeltaOk = Node::checkDenseDelta(
          root_, root_->rid, false);
      return denseDeltaOk && Node::checkConsistency(root_) != 0;
    } else {
      return true;
    }
  }

 private:
  const Node *root_;
  std::size_t size_;
};

NodePointer NodePointer::copy() const
{
  if (ptr_)
    ptr_->get();
  return NodePointer(ptr_);
}
