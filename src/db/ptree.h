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
      const Entry * const entry, const Node * const left,
      const Node * const right) {
    return new Node(red, entry, left, right, ctx.rid);
  }

  // bumps all reference counts
  static inline const Node * makeNode(const OpContext& ctx, const bool red,
      const Entry * const entry,
      const Node * const left,
      const Node * const right) {
    if (left)
      left->get();

    if (right)
      right->get();

    assert(entry);
    entry->get();

    return _makeNode(ctx, red, entry, left, right);
  }

  // caller ensure correct left/right reference counts
  static inline auto makeNodeWithEntry(const OpContext& ctx, const bool red,
      const std::string& key, const std::string& value,
      const Node * const left, const Node * const right) {
    const auto entry = new Entry(key, value);
    return _makeNode(ctx, red, entry, left, right);
  }

 private:
  inline auto copyWithEntry(const OpContext& ctx, const std::string& key,
      const std::string& value) const {
    return makeNodeWithEntry(ctx, red, key, value, get(left), get(right));
  }

  // steals new_left reference
  inline auto __copyWithLeft(const OpContext& ctx,
      const Node * const new_left) const {
    return _makeNode(ctx, red, get(entry), new_left, get(right));
  }

  // steals new_right reference
  inline auto __copyWithRight(const OpContext& ctx,
      const Node * const new_right) const {
    return _makeNode(ctx, red, get(entry), get(left), new_right);
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
      const Node * const left,
      const Node * const right,
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
      if (left)
        left->put();
      if (right)
        right->put();
      assert(entry);
      entry->put();
      delete this;
    }
  }

 public:
  static std::pair<const Node *, bool> insert(const OpContext& ctx,
      const Node * const node, const std::string& key,
      const std::string& value) {
    if (node) {
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

  static std::pair<const Node *, bool> remove(const OpContext& ctx,
      const Node * const node, const std::string& key) {
    if (node) {
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
  static bool checkDenseDelta(const Node * const node, const uint64_t rid,
      const bool other_rid_above) {
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
  static std::size_t checkConsistency(const Node * const node) {
    if (!node) {
      return 1;
    }

    const auto left = node->left;
    const auto right = node->right;

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
      if (left && left->red) {
        // case: (Some(R), Some(R), ..)
        if (left->left && left->left->red) {
          const auto new_left = makeNode(ctx,
              false,
              left->left->entry,
              left->left->left,
              left->left->right);

          const auto new_right = makeNode(ctx,
              false,
              entry,
              left->right,
              right);

          return _makeNode(ctx,
              true,
              get(left->entry),
              new_left,
              new_right);

          // case: (Some(R), _, Some(R), ..)
        } else if (left->right && left->right->red) {
          const auto new_left = makeNode(ctx,
              false,
              left->entry,
              left->left,
              left->right->left);

          const auto new_right = makeNode(ctx,
              false,
              entry,
              left->right->right,
              right);

          return _makeNode(ctx,
              true,
              get(left->right->entry),
              new_left,
              new_right);
        }
      }

      // case: (.., Some(R), Some(R), _)
      if (right && right->red) {
        if (right->left && right->left->red) {
          const auto new_left = makeNode(ctx,
              false,
              entry,
              left,
              right->left->left);

          const auto new_right = makeNode(ctx,
              false,
              right->entry,
              right->left->right,
              right->right);

          return _makeNode(ctx,
              true,
              get(right->left->entry),
              new_left,
              new_right);

          // case: (.., Some(R), _, Some(R))
        } else if (right->right && right->right->red) {
          const auto new_left = makeNode(ctx,
              false,
              entry,
              left,
              right->left);

          const auto new_right = makeNode(ctx,
              false,
              right->right->entry,
              right->right->left,
              right->right->right);

          return _makeNode(ctx,
              true,
              get(right->entry),
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
  static const Node * fuse(const OpContext& ctx,
      const Node * const left,
      const Node * const right) {
    // match: (left, right)
    // case: (None, r)
    if (!left) {
      return get(right);
      // case: (l, None)
    } else if (!right) {
      return get(left);
    }
    // case: (Some(l), Some(r))
    // fall through
    assert(left && right);

    // match: (left.color, right.color)
    // case: (B, R)
    if (!left->red && right->red) {
      return _makeNode(ctx,
          true,
          get(right->entry),
          fuse(ctx, left, right->left),
          get(right->right));

      // case: (R, B)
    } else if (left->red && !right->red) {
      return _makeNode(ctx,
          true,
          get(left->entry),
          get(left->left),
          fuse(ctx, left->right, right));

      // case: (R, R)
    } else if (left->red && right->red) {
      const auto fused = fuse(ctx, left->right, right->left);
      if (fused && fused->red) {
        const auto new_left = makeNode(ctx,
            true,
            left->entry,
            left->left,
            fused->left);

        const auto new_right = makeNode(ctx,
            true,
            right->entry,
            fused->right,
            right->right);

        const auto new_node = _makeNode(ctx,
            true,
            get(fused->entry),
            new_left,
            new_right);

        fused->put();
        return new_node;
      }

      const auto new_right = _makeNode(ctx,
          true,
          get(right->entry),
          fused,
          get(right->right));

      return _makeNode(ctx,
          true,
          get(left->entry),
          get(left->left),
          new_right);

      // case: (B, B)
    } else if (!left->red && !right->red) {
      const auto fused = fuse(ctx, left->right, right->left);
      if (fused && fused->red) {
        const auto new_left = makeNode(ctx,
            false,
            left->entry,
            left->left,
            fused->left);

        const auto new_right = makeNode(ctx,
            false,
            right->entry,
            fused->right,
            right->right);

        const auto new_node = _makeNode(ctx,
            true,
            get(fused->entry),
            new_left,
            new_right);

        fused->put();
        return new_node;
      }

      const auto new_right = _makeNode(ctx,
          false,
          get(right->entry),
          fused,
          get(right->right));

      const auto new_node = _makeNode(ctx,
          true,
          get(left->entry),
          get(left->left),
          new_right);

      const auto balanced = balance_left(ctx, new_node);

      new_node->put();
      return balanced;
    }

    assert(0); // LCOV_EXCL_LINE
  }

  static const Node * balance(const OpContext& ctx, const Node * const node) {
    if (node->left && node->left->red &&
        node->right && node->right->red) {

      const auto new_left = node->left ?
        node->left->copyAsBlack(ctx) : nullptr;

      const auto new_right = node->right ?
        node->right->copyAsBlack(ctx) : nullptr;

      return _makeNode(ctx,
          true,
          get(node->entry),
          new_left,
          new_right);
    }

    assert(!node->red);
    return node->balance(ctx);
  }

  static const Node * balance_left(const OpContext& ctx, const Node * const node) {
    // match: (color_l, color_r, color_r_l)
    // case: (Some(R), ..)
    if (node->left && node->left->red) {
      const auto new_left = makeNode(ctx,
          false,
          node->left->entry,
          node->left->left,
          node->left->right);

      return _makeNode(ctx,
          true,
          get(node->entry),
          new_left,
          get(node->right));

      // case: (_, Some(B), _)
    } else if (node->right && !node->right->red) {
      const auto new_right = makeNode(ctx,
          true,
          node->right->entry,
          node->right->left,
          node->right->right);

      const auto new_node = _makeNode(ctx,
          false,
          get(node->entry),
          get(node->left),
          new_right);

      const auto balanced = balance(ctx, new_node);
      new_node->put();

      return balanced;

      // case: (_, Some(R), Some(B))
    } else if (node->right && node->right->red &&
        node->right->left && !node->right->left->red) {

      const auto unbalanced_new_right = _makeNode(ctx,
          false,
          get(node->right->entry),
          get(node->right->left->right),
          node->right->right->copyAsRed(ctx));

      const auto new_right = balance(ctx, unbalanced_new_right);
      unbalanced_new_right->put();

      const auto new_left = makeNode(ctx,
          false,
          node->entry,
          node->left,
          node->right->left->left);

      return _makeNode(ctx,
          true,
          get(node->right->left->entry),
          new_left,
          new_right);
    }

    assert(0); // LCOV_EXCL_LINE
  }

  static const Node * balance_right(const OpContext& ctx,
      const Node * const node) {
    // match: (color_l, color_l_r, color_r)
    // case: (.., Some(R))
    if (node->right && node->right->red) {
      const auto new_right = makeNode(ctx,
          false,
          node->right->entry,
          node->right->left,
          node->right->right);

      return _makeNode(ctx,
          true,
          get(node->entry),
          get(node->left),
          new_right);

      // case: (Some(B), ..)
    } else if (node->left && !node->left->red) {
      const auto new_left = makeNode(ctx,
          true,
          node->left->entry,
          node->left->left,
          node->left->right);

      const auto new_node = _makeNode(ctx,
          false,
          get(node->entry),
          new_left,
          get(node->right));

      const auto balanced = balance(ctx, new_node);
      new_node->put();

      return balanced;

      // case: (Some(R), Some(B), _)
    } else if (node->left && node->left->red &&
        node->left->right && !node->left->right->red) {

      const auto unbalanced_new_left = _makeNode(ctx,
          false,
          get(node->left->entry),
          node->left->left->copyAsRed(ctx),
          get(node->left->right->left));

      const auto new_left = balance(ctx, unbalanced_new_left);
      unbalanced_new_left->put();

      const auto new_right = makeNode(ctx,
          false,
          node->entry,
          node->left->right->right,
          node->right);

      return _makeNode(ctx,
          true,
          get(node->left->right->entry),
          new_left,
          new_right);
    }

    assert(0); // LCOV_EXCL_LINE
  }

  static std::pair<const Node *, bool> remove_left(const OpContext& ctx,
      const Node * const node, const std::string& key) {
    const auto [new_left, removed] = remove(ctx, node->left, key);

    const auto new_node = _makeNode(ctx,
        true, // In case of rebalance the color does not matter
        get(node->entry),
        new_left,
        get(node->right));

    const bool left_black = node->left && !node->left->red;
    if (left_black) {
      const auto balanced_new_node = balance_left(ctx, new_node);
      new_node->put();
      return std::make_pair(balanced_new_node, removed);
    } else {
      return std::make_pair(new_node, removed);
    }
  }

  static std::pair<const Node *, bool> remove_right(const OpContext& ctx,
      const Node * const node, const std::string& key) {
    const auto [new_right, removed] = remove(ctx, node->right, key);

    const auto new_node = _makeNode(ctx,
        true, // In case of rebalance the color does not matter
        get(node->entry),
        get(node->left),
        new_right);

    const bool right_black = node->right && !node->right->red;
    if (right_black) {
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
  const Node * const left;
  const Node * const right;
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
      if (mb_new_root) {
        const auto new_root = mb_new_root->copyAsBlack(ctx);
        mb_new_root->put();
        return Tree(new_root, size_ - 1);
      } else {
        return Tree(nullptr, size_ - 1);
      }
    } else {
      if (mb_new_root)
        mb_new_root->put();
      return *this; // copy constructor takes reference
    }
  }

  boost::optional<std::pair<std::string, std::string>> get(const std::string& key) const {
    auto cur = root_;
    while (cur) {
      if (key < cur->entry->key) {
        cur = cur->left;
      } else if (key > cur->entry->key) {
        cur = cur->right;
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
        node = node->left;
      } else {
        node = s.top();
        s.pop();
        out.emplace(node->entry->key, node->entry->value);
        node = node->right;
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
