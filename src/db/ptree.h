/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#pragma once
#include <map>
#include <cassert>
#include <memory>
#include <stack>
#include <string>
#include <utility>
#include <cstddef>

struct OpContext {
  const uint64_t rid;
};

template<
  typename Key,
  typename T>
class Tree {
 private:
  struct Node;
  struct Entry;

  typedef std::shared_ptr<const Node> node_ptr_type;
  typedef std::shared_ptr<const Entry> entry_ptr_type;

  typedef Key key_type;
  typedef T   mapped_type;

  struct Entry {
    Entry(const key_type& key, const mapped_type& value) :
      key(key),
      value(value)
    {}

    const key_type key;
    const mapped_type value;
  };

  struct Node : std::enable_shared_from_this<const Node> {
   public:
    Node(const bool red,
        const entry_ptr_type& entry,
        const node_ptr_type& left,
        const node_ptr_type& right,
        const uint64_t rid) :
      red(red),
      entry(entry),
      left(left),
      right(right),
      rid(rid)
    {}

    static inline auto makeNode(const OpContext& ctx, const bool red,
        const entry_ptr_type& entry, const node_ptr_type& left,
        const node_ptr_type& right) {
      return std::make_shared<const Node>(red, entry, left, right, ctx.rid);
    }

    static inline auto makeNode(const OpContext& ctx, const bool red,
        const key_type& key, const mapped_type& value,
        const node_ptr_type& left, const node_ptr_type& right) {
      const auto entry = std::make_shared<const Entry>(key, value);
      return makeNode(ctx, red, entry, left, right);
    }

   public:
    inline auto copyWithEntry(const OpContext& ctx, const key_type& key,
        const mapped_type& value) const {
      return makeNode(ctx, red, key, value, left, right);
    }

    inline auto copyWithLeft(const OpContext& ctx,
        const node_ptr_type& new_left) const {
      return makeNode(ctx, red, entry, new_left, right);
    }

    inline auto copyWithRight(const OpContext& ctx,
        const node_ptr_type& new_right) const {
      return makeNode(ctx, red, entry, left, new_right);
    }

    inline auto copyAsBlack(const OpContext& ctx) const {
      return makeNode(ctx, false, entry, left, right);
    }

    inline auto copyAsRed(const OpContext& ctx) const {
      return makeNode(ctx, true, entry, left, right);
    }

   public:
    static std::pair<node_ptr_type, bool> insert(const OpContext& ctx,
        const node_ptr_type& node, const key_type& key,
        const mapped_type& value) {
      if (node) {
        if (key < node->entry->key) {
          const auto [new_left, is_new_key] = insert(ctx, node->left, key, value);
          const auto new_node = node->copyWithLeft(ctx, new_left);
          if (is_new_key) {
            return std::pair(new_node->balance(ctx), is_new_key);
          } else {
            return std::pair(new_node, is_new_key);
          }

        } else if (key > node->entry->key) {
          const auto [new_right, is_new_key] = insert(ctx, node->right, key, value);
          const auto new_node = node->copyWithRight(ctx, new_right);
          if (is_new_key) {
            return std::pair(new_node->balance(ctx), is_new_key);
          } else {
            return std::pair(new_node, is_new_key);
          }

        } else {
          const auto new_node = node->copyWithEntry(ctx, key, value);
          return std::pair(new_node, false);
        }
      } else {
        const auto new_node = makeNode(ctx, true, key, value, nullptr, nullptr);
        return std::pair(new_node, true);
      }
    }

    node_ptr_type balance(const OpContext& ctx) const {
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

            return makeNode(ctx,
                true,
                left->entry,
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

            return makeNode(ctx,
                true,
                left->right->entry,
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

            return makeNode(ctx,
                true,
                right->left->entry,
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

            return makeNode(ctx,
                true,
                right->entry,
                new_left,
                new_right);
          }
        }
      }

      // red, or no matching case above
      return this->shared_from_this();
    }

    // check that all nodes in the tree with the given rid are accessible via a
    // path from the root that is composed only of nodes with the same rid.
    static bool checkDenseDelta(const node_ptr_type& node, const uint64_t rid,
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
    static std::size_t checkConsistency(const node_ptr_type& node) {
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

    // remove
   public:
    static node_ptr_type fuse(const OpContext& ctx, const node_ptr_type& left,
        const node_ptr_type& right) {
      // match: (left, right)
      // case: (None, r)
      if (!left) {
        return right;
        // case: (l, None)
      } else if (!right) {
        return left;
      }
      // case: (Some(l), Some(r))
      // fall through
      assert(left && right);

      // match: (left.color, right.color)
      // case: (B, R)
      if (!left->red && right->red) {
        return makeNode(ctx,
            true,
            right->entry,
            fuse(ctx, left, right->left),
            right->right);

        // case: (R, B)
      } else if (left->red && !right->red) {
        return makeNode(ctx,
            true,
            left->entry,
            left->left,
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

          return makeNode(ctx,
              true,
              fused->entry,
              new_left,
              new_right);
        }

        const auto new_right = makeNode(ctx,
            true,
            right->entry,
            fused,
            right->right);

        return makeNode(ctx,
            true,
            left->entry,
            left->left,
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

          return makeNode(ctx,
              true,
              fused->entry,
              new_left,
              new_right);
        }

        const auto new_right = makeNode(ctx,
            false,
            right->entry,
            fused,
            right->right);

        const auto new_node = makeNode(ctx,
            true,
            left->entry,
            left->left,
            new_right);

        return balance_left(ctx, new_node);
      }

      assert(0); // LCOV_EXCL_LINE
    }

    static node_ptr_type balance(const OpContext& ctx,
        const node_ptr_type& node) {
      if (node->left && node->left->red &&
          node->right && node->right->red) {

        const auto new_left = node->left ?
          node->left->copyAsBlack(ctx) : node->left;

        const auto new_right = node->right ?
          node->right->copyAsBlack(ctx) : node->right;

        return makeNode(ctx,
            true,
            node->entry,
            new_left,
            new_right);
      }

      assert(!node->red);
      return node->balance(ctx);
    }

    static node_ptr_type balance_left(const OpContext& ctx,
        const node_ptr_type& node) {
      // match: (color_l, color_r, color_r_l)
      // case: (Some(R), ..)
      if (node->left && node->left->red) {
        const auto new_left = makeNode(ctx,
            false,
            node->left->entry,
            node->left->left,
            node->left->right);

        return makeNode(ctx,
            true,
            node->entry,
            new_left,
            node->right);

        // case: (_, Some(B), _)
      } else if (node->right && !node->right->red) {
        const auto new_right = makeNode(ctx,
            true,
            node->right->entry,
            node->right->left,
            node->right->right);

        const auto new_node = makeNode(ctx,
            false,
            node->entry,
            node->left,
            new_right);

        return balance(ctx, new_node);

        // case: (_, Some(R), Some(B))
      } else if (node->right && node->right->red &&
          node->right->left && !node->right->left->red) {

        const auto unbalanced_new_right = makeNode(ctx,
            false,
            node->right->entry,
            node->right->left->right,
            node->right->right->copyAsRed(ctx));

        const auto new_right = balance(ctx, unbalanced_new_right);

        const auto new_left = makeNode(ctx,
            false,
            node->entry,
            node->left,
            node->right->left->left);

        return makeNode(ctx,
            true,
            node->right->left->entry,
            new_left,
            new_right);
      }

      assert(0); // LCOV_EXCL_LINE
    }

    static node_ptr_type balance_right(const OpContext& ctx,
        const node_ptr_type& node) {
      // match: (color_l, color_l_r, color_r)
      // case: (.., Some(R))
      if (node->right && node->right->red) {
        const auto new_right = makeNode(ctx,
            false,
            node->right->entry,
            node->right->left,
            node->right->right);

        return makeNode(ctx,
            true,
            node->entry,
            node->left,
            new_right);

        // case: (Some(B), ..)
      } else if (node->left && !node->left->red) {
        const auto new_left = makeNode(ctx,
            true,
            node->left->entry,
            node->left->left,
            node->left->right);

        const auto new_node = makeNode(ctx,
            false,
            node->entry,
            new_left,
            node->right);

        return balance(ctx, new_node);

        // case: (Some(R), Some(B), _)
      } else if (node->left && node->left->red &&
          node->left->right && !node->left->right->red) {

        const auto unbalanced_new_left = makeNode(ctx,
            false,
            node->left->entry,
            node->left->left->copyAsRed(ctx),
            node->left->right->left);

        const auto new_left = balance(ctx, unbalanced_new_left);

        const auto new_right = makeNode(ctx,
            false,
            node->entry,
            node->left->right->right,
            node->right);

        return makeNode(ctx,
            true,
            node->left->right->entry,
            new_left,
            new_right);
      }

      assert(0); // LCOV_EXCL_LINE
    }

    static std::pair<node_ptr_type, bool> remove_left(const OpContext& ctx,
        const node_ptr_type& node, const key_type& key) {
      const auto [new_left, removed] = remove(ctx, node->left, key);

      const auto new_node = makeNode(ctx,
          true, // In case of rebalance the color does not matter
          node->entry,
          new_left,
          node->right);

      const bool left_black = node->left && !node->left->red;
      const auto balanced_new_node = left_black ?
        balance_left(ctx, new_node) : new_node;

      return std::pair(balanced_new_node, removed);
    }

    static std::pair<node_ptr_type, bool> remove_right(const OpContext& ctx,
        const node_ptr_type& node, const key_type& key) {
      const auto [new_right, removed] = remove(ctx, node->right, key);

      const auto new_node = makeNode(ctx,
          true, // In case of rebalance the color does not matter
          node->entry,
          node->left,
          new_right);

      const bool right_black = node->right && !node->right->red;
      const auto bal_new_node = right_black ?
        balance_right(ctx, new_node) : new_node;

      return std::pair(bal_new_node, removed);
    }

    static std::pair<node_ptr_type, bool> remove(const OpContext& ctx,
        const node_ptr_type& node, const key_type& key) {
      if (node) {
        if (key < node->entry->key) {
          return remove_left(ctx, node, key);
        } else if (key > node->entry->key) {
          return remove_right(ctx, node, key);
        } else {
          const auto new_node = fuse(ctx, node->left, node->right);
          return std::pair(new_node, true);
        }
      } else {
        return std::pair(nullptr, false);
      }
    }

   public:
    const bool red;
    const entry_ptr_type entry;
    const node_ptr_type left;
    const node_ptr_type right;
    const uint64_t rid;
  };

 public:
  Tree() :
    root_(nullptr),
    size_(0)
  {}

 private:
  Tree(node_ptr_type root, std::size_t size) :
    root_(root), size_(size)
  {}

 public:
  Tree insert(const OpContext& ctx, const key_type& key,
      const mapped_type& value) const {
    const auto [mb_new_root, is_new_key] = Node::insert(ctx, root_, key, value);
    const auto new_root = mb_new_root->copyAsBlack(ctx); // mb = maybe black
    const auto new_size = size_ + (is_new_key ? 1 : 0);
    return Tree(new_root, new_size);
  }

  Tree remove(const OpContext& ctx, const key_type& key) const {
    const auto [mb_new_root, removed] = Node::remove(ctx, root_, key);
    if (removed) {
      const auto new_root = mb_new_root ?
        mb_new_root->copyAsBlack(ctx) : mb_new_root;
      return Tree(new_root, size_ - 1);
    } else {
      return *this;
    }
  }

  std::map<key_type, mapped_type> items() const {
    std::map<key_type, mapped_type> out;
    auto node = root_;
    auto s = std::stack<node_ptr_type>();
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
  node_ptr_type root_;
  std::size_t size_;
};
