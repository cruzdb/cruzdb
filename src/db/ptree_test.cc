#include <gtest/gtest.h>
#include <map>
#include <cassert>
#include <sstream>
#include <list>
#include <iomanip>
#include <random>
#include "ptree.h"

struct tree_pair {
  Tree<std::string, std::string> tree;
  std::map<std::string, std::string> truth;
};

static inline std::string tostr(uint32_t value)
{
  std::stringstream ss;
  ss << std::setw(10) << std::setfill('0') << value;
  return ss.str();
}

TEST(Foo, Bar) {
  const uint32_t coin_toss = 50;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, 50000);
  std::uniform_int_distribution<uint32_t> coin(0, 100);
  std::uniform_int_distribution<uint32_t> ops(1, 5);

  // snapshot history
  std::list<tree_pair> trees;

  // on-going state
  Tree<std::string, std::string> tree;
  std::map<std::string, std::string> truth;

  uint64_t rid = 0;

  // build a bunch of snapshots
  for (int i = 0; i < 500; i++) {
    for (int j = 0; j < 50; j++) {
      OpContext ctx = { rid++ };
      auto op_count = ops(gen);
      // FIXME: do until op_count SUCCESSFUL ops
      while (op_count) {
        const std::string key = tostr(dis(gen));
        if (coin(gen) < coin_toss) {
          tree = tree.insert(ctx, key, key);
          truth.emplace(key, key);
        } else {
          tree = tree.remove(ctx, key);
          truth.erase(key);
        }
        op_count--;
      }
    }
    trees.emplace_back();
    trees.back().tree = tree;
    trees.back().truth = truth;
  }

  // verify all snapshots
  for (const auto& tree : trees) {
    ASSERT_EQ(tree.tree.items(), tree.truth);
    ASSERT_TRUE(tree.tree.consistent());
  }
}
