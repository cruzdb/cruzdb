#include <map>
#include <cassert>
#include <sstream>
#include <list>
#include <iomanip>
#include <random>
#include "ptree.h"

struct tree_pair {
  Tree tree;
  std::map<std::string, std::string> truth;
};

static inline std::string tostr(uint32_t value)
{
  std::stringstream ss;
  ss << std::setw(10) << std::setfill('0') << value;
  return ss.str();
}

int main() {
  // snapshot history
  std::list<tree_pair> trees;

  // on-going state
  Tree tree;
  std::map<std::string, std::string> truth;

  uint64_t rid = 0;

  // build a bunch of snapshots
  for (int i = 0; i < 100; i++) {
    for (int j = 0; j < 20; j++) {
      OpContext ctx = { rid++ };
      auto op_count = rand() % 6;
      // FIXME: do until op_count SUCCESSFUL ops
      while (op_count) {
        const std::string key = tostr(rand() % 50000);
        if ((double)rand() < (RAND_MAX * 0.75)) {
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
    assert(tree.tree.items() == tree.truth);
    assert(tree.tree.consistent());
  }
}
