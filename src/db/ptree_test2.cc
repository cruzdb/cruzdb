#include <map>
#include <cassert>
#include <sstream>
#include <list>
#include <iomanip>
#include <random>
#include <iostream>
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

int main()
{
  std::list<tree_pair> trees;
  Tree<std::string, std::string> tree;
  std::map<std::string, std::string> truth;

  Tree<std::string, std::string>::OpContext ctx;

  uint64_t rid = 0;
  for (int i = 0; i < 200; i++) {
    for (int j = 0; j < 20; j++) {
      ctx.rid = rid++;
      auto op_count = rand() % 6;
      while (op_count) {
        const std::string key = tostr((rand() % 50000)+ 1);
        if (rand() < (RAND_MAX / 2)) {
          auto t = tree.insert(ctx, key, key);
          tree.assign(ctx, std::move(t));
          truth.emplace(key, key);
        } else {
          auto t = tree.remove(ctx, key);
          tree.assign(ctx, std::move(t));
          truth.erase(key);
        }
        op_count--;
      }
    }
    trees.emplace_back();
    trees.back().tree.assign(ctx, tree);
    trees.back().truth = truth;
  }

  for (const auto& tree : trees) {
    assert(tree.tree.items() == tree.truth);
    assert(tree.tree.consistent());
  }
}
