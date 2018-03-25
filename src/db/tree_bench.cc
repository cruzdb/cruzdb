#include <condition_variable>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>
#include <benchmark/benchmark.h>
#include "ptree.h"

struct rng {
  rng() :
    gen(rd()),
    dis(std::numeric_limits<uint64_t>::min(),
        std::numeric_limits<uint64_t>::max())
  {}

  inline auto next() {
    std::lock_guard<std::mutex> lk(lock);
    return dis(gen);
  }

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<uint64_t> dis;
  std::mutex lock;
};

static auto buildTree(rng& r, std::size_t size)
{
  Tree<uint64_t, uint64_t> tree;
  while (tree.size() < size) {
    const uint64_t key = r.next();
    tree = tree.insert(key, key);
  }
  return tree;
}

static Tree<uint64_t, uint64_t> tree;

static std::condition_variable cond;
static bool init_complete = false;
static std::mutex lock;

static void BM_Insert(benchmark::State& state)
{
  const int tree_size = state.range(0);
  const int num_inserts = state.range(1);
  rng r;

  if (state.thread_index == 0) {
    std::lock_guard<std::mutex> lk(lock);

    // build the shared tree
    if (tree.size() != tree_size) {
      tree.clear();
      tree = buildTree(r, tree_size);
    }

    // notify build is complete
    init_complete = true;
    cond.notify_all();
  }

  // all threads wait until the tree is built
  std::unique_lock<std::mutex> lk(lock);
  cond.wait(lk, [&] { return init_complete; });
  lk.unlock();

  assert(tree_size > 0);
  assert(tree.size() == tree_size);

  // generate set of keys to insert
  std::vector<uint64_t> keys;
  keys.reserve(num_inserts);
  while (keys.size() < num_inserts) {
    const auto key = r.next();
    if (!tree.get(key)) {
      keys.emplace_back(key);
    }
  }

  for (auto _ : state) {
    for (const auto& key : keys) {
      benchmark::DoNotOptimize(tree.insert(key, key));
    }
  }

  state.SetItemsProcessed(state.iterations() * keys.size());

  if (state.thread_index == 0) {
    // tree is cleared when the size changes in the initialization phase. Note
    // that if there are more benchmarks in this executable, we might want to
    // figure out a way to clear the tree after the very last run.
  }
}

BENCHMARK(BM_Insert)
  ->RangeMultiplier(10)
  ->Ranges({{100000, 100000}, {100000, 100000}})
  ->ThreadRange(1, 1);

BENCHMARK_MAIN();
