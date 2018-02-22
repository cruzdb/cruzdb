#include <iostream>
#include <algorithm>
#include <cassert>
#include <random>
#include <set>
#include <chrono>

static auto bench_vec(int num_ops, int zone_len, int repeat)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  // build zone
  std::vector<std::vector<uint64_t>> zone;
  for (int i = 0; i < zone_len; i++) {
    std::vector<uint64_t> tmp;
    for (int j = 0; j < num_ops; j++) {
      tmp.emplace_back(dis(gen));
    }
    std::sort(tmp.begin(), tmp.end());
    zone.emplace_back(std::move(tmp));
  }

  // build intention
  std::vector<uint64_t> intention;
  for (int j = 0; j < num_ops; j++) {
    intention.emplace_back(dis(gen));
  }
  std::sort(intention.begin(), intention.end());

  int count = 0;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < repeat; i++) {
    for (const auto& zset : zone) {
      for (const auto& key : intention) {
        if (std::binary_search(zset.begin(), zset.end(), key)) {
          count++;
        }
      }
    }
  }
  auto end = std::chrono::steady_clock::now();
  assert(count == 0);

  auto dur_us = std::chrono::duration_cast<
    std::chrono::nanoseconds>(end - start);

  return dur_us.count();

}

static auto bench_set(int num_ops, int zone_len, int repeat)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  // build zone
  std::vector<std::set<uint64_t>> zone;
  for (int i = 0; i < zone_len; i++) {
    std::set<uint64_t> tmp;
    for (int j = 0; j < num_ops; j++) {
      tmp.insert(dis(gen));
    }
    zone.emplace_back(std::move(tmp));
  }

  // build intention
  std::set<uint64_t> intention;
  for (int j = 0; j < num_ops; j++) {
    intention.insert(dis(gen));
  }

  int count = 0;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < repeat; i++) {
    for (const auto& zset : zone) {
      for (const auto& key : intention) {
        if (zset.find(key) != zset.end()) {
          count++;
        }
      }
    }
  }
  auto end = std::chrono::steady_clock::now();
  assert(count == 0);

  auto dur_us = std::chrono::duration_cast<
    std::chrono::nanoseconds>(end - start);

  return dur_us.count();
}

int main(int argc, char **argv)
{
  const int repeat = 10000;
  std::cout << "expr,nops,zonelen,ns" << std::endl;
  for (int i = 2; i <= 32; i *= 2) {
    for (int j = 1; j <= 256; j*=4) {
      auto ns = bench_set(i, j, repeat);
      std::cout << "set" << "," << i << "," << j << "," << (ns/repeat) << std::endl;

      ns = bench_vec(i, j, repeat);
      std::cout << "vec" << "," << i << "," << j << "," << (ns/repeat) << std::endl;
    }
  }

  return 0;
}
