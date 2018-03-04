#pragma once
#include <memory>

namespace cruzdb {

class Statistics;

struct Options {
  std::shared_ptr<Statistics> statistics = nullptr;
  size_t node_cache_size = 512*1024*1024;
  size_t imap_cache_size = 100000;
  size_t entry_cache_size = 1000;
};

}
