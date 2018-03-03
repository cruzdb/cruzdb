#pragma once
#include <memory>

namespace cruzdb {

class Statistics;

struct Options {
  std::shared_ptr<Statistics> statistics = nullptr;
};

}
