#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace cruzdb {

enum Tickers : uint32_t {
  LOG_APPENDS,
  LOG_READS,
  LOG_READS_FILLED,
  LOG_READS_UNWRITTEN,
  LOG_READ_CACHE_HIT,
  NODE_CACHE_HIT,
  NODE_CACHE_NODES_READ,
  NODE_CACHE_FETCHES,
  NODE_CACHE_FREE,
  BYTES_WRITTEN,
  BYTES_READ,
  TICKER_ENUM_MAX
};

const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
  {LOG_APPENDS, "cruzdb.log.appends"},
  {LOG_READS, "cruzdb.log.reads"},
  {LOG_READS_FILLED, "cruzdb.log.reads_filled"},
  {LOG_READS_UNWRITTEN, "cruzdb.log.reads_unwritten"},
  {LOG_READ_CACHE_HIT, "cruzdb.log.read.cache.hit"},
  {NODE_CACHE_HIT, "cruzdb.node_cache.hit"},
  {NODE_CACHE_NODES_READ, "cruzdb.node_cache.nodes.read"},
  {NODE_CACHE_FETCHES, "cruzdb.node_cache.fetches"},
  {NODE_CACHE_FREE, "cruzdb.node_cache.free"},
  {BYTES_WRITTEN, "cruzdb.bytes.written"},
  {BYTES_READ, "cruzdb.bytes.read"},
};

enum Histograms : uint32_t {
  HISTOGRAM_ENUM_MAX,  // TODO(ldemailly): enforce HistogramsNameMap match
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
};

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
  // zero-initialize new members since old Statistics::histogramData()
  // implementations won't write them.
  double max = 0.0;
};

enum StatsLevel {
  // Collect all stats except time inside mutex lock AND time spent on
  // compression.
  kExceptDetailedTimers,
  // Collect all stats except the counters requiring to get time inside the
  // mutex lock.
  kExceptTimeForMutex,
  // Collect all stats, including measuring duration of mutex operations.
  // If getting time is expensive on the platform to run, it can
  // reduce scalability to more threads, especially for writes.
  kAll,
};

class Statistics {
 public:
  virtual ~Statistics() {}

  virtual uint64_t getTickerCount(uint32_t tickerType) const = 0;
  virtual void histogramData(uint32_t type, HistogramData* const data) const = 0;
  virtual std::string getHistogramString(uint32_t type) const { return ""; }
  virtual void recordTick(uint32_t tickerType, uint64_t count = 0) = 0;
  virtual void setTickerCount(uint32_t tickerType, uint64_t count) = 0;
  virtual uint64_t getAndResetTickerCount(uint32_t tickerType) = 0;
  virtual void measureTime(uint32_t histogramType, uint64_t time) = 0;

  //virtual Status Reset() = 0;
  virtual bool Reset() = 0;

  // String representation of the statistic object.
  virtual std::string ToString() const {
    // Do nothing by default
    return std::string("ToString(): not implemented");
  }

  // Override this function to disable particular histogram collection
  virtual bool HistEnabledForType(uint32_t type) const {
    return type < HISTOGRAM_ENUM_MAX;
  }

  StatsLevel stats_level_ = kExceptDetailedTimers;
};

std::shared_ptr<Statistics> CreateDBStatistics();

}
