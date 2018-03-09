#include <chrono>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <limits>
#include <random>
#include <fstream>
#include <boost/program_options.hpp>
#include "cruzdb/db.h"
#include "include/cruzdb/statistics.h"
#include "db/db_impl.h"

namespace po = boost::program_options;

static std::shared_ptr<cruzdb::Statistics> stats;

static std::ostream *key_fetch_stats_out;
static std::ostream *reachable_stats_out;

static inline std::string tostr(uint64_t value)
{
  std::stringstream ss;
  ss << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}

static std::vector<std::string> fill(cruzdb::DB *db, size_t num_items)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(
      std::numeric_limits<uint32_t>::min(),
      std::numeric_limits<uint32_t>::max());

  std::vector<std::string> keys;
  keys.reserve(num_items);
  {
    std::set<std::string> keys_dedup;
    while (keys_dedup.size() < num_items) {
      uint32_t nkey = dis(gen);
      const std::string key = tostr(nkey);
      keys_dedup.insert(key);
    }
    keys.insert(keys.begin(),
        keys_dedup.begin(), keys_dedup.end());
  }

  std::random_shuffle(keys.begin(), keys.end());

  for (auto& key : keys) {
    auto txn = db->BeginTransaction();
    txn->Put(key, key);
    txn->Commit();
    delete txn;
  }

  std::random_shuffle(keys.begin(), keys.end());

  return keys;
}

// one would expect the smallest number of log reads to be 1, but in most cases
// it is two due to the resolution of intention pointers. generally this
// translation is cached, but we also clear this index when we clear the cache.
// it certainly represents the worst case, and could probably be not be cleared
// and still be argued to be realistic. at the very least we just need to
// remember the settings and scenarios like this so we can answer questions
// about results that seem strange.
static void read_key(cruzdb::DB *db, const std::string& key)
{
  static_cast<cruzdb::DBImpl*>(db)->ClearCaches();
  int ret = stats->Reset();
  assert(ret);

  std::string value;
  ret = db->Get(key, &value);
  assert(ret == 0);

  // clearing cache worked: we should be reading the log!
  assert(stats->getTickerCount(cruzdb::Tickers::LOG_READS) > 0);

  // the cache should be large enough to hold all the nodes read. this means we
  // don't need to worry about double counting etc...
  assert(stats->getTickerCount(cruzdb::Tickers::NODE_CACHE_FREE) == 0);

  // the total number of nodes fetched from the node cache during tree
  // traversal. for a Get() operation this would be at most the height of the
  // tree.
  auto num_fetches = stats->getTickerCount(cruzdb::Tickers::NODE_CACHE_FETCHES);

  // the total number of nodes fetched from the node cache during tree traversal
  // that were already in the cache.
  auto cache_hits = stats->getTickerCount(cruzdb::Tickers::NODE_CACHE_HIT);

  // the total number of nodes read out of afterimages and added to the cache.
  // this includes nodes in an afterimage that may never be accessed. 
  auto nodes_read = stats->getTickerCount(cruzdb::Tickers::NODE_CACHE_NODES_READ);

  if (key_fetch_stats_out) {
    *key_fetch_stats_out
      << num_fetches << ","
      << cache_hits << ","
      << nodes_read
      << std::endl;
  }
}

int main(int argc, char **argv)
{
  size_t num_items;
  std::string name;

  po::options_description opts("General options");
  opts.add_options()
    ("help,h", "show help message")
    ("num-items", po::value<size_t>(&num_items)->default_value(1000), "num items")
    ("name", po::value<std::string>(&name)->default_value(""), "name")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, opts), vm);

  if (vm.count("help")) {
    std::cout << opts << std::endl;
    return 1;
  }

  po::notify(vm);

  std::ofstream reachable_stats_outfile;
  std::ofstream key_fetch_stats_outfile;

  std::string reachable_stats = name + "_reachable.csv";
  std::string key_fetch_stats = name + "_key_fetches.csv";

  if (name.size()) {
    if (name == "-") {
      reachable_stats_out = &std::cout;
      key_fetch_stats_out = &std::cout;
    } else {
      reachable_stats_outfile.open(reachable_stats, std::ios::trunc);
      reachable_stats_out = &reachable_stats_outfile;
      key_fetch_stats_outfile.open(key_fetch_stats, std::ios::trunc);
      key_fetch_stats_out = &key_fetch_stats_outfile;
    }
  }

  zlog::Log *log;
  int ret = zlog::Log::Create("ram", "log", {}, "", "", &log);
  assert(ret == 0);

  stats = cruzdb::CreateDBStatistics();

  cruzdb::DB *db;
  cruzdb::Options options;
  options.statistics = stats;
  ret = cruzdb::DB::Open(options, log, true, &db);
  assert(ret == 0);

  auto keys = fill(db, num_items);

  // this waits for the txn processor and other stuff to finish up which might
  // be holding references to tree nodes. those references prevent the cache
  // clearing from being effective leading to possible key access that have
  // cache hits which isn't good for our experiment that expects all accesses to
  // hit the log. instead of sleeping here, the log should expose a flush
  // interface to let intentions in flight make it through the txn processing
  // pipeline.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  if (key_fetch_stats_out) {
    *key_fetch_stats_out << "num_fetches,cache_hits,nodes_read" << std::endl;
  }

  for (auto& key : keys) {
    read_key(db, key);
  }

  if (reachable_stats_out) {
    *reachable_stats_out << "pos,reachable,total" << std::endl;
    auto usage = static_cast<cruzdb::DBImpl*>(db)->reachable_node_stats();
    for (auto it : usage) {
      *reachable_stats_out << it.first << "," << it.second.second
        << "," << it.second.first << std::endl;
    }
  }

  delete db;
  delete log;

  if (name.size()) {
    reachable_stats_outfile.flush();
    key_fetch_stats_outfile.flush();
    if (name != "-") {
      reachable_stats_outfile.close();
      key_fetch_stats_outfile.close();
    }
  }

  return 0;
}
