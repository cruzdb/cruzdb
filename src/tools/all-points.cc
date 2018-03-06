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
static std::ostream *out;

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

static void read_key(cruzdb::DB *db, const std::string& key)
{
  //std::this_thread::sleep_for(std::chrono::milliseconds(10));
  static_cast<cruzdb::DBImpl*>(db)->ClearCaches();
  int ret = stats->Reset();
  assert(ret);

  auto before = stats->ToString();

  std::string value;
  ret = db->Get(key, &value);
  assert(ret == 0);

  auto after = stats->ToString();

  if (stats->getTickerCount(cruzdb::Tickers::LOG_READS) == 0) {
    std::cout << "BEFORE ========================" << std::endl << before << std::endl;
    std::cout << "AFTER ========================" << std::endl << after << std::endl;
  }

  // it may seem weird that there are no accesses that require only 1 log read.
  // this is because for all the access that read 2 log entries perform an
  // intention to afterimage translation using the iterator resolution strategy
  // that requires reading more from the log. generally this is cached, but this
  // also points to an optimization opportunity to perhaps do some snapshotting
  // of index entries. another option is to figure out if it makes sense to keep
  // that cache around for the particular benchmark we are running (i.e. not
  // clear it out during cache clear steps).
  //
  // at the very least we need remember this so that we can answer questions
  // about what might seem like a weird result.
#if 1
  if (out)
    *out
    << stats->getTickerCount(cruzdb::Tickers::LOG_READS)
    << std::endl;
#endif
}

int main(int argc, char **argv)
{
  size_t num_items;
  std::string out_file;

  po::options_description opts("General options");
  opts.add_options()
    ("help,h", "show help message")
    ("num-items", po::value<size_t>(&num_items)->default_value(1000), "num items")
    ("output", po::value<std::string>(&out_file)->default_value(""), "output file")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, opts), vm);

  if (vm.count("help")) {
    std::cout << opts << std::endl;
    return 1;
  }

  po::notify(vm);

  std::ofstream ofile;
  if (out_file.size()) {
    if (out_file == "-") {
      out = &std::cout;
    } else {
      ofile.open(out_file, std::ios::trunc);
      out = &ofile;
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
  // interface!
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  for (auto& key : keys) {
    read_key(db, key);
  }

  delete db;
  delete log;

  if (out_file.size()) {
    ofile.flush();
    if (out_file != "-")
      ofile.close();
  }

  return 0;
}
