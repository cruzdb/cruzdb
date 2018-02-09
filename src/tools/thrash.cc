#include <thread>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <limits>
#include <random>
#include <spdlog/spdlog.h>
#include <boost/program_options.hpp>
#include "port/stack_trace.h"
#include "cruzdb/db.h"

namespace po = boost::program_options;

class TempDir {
 public:
  TempDir() {
    memset(path, 0, sizeof(path));
    sprintf(path, "/tmp/cruzdb.db.XXXXXX");
    assert(mkdtemp(path));
  }

  ~TempDir() {
    char cmd[64];
    memset(cmd, 0, sizeof(cmd));
    sprintf(cmd, "rm -rf %s", path);
    assert(system(cmd) == 0);
  }

  char path[32];
};

template<class T>
static inline std::string tostr(const T& prefix, uint32_t value)
{
  std::stringstream ss;
  ss << prefix << "." << std::setw(10) << std::setfill('0') << value;
  return ss.str();
}

static std::map<std::string, std::string> get_map(cruzdb::DB *db,
    cruzdb::Snapshot *snapshot)
{
  std::map<std::string, std::string> ret;
  auto it = db->NewIterator(snapshot);
  it->SeekToFirst();
  while (it->Valid()) {
    ret[it->key().ToString()] = it->value().ToString();
    it->Next();
  }
  return ret;
}

static void runner(cruzdb::DB *db,
    std::map<std::string, std::string>& cc_map)
{
  std::thread::id tid = std::this_thread::get_id();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(
      std::numeric_limits<uint32_t>::min(),
      std::numeric_limits<uint32_t>::max());

  for (int i = 0; i < 1000; i++) {
    auto txn = db->BeginTransaction();
    uint32_t nkey = dis(gen);
    const std::string key = tostr(tid, nkey);
    txn->Put(key, key);
    cc_map[key] = key;
    txn->Commit();
    delete txn;
  }
}

int main(int argc, char **argv)
{
  int nthreads;

  po::options_description opts("General options");
  opts.add_options()
    ("help,h", "show help message")
    ("nthreads", po::value<int>(&nthreads)->default_value(1), "num threads")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, opts), vm);

  if (vm.count("help")) {
    std::cout << opts << std::endl;
    return 1;
  }

  po::notify(vm);

  auto logger = spdlog::stdout_color_mt("cruzdb");
  rocksdb::port::InstallStackTraceHandler();

  zlog::Log *log;
  int ret = zlog::Log::Create("ram", "log", {}, "", "", &log);
  assert(ret == 0);

  cruzdb::DB *db;
  ret = cruzdb::DB::Open(log, true, &db);
  assert(ret == 0);

  std::vector<std::thread> threads;
  std::vector<std::map<std::string, std::string>> cc_maps(nthreads);
  for (int i = 0; i < nthreads; i++) {
    threads.push_back(std::thread(runner, db, std::ref(cc_maps[i])));
  }

  for (auto& t : threads) {
    t.join();
  }

  // truth
  std::map<std::string, std::string> cc_map;
  for (const auto& m : cc_maps) {
    cc_map.insert(m.begin(), m.end());
  }

  auto db_map = get_map(db, db->GetSnapshot());
  assert(db_map == cc_map);

  delete db;
  delete log;

  return 0;
}
