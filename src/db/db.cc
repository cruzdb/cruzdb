#include "db_impl.h"
#include <unistd.h>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <spdlog/spdlog.h>

namespace cruzdb {

DB::~DB()
{
}

int DB::Open(const Options& options, zlog::Log *log,
    bool create_if_empty, DB **db)
{
  return Open(options, log, create_if_empty, db, nullptr);
}

int DB::Open(const Options& options, zlog::Log *log,
    bool create_if_empty, DB **db,
    std::shared_ptr<spdlog::logger> logger)
{
  auto entry_service = std::unique_ptr<EntryService>(
      new EntryService(options.statistics.get(), log));

  uint64_t tail = entry_service->CheckTail();
  if (tail == 0) {
    if (!create_if_empty) {
      return -EINVAL;
    }

    // TODO: get even more defensive and add a fatal error if position 0 is ever
    // accessed in the I/O layer.
    entry_service->Fill(0);

    auto empty_tree = NodePtr(Node::Nil(), nullptr);
    TransactionImpl txn(nullptr, empty_tree, 0, -1, 0);

    std::stringstream key;
    key << std::setw(20) << std::setfill('0') << 1;
    txn.Put(PREFIX_COMMITTED_INTENTION, key.str(), "");

    auto pos = entry_service->Append(std::move(txn.GetIntention()));
    assert(pos == 1);

    auto tree = std::move(txn.Tree());

    std::vector<SharedNodeRef> delta;
    cruzdb_proto::AfterImage after_image;
    tree->SerializeAfterImage(after_image, 1, delta);
    assert(after_image.intention() == 1);

    pos = entry_service->Append(after_image);
    assert(pos == 2);
  }

  DBImpl::RestorePoint point;
  uint64_t latest_intention;
  int ret = DBImpl::FindRestorePoint(entry_service.get(),
      point, latest_intention);
  assert(ret == 0);

  DBImpl *impl = new DBImpl(log, point,
      std::move(entry_service), logger);

  // if there is stuff to roll forward
  impl->WaitOnIntention(latest_intention);

  *db = impl;

  return 0;
}

}
