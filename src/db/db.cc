#include "db_impl.h"
#include <unistd.h>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <spdlog/spdlog.h>

static std::string prefix_string(const std::string& prefix,
    const std::string& value)
{
  auto out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

namespace cruzdb {

DB::~DB()
{
}

int DB::Open(zlog::Log *log, bool create_if_empty, DB **db)
{
  return Open(log, create_if_empty, db, nullptr);
}

int DB::Open(zlog::Log *log, bool create_if_empty, DB **db,
    std::shared_ptr<spdlog::logger> logger)
{
  uint64_t tail;
  int ret = log->CheckTail(&tail);
  assert(ret == 0);

  if (tail == 0) {
    if (!create_if_empty) {
      return -EINVAL;
    }

    // write intention to position 0
    {
      cruzdb_proto::Intention intention;
      intention.set_snapshot(-1);
      intention.set_token(0);

      cruzdb_proto::LogEntry entry;
      entry.set_type(cruzdb_proto::LogEntry::INTENTION);
      entry.set_allocated_intention(&intention);
      assert(entry.IsInitialized());

      std::string blob;
      assert(entry.SerializeToString(&blob));
      entry.release_intention();

      uint64_t pos;
      ret = log->Append(blob, &pos);
      assert(ret == 0);
      assert(pos == 0);
    }

    // build and write the corresponding afterimage.
    //
    // TODO: ideally we would _not_ write this afterimage, and would package any
    // initial data up into the init intention, and let it be processed as
    // normal. to do this we want to avoid special cases. one way to avoid these
    // special cases might be to fill position 0, write the init intention at
    // position 1, using a snapshot of position 0 so it is serial.
    {
      cruzdb_proto::AfterImage after_image;

      // add intention 0 to the committed intention index
      std::stringstream ci_key;
      ci_key << std::setw(20) << std::setfill('0') << 0;

      auto node = after_image.add_tree();
      node->set_red(false);
      node->set_key(prefix_string(PREFIX_COMMITTED_INTENTION, ci_key.str()));
      node->set_val("");

      auto left = node->mutable_left();
      left->set_nil(true);
      left->set_self(false);

      auto right = node->mutable_right();
      right->set_nil(true);
      right->set_self(false);

      after_image.set_intention(0);

      // serialize and append
      cruzdb_proto::LogEntry entry;
      entry.set_type(cruzdb_proto::LogEntry::AFTER_IMAGE);
      entry.set_allocated_after_image(&after_image);
      assert(entry.IsInitialized());

      std::string blob;
      assert(entry.SerializeToString(&blob));
      entry.release_after_image();

      uint64_t pos;
      ret = log->Append(blob, &pos);
      assert(ret == 0);
      assert(pos > 0);
    }
  }

  DBImpl::RestorePoint point;
  uint64_t latest_intention;
  ret = DBImpl::FindRestorePoint(log, point, latest_intention);
  assert(ret == 0);

  DBImpl *impl = new DBImpl(log, point, logger);

  // if there is stuff to roll forward
  impl->WaitOnIntention(latest_intention);

  *db = impl;

  return 0;
}

}
