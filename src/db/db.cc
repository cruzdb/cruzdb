#include "db_impl.h"
#include <unistd.h>
#include <sstream>
#include <chrono>

namespace cruzdb {

DB::~DB()
{
}

int DB::Open(zlog::Log *log, bool create_if_empty, DB **db)
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
      intention.set_snapshot_intention(-1);
      intention.set_token(0);

      cruzdb_proto::LogEntry entry;
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

    // write a corresponding after image. when/if we start adding some initial
    // data to the database, then we'll need to make sure that this after image
    // reflects that.
    {
      cruzdb_proto::AfterImage after_image;
      after_image.set_intention(0);

      cruzdb_proto::LogEntry entry;
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

  RestorePoint point;
  uint64_t latest_intention;
  ret = DBImpl::FindRestorePoint(log, point, latest_intention);
  assert(ret == 0);

  DBImpl *impl = new DBImpl(log, point);

  // if there is stuff to roll forward
  impl->WaitOnIntention(latest_intention);

  *db = impl;

  return 0;
}

}
