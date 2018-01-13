#pragma once
#include <vector>
#include <zlog/log.h>
#include "iterator.h"
#include "transaction.h"

namespace cruzdb {

class Snapshot;
class Iterator;
class Transaction;

class DB {
 public:
  DB() {}
  virtual ~DB();

  DB(const DB&) = delete;
  void operator=(const DB&) = delete;

  /*
   *
   */
  static int Open(zlog::Log *log, bool create_if_empty, DB **db);

  /*
   *
   */
  virtual Transaction *BeginTransaction() = 0;

  /*
   *
   */
  virtual Snapshot *GetSnapshot() = 0;

  /*
   *
   */
  virtual void ReleaseSnapshot(Snapshot *snapshot) = 0;

  /*
   *
   */
  virtual Iterator *NewIterator(Snapshot *snapshot) = 0;

  Iterator *NewIterator() {
    return NewIterator(GetSnapshot());
  }

  /*
   * Lookup a key in the latest committed database snapshot.
   */
  virtual int Get(const zlog::Slice& key, std::string *value) = 0;
};

}
