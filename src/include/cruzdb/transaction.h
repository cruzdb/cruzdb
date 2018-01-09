#pragma once
#include <string>
#include <zlog/slice.h>

namespace cruzdb {

// A read-only transaction will always commit immediately. Are there use cases
// in which it's desirable to find out if a set of keys changed during some
// period? If so, we could examine the transaction's conflict zone at commit
// time without actually appending any intentions to the log.
class Transaction {
 public:
  virtual ~Transaction() {}

  virtual int Get(const zlog::Slice& key, std::string *value) = 0;
  virtual void Put(const zlog::Slice& key, const zlog::Slice& value) = 0;
  virtual void Delete(const zlog::Slice& key) = 0;

  virtual bool Commit() = 0;
};

}
