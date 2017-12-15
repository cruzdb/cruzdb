#pragma once
#include <string>
#include <zlog/slice.h>

namespace cruzdb {

class Transaction {
 public:
  virtual ~Transaction() {}
  virtual void Put(const zlog::Slice& key, const zlog::Slice& value) = 0;
  virtual int Get(const zlog::Slice& key, std::string *value) = 0;
  virtual void Delete(const zlog::Slice& key) = 0;
  virtual void Commit() = 0;
  virtual void Abort() = 0;
};

}
