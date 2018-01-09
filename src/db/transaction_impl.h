#pragma once
#include "cruzdb/transaction.h"
#include "db/cruzdb.pb.h"
#include "db/node.h"
#include "db/persistent_tree.h"

namespace cruzdb {

class DBImpl;

class TransactionImpl : public Transaction {
 public:
  TransactionImpl(DBImpl *db, NodePtr root, int64_t root_intention,
      int64_t rid, uint64_t token);

  ~TransactionImpl();

 public:
  virtual int Get(const zlog::Slice& key, std::string *value) override;
  virtual void Put(const zlog::Slice& key, const zlog::Slice& value) override;
  virtual void Delete(const zlog::Slice& key) override;

  virtual bool Commit() override;

 public:
  cruzdb_proto::Intention& GetIntention() {
    return intention_;
  }

  uint64_t Token() const {
    return token_;
  }

 private:
  DBImpl *db_;
  PersistentTree tree_;
  const uint64_t token_;
  bool committed_;
  cruzdb_proto::Intention intention_;
};

}
