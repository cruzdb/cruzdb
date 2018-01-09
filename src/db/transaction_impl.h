#pragma once
#include "cruzdb/transaction.h"
#include "db/node.h"
#include "db/persistent_tree.h"
#include "db/intention.h"

namespace cruzdb {

class DBImpl;

class TransactionImpl : public Transaction {
 public:
  TransactionImpl(DBImpl *db, NodePtr root, uint64_t snapshot,
      int64_t rid, uint64_t token);

  ~TransactionImpl();

 public:
  virtual int Get(const zlog::Slice& key, std::string *value) override;
  virtual void Put(const zlog::Slice& key, const zlog::Slice& value) override;
  virtual void Delete(const zlog::Slice& key) override;

  virtual bool Commit() override;

 public:
  Intention& GetIntention() {
    return intention_;
  }

 private:
  DBImpl *db_;
  PersistentTree tree_;
  Intention intention_;
  bool committed_;
};

}
