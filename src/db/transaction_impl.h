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

  // exported transaction api
 public:
  virtual int Get(const zlog::Slice& key, std::string *value) override;
  virtual void Put(const zlog::Slice& key, const zlog::Slice& value) override;
  virtual void Delete(const zlog::Slice& key) override;
  virtual bool Commit() override;

  // internal api
 public:
  void Put(const std::string& prefix, const zlog::Slice& key,
      const zlog::Slice& value);

 public:
  uint64_t Token() const {
    return token_;
  }

  std::unique_ptr<Intention> GetIntention() {
    return std::move(intention_);
  }

  std::unique_ptr<PersistentTree> Tree() {
    return std::move(tree_);
  }

 private:
  DBImpl *db_;
  const uint64_t token_;
  std::unique_ptr<PersistentTree> tree_;
  std::unique_ptr<Intention> intention_;
  bool committed_;
};

}
