#pragma once
#include <deque>
#include "cruzdb/transaction.h"
#include "db/cruzdb.pb.h"
#include "node.h"
#include "persistent_tree.h"

namespace cruzdb {

class DBImpl;

/*
 * would be nice to have some mechanism here for really enforcing the idea
 * that this transaction is isolated.
 *
 * TODO: combine root intention and nodeptr.
 */
class TransactionImpl : public Transaction {
 public:
  TransactionImpl(DBImpl *db, NodePtr root, int64_t root_intention, int64_t rid,
      uint64_t token) :
    db_(db),
    tree_(db, root, rid),
    committed_(false),
    aborted_(false),
    token_(token)
  {
    assert(tree_.rid() < 0);
    intention_.set_snapshot_intention(root_intention);
  }

  ~TransactionImpl() {
    if (!aborted_ && !committed_)
      Abort();
  }

  virtual void Put(const zlog::Slice& key, const zlog::Slice& value) override;
  virtual void Delete(const zlog::Slice& key) override;
  virtual int Get(const zlog::Slice& key, std::string *value) override;
  virtual bool Commit() override;
  virtual void Abort() override;

 public:
  void MarkCommitted() {
    committed_ = true;
  }

  cruzdb_proto::Intention& GetIntention() {
    return intention_;
  }

  uint64_t Token() const {
    return token_;
  }

 private:
  DBImpl *db_;
  PersistentTree tree_;

  bool committed_;
  bool aborted_;
  const uint64_t token_;

  cruzdb_proto::Intention intention_;
};

}
