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
 */
class TransactionImpl : public Transaction {
 public:
  TransactionImpl(DBImpl *db, NodePtr root, int64_t root_intention, int64_t rid,
      uint64_t max_intention_resolvable) :
    db_(db),
    tree_(db, root, root_intention, rid, max_intention_resolvable),
    committed_(false),
    aborted_(false)
  {
    //assert(rid_ < 0); this doesn't hold any longer because we are, silly
    //enough, using the txn as a container for the after image when we read up
    //the intention from the log. one of the immediate next steps will be to
    //pull out the tree logic so its useful outside the txn context. TODO.
    //intention_.set_snapshot(src_root_.csn());
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

 private:
  DBImpl *db_;
  PersistentTree tree_;

  bool committed_;
  bool aborted_;

  cruzdb_proto::Intention intention_;
};

}
