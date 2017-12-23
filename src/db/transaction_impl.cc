// The copy-on-write red-black tree that forms the basis for the database is
// adapted from:
//    http://codereview.stackexchange.com/questions/119728/persistent-set-red-black-tree-follow-up
// a copy of the original code is in commit 2ac82b5 at src/kvstore/persistent-rbtree.cc
#include <deque>
#include <sstream>
#include "db_impl.h"

namespace cruzdb {

void TransactionImpl::Put(const zlog::Slice& key, const zlog::Slice& value)
{
  auto op = intention_.add_ops();
  op->set_op(cruzdb_proto::TransactionOp::PUT);
  op->set_key(key.ToString());
  op->set_val(value.ToString());
  tree_.Put(key, value);
}

int TransactionImpl::Get(const zlog::Slice& key, std::string *value)
{
  auto op = intention_.add_ops();
  op->set_op(cruzdb_proto::TransactionOp::GET);
  op->set_key(key.ToString());
  return tree_.Get(key, value);
}

void TransactionImpl::Delete(const zlog::Slice& key)
{
  auto op = intention_.add_ops();
  op->set_op(cruzdb_proto::TransactionOp::DELETE);
  op->set_key(key.ToString());
  tree_.Delete(key);
}

void TransactionImpl::Abort()
{
  assert(!committed_);
  assert(!aborted_);
  aborted_ = true;
  // doesn't seem to be necessary to abort on the db side...
  //db_->AbortTransaction(this);
}

bool TransactionImpl::Commit()
{
  assert(!committed_);
  assert(!aborted_);

  if (tree_.EmptyDelta()) {
    // abort had been here in the days of 1 txn at a time, because when we
    // aborted we needed to notify the db that this txn wasn't blocking anyone
    // any longer.
    //db_->AbortTransaction(this);
    committed_ = true;
    //completed_ = true;
    return true; //?? special case
  }

  return db_->CompleteTransaction(this);
}

}
