#include <deque>
#include <sstream>
#include "db_impl.h"

namespace cruzdb {

TransactionImpl::TransactionImpl(DBImpl *db, NodePtr root,
    int64_t root_intention, int64_t rid, uint64_t token) :
  db_(db),
  tree_(db, root, rid),
  token_(token),
  committed_(false)
{
  assert(tree_.rid() < 0);
  intention_.set_snapshot_intention(root_intention);
}

TransactionImpl::~TransactionImpl()
{
}

int TransactionImpl::Get(const zlog::Slice& key, std::string *value)
{
  assert(!committed_);

  auto op = intention_.add_ops();
  op->set_op(cruzdb_proto::TransactionOp::GET);
  op->set_key(key.ToString());

  return tree_.Get(key, value);
}

void TransactionImpl::Put(const zlog::Slice& key, const zlog::Slice& value)
{
  assert(!committed_);

  auto op = intention_.add_ops();
  op->set_op(cruzdb_proto::TransactionOp::PUT);
  op->set_key(key.ToString());
  op->set_val(value.ToString());

  tree_.Put(key, value);
}

void TransactionImpl::Delete(const zlog::Slice& key)
{
  assert(!committed_);

  auto op = intention_.add_ops();
  op->set_op(cruzdb_proto::TransactionOp::DELETE);
  op->set_key(key.ToString());

  tree_.Delete(key);
}

bool TransactionImpl::Commit()
{
  assert(!committed_);
  committed_ = true;

  if (tree_.ReadOnly()) {
    return true;
  }

  return db_->CompleteTransaction(this);
}

}
