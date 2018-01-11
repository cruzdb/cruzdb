#include "db_impl.h"

namespace cruzdb {

  // root intention unsigned?
TransactionImpl::TransactionImpl(DBImpl *db, NodePtr root,
    uint64_t snapshot, int64_t rid, uint64_t token) :
  db_(db),
  tree_(std::make_unique<PersistentTree>(db, root, rid)),
  intention_(snapshot, token),
  committed_(false)
{
  assert(tree_);
  assert(tree_->rid() < 0);
}

TransactionImpl::~TransactionImpl()
{
}

int TransactionImpl::Get(const zlog::Slice& key, std::string *value)
{
  assert(tree_);
  assert(!committed_);

  intention_.Get(key);
  return tree_->Get(key, value);
}

void TransactionImpl::Put(const zlog::Slice& key, const zlog::Slice& value)
{
  assert(tree_);
  assert(!committed_);

  intention_.Put(key, value);
  tree_->Put(key, value);
}

void TransactionImpl::Delete(const zlog::Slice& key)
{
  assert(tree_);
  assert(!committed_);

  intention_.Delete(key);
  tree_->Delete(key);
}

bool TransactionImpl::Commit()
{
  assert(tree_);
  assert(!committed_);
  committed_ = true;

  if (tree_->ReadOnly()) {
    return true;
  }

  return db_->CompleteTransaction(this);
}

}
