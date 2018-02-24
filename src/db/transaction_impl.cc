#include "db_impl.h"

static std::string prefix_string(const std::string& prefix,
    const std::string& value)
{
  auto out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

namespace cruzdb {

  // root intention unsigned?
TransactionImpl::TransactionImpl(DBImpl *db, NodePtr root,
    uint64_t snapshot, int64_t rid, uint64_t token) :
  db_(db),
  token_(token),
  tree_(std::make_unique<PersistentTree>(db_, root, rid)),
  intention_(std::make_unique<Intention>(snapshot, token_)),
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
  assert(intention_);
  assert(!committed_);

  intention_->Get(key);
  return tree_->Get(PREFIX_USER, key, value);
}

void TransactionImpl::Put(const zlog::Slice& key, const zlog::Slice& value)
{
  return Put(PREFIX_USER, key, value);
}

void TransactionImpl::Delete(const zlog::Slice& key)
{
  assert(tree_);
  assert(intention_);
  assert(!committed_);

  intention_->Delete(key);
  tree_->Delete(PREFIX_USER, key);
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

void TransactionImpl::Put(const std::string& prefix, const zlog::Slice& key,
    const zlog::Slice& value)
{
  assert(tree_);
  assert(intention_);
  assert(!committed_);

  auto prefixed_key = prefix_string(prefix, key.ToString());

  intention_->Put(prefixed_key, value);
  tree_->Put(prefixed_key, value);
}

}
