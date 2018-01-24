#include "iterator_impl.h"
#include "db_impl.h"

namespace cruzdb {

class IteratorTraceApplier {
 public:
  explicit IteratorTraceApplier(DBImpl *db) :
    db_(db)
  {}

  ~IteratorTraceApplier() {
    db_->UpdateLRU(trace);
  }

  std::vector<NodeAddress> trace;

 private:
  DBImpl *db_;
};

RawIteratorImpl::RawIteratorImpl(Snapshot *snapshot) :
  snapshot_(snapshot)
{
}

bool RawIteratorImpl::Valid() const
{
  return !stack_.empty();
}

void RawIteratorImpl::SeekToFirst()
{
  IteratorTraceApplier ta(snapshot_->db);

  // clear stack
  std::stack<SharedNodeRef> stack;
  stack_.swap(stack);

  // all the way to the left
  SharedNodeRef node = snapshot_->root.ref(ta.trace);
  while (node != Node::Nil()) {
    stack_.push(node);
    node = node->left.ref(ta.trace);
  }

  dir = Forward;
}

void RawIteratorImpl::SeekToLast()
{
  IteratorTraceApplier ta(snapshot_->db);

  // clear stack
  std::stack<SharedNodeRef> stack;
  stack_.swap(stack);

  // all the way to the right
  SharedNodeRef node = snapshot_->root.ref(ta.trace);
  while (node != Node::Nil()) {
    stack_.push(node);
    node = node->right.ref(ta.trace);
  }

  dir = Reverse;
}

void RawIteratorImpl::Seek(const zlog::Slice& key)
{
  IteratorTraceApplier ta(snapshot_->db);

  // clear stack
  std::stack<SharedNodeRef> stack;
  stack_.swap(stack);

  SharedNodeRef node = snapshot_->root.ref(ta.trace);
  while (node != Node::Nil()) {
    int cmp = key.compare(zlog::Slice(node->key().data(),
          node->key().size()));
    if (cmp == 0) {
      stack_.push(node);
      break;
    } else if (cmp < 0) {
      stack_.push(node);
      node = node->left.ref(ta.trace);
    } else
      node = node->right.ref(ta.trace);
  }

  assert(stack_.empty() ||
      zlog::Slice(stack_.top()->key().data(),
        stack_.top()->key().size()).compare(key) >= 0);

  dir = Forward;
}

void RawIteratorImpl::SeekForward(const zlog::Slice& key)
{
  IteratorTraceApplier ta(snapshot_->db);

  // clear stack
  std::stack<SharedNodeRef> stack;
  stack_.swap(stack);

  SharedNodeRef node = snapshot_->root.ref(ta.trace);
  while (node != Node::Nil()) {
    int cmp = key.compare(zlog::Slice(node->key().data(),
          node->key().size()));
    if (cmp == 0) {
      stack_.push(node);
      break;
    } else if (cmp < 0) {
      stack_.push(node);
      node = node->left.ref(ta.trace);
    } else
      node = node->right.ref(ta.trace);
  }

  assert(stack_.empty() ||
      zlog::Slice(stack_.top()->key().data(),
        stack_.top()->key().size()).compare(key) == 0);

  dir = Forward;
}

void RawIteratorImpl::SeekPrevious(const zlog::Slice& key)
{
  IteratorTraceApplier ta(snapshot_->db);

  // clear stack
  std::stack<SharedNodeRef> stack;
  stack_.swap(stack);

  SharedNodeRef node = snapshot_->root.ref(ta.trace);
  while (node != Node::Nil()) {
    int cmp = key.compare(zlog::Slice(node->key().data(),
          node->key().size()));
    if (cmp == 0) {
      stack_.push(node);
      break;
    } else if (cmp < 0) {
      node = node->left.ref(ta.trace);
    } else {
      stack_.push(node);
      node = node->right.ref(ta.trace);
    }
  }

  assert(stack_.empty() ||
      zlog::Slice(stack_.top()->key().data(),
        stack_.top()->key().size()).compare(key) == 0);

  dir = Reverse;
}

void RawIteratorImpl::Next()
{
  IteratorTraceApplier ta(snapshot_->db);

  assert(!stack_.empty());
  if (dir == Reverse) {
    SeekForward(RawIteratorImpl::key());
    assert(dir == Forward);
  }
  assert(!stack_.empty());
  SharedNodeRef node = stack_.top()->right.ref(ta.trace);
  stack_.pop();
  while (node != Node::Nil()) {
    stack_.push(node);
    node = node->left.ref(ta.trace);
  }
}

void RawIteratorImpl::Prev()
{
  IteratorTraceApplier ta(snapshot_->db);

  assert(!stack_.empty());
  if (dir == Forward) {
    SeekPrevious(RawIteratorImpl::key());
    assert(dir == Reverse);
  }
  assert(!stack_.empty());
  SharedNodeRef node = stack_.top()->left.ref(ta.trace);
  stack_.pop();
  while (node != Node::Nil()) {
    stack_.push(node);
    node = node->right.ref(ta.trace);
  }
}

zlog::Slice RawIteratorImpl::key() const
{
  assert(!stack_.empty());
  return zlog::Slice(stack_.top()->key().data(),
      stack_.top()->key().size());
}

zlog::Slice RawIteratorImpl::value() const
{
  assert(!stack_.empty());
  return zlog::Slice(stack_.top()->val().data(),
      stack_.top()->val().size());
}

}
