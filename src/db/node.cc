#include "db_impl.h"

// TODO: this is dumb... we should move these definitions out outside classes so
// we don't have this issue..
namespace cruzdb {

SharedNodeRef NodePtr::fetch(uint64_t pos, std::vector<std::pair<int64_t, int>>& trace)
{
  assert(db_);
  return db_->fetch(trace, pos, offset_);
}

uint64_t NodePtr::resolve_intention_to_csn(uint64_t intention) {
  return db_->resolve_intention_to_csn(intention);
}

uint64_t Node::resolve_intention_to_csn(DBImpl *db, uint64_t intention) {
  return db->resolve_intention_to_csn(intention);
}

}
