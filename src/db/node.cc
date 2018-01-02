#include "db_impl.h"

// TODO: this is dumb... we should move these definitions out outside classes so
// we don't have this issue..
namespace cruzdb {

SharedNodeRef NodePtr::fetch(boost::optional<NodeAddress>& address,
    std::vector<NodeAddress>& trace)
{
  assert(db_);
  return db_->fetch(trace, address);
}

}
