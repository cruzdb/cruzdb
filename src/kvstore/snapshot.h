#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "node.h"

namespace cruzdb {

class Snapshot {
 public:
  Snapshot(DBImpl *db, const NodePtr root) :
    db(db), root(root)
  {}

  DBImpl *db;
  NodePtr root;
};

}
