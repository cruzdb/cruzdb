#pragma once
#include "db/cruzdb.pb.h"

class Intention {
 public:
  Intention(uint64_t snapshot, uint64_t token) {
    intention_.set_snapshot(snapshot);
    intention_.set_token(token);
    assert(intention_.IsInitialized());
  }

  Intention(const cruzdb_proto::Intention& intention) :
    intention_(intention)
  {
    assert(intention_.IsInitialized());
  }

  void Get(const zlog::Slice& key) {
    auto op = intention_.add_ops();
    op->set_op(cruzdb_proto::TransactionOp::GET);
    op->set_key(key.ToString());
  }

  void Put(const zlog::Slice& key, const zlog::Slice& value) {
    auto op = intention_.add_ops();
    op->set_op(cruzdb_proto::TransactionOp::PUT);
    op->set_key(key.ToString());
    op->set_val(value.ToString());
  }

  void Delete(const zlog::Slice& key) {
    auto op = intention_.add_ops();
    op->set_op(cruzdb_proto::TransactionOp::DELETE);
    op->set_key(key.ToString());
  }

  uint64_t Snapshot() const {
    return intention_.snapshot();
  }

  uint64_t Token() const {
    return intention_.token();
  }

  std::string Serialize() {
    cruzdb_proto::LogEntry entry;
    entry.set_allocated_intention(&intention_);
    assert(entry.IsInitialized());

    std::string blob;
    assert(entry.SerializeToString(&blob));
    // cannot use entry after release...
    entry.release_intention();

    return blob;
  }

  auto begin() const {
    return intention_.ops().begin();
  }

  auto end() const {
    return intention_.ops().end();
  }

 private:
  cruzdb_proto::Intention intention_;
};
