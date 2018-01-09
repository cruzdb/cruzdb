#pragma once
#include "db/cruzdb.pb.h"

class Intention {
 public:
  Intention(int64_t snapshot) {
    intention_.set_snapshot(snapshot);
  }

  Intention(const cruzdb_proto::Intention& intention) :
    intention_(intention)
  {}

  std::string Serialize(uint64_t token) {
    intention_.set_token(token);
    cruzdb_proto::LogEntry entry;
    entry.set_allocated_intention(&intention_);
    assert(entry.IsInitialized());

    std::string blob;
    assert(entry.SerializeToString(&blob));
    // cannot use entry after release...
    entry.release_intention();

    return blob;
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

  int64_t Snapshot() const {
    return intention_.snapshot();
  }

  int64_t Token() const {
    return intention_.token();
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
