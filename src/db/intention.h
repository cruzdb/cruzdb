#pragma once
#include "db/cruzdb.pb.h"

namespace cruzdb {

class Intention {
 public:
  Intention(uint64_t snapshot, uint64_t token) :
    pos_(boost::none)
  {
    intention_.set_snapshot(snapshot);
    intention_.set_token(token);
    assert(intention_.IsInitialized());
  }

  Intention(const cruzdb_proto::Intention& intention, uint64_t pos) :
    intention_(intention),
    pos_(pos)
  {
    assert(intention_.IsInitialized());
  }

  void Get(const zlog::Slice& key) {
    assert(!pos_);
    auto op = intention_.add_ops();
    op->set_op(cruzdb_proto::TransactionOp::GET);
    op->set_key(key.ToString());
  }

  void Put(const zlog::Slice& key, const zlog::Slice& value) {
    assert(!pos_);
    auto op = intention_.add_ops();
    op->set_op(cruzdb_proto::TransactionOp::PUT);
    op->set_key(key.ToString());
    op->set_val(value.ToString());
  }

  void Delete(const zlog::Slice& key) {
    assert(!pos_);
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
    assert(!pos_);
    cruzdb_proto::LogEntry entry;
    entry.set_allocated_intention(&intention_);
    assert(entry.IsInitialized());

    std::string blob;
    assert(entry.SerializeToString(&blob));
    entry.release_intention();

    return blob;
  }

  auto begin() const {
    assert(pos_);
    return intention_.ops().begin();
  }

  auto end() const {
    assert(pos_);
    return intention_.ops().end();
  }

  uint64_t Position() const {
    assert(pos_);
    return *pos_;
  }

  void SetPosition(uint64_t pos) {
    assert(!pos_);
    pos_ = pos;
  }

 private:
  cruzdb_proto::Intention intention_;
  boost::optional<uint64_t> pos_;
};

}
