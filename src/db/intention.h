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
    intention_.set_flush(false); // TODO: annoying to have to set a value here
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

  void Copy(const zlog::Slice& key) {
    assert(!pos_);
    auto op = intention_.add_ops();
    op->set_op(cruzdb_proto::TransactionOp::COPY);
    op->set_key(key.ToString());
  }

  bool Flush() const {
    return intention_.flush();
  }

  uint64_t Snapshot() const {
    return intention_.snapshot();
  }

  uint64_t Token() const {
    return intention_.token();
  }

  std::string Serialize() {
    assert(!pos_);

    // ensure that copy and non-copy operations are not mixed within the same
    // intention. the reasoning is that copy operations are used for GC, and
    // never conflict. so if we tried to do something like incremental GC where
    // we slowly fed copy operations through the transaction stream, an aborted
    // intention would mean special handling to avoid losing the copy operation.
    // for this reason we simplify for now by not allowing mixed operations.
    boost::optional<bool> copying;
    for (const auto& op : intention_.ops()) {
      if (op.op() == cruzdb_proto::TransactionOp::COPY) {
        if (!copying) {
          copying = true;
        }
        assert(*copying);
      } else {
        if (!copying) {
          copying = false;
        }
        assert(!*copying);
      }
    }

    assert(copying);
    if (*copying)
      intention_.set_flush(true);
    else
      intention_.set_flush(false);

    cruzdb_proto::LogEntry entry;
    entry.set_type(cruzdb_proto::LogEntry::INTENTION);
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

  std::set<std::string> OpKeys() const {
    std::set<std::string> keys;
    for (auto& op : intention_.ops()) {
      keys.insert(op.key());
    }
    return keys;
  }

  std::set<std::string> UpdateOpKeys() const {
    std::set<std::string> keys;
    for (auto& op : intention_.ops()) {
      if (op.op() == cruzdb_proto::TransactionOp::PUT ||
          op.op() == cruzdb_proto::TransactionOp::DELETE) {
        keys.insert(op.key());
      }
    }
    return keys;
  }

 private:
  cruzdb_proto::Intention intention_;
  boost::optional<uint64_t> pos_;
};

}
