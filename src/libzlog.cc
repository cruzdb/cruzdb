#include <cerrno>
#include <iostream>
#include <sstream>
#include <string>
#include <condition_variable>
#include <rados/librados.hpp>
#include <rados/cls_zlog_client.h>
#include "libzlog.hpp"
#include "libzlog.h"
#include "zlog.pb.h"
#include "protobuf_bufferlist_adapter.h"

namespace zlog {

std::string Log::metalog_oid_from_name(const std::string& name)
{
  std::stringstream ss;
  ss << name << ".meta";
  return ss.str();
}

std::string Log::position_to_oid(uint64_t position)
{
  // round-robin striping
  int slot = position % stripe_size_;
  return slot_to_oid(slot);
}

std::string Log::slot_to_oid(int slot)
{
  std::stringstream ss;
  ss << name_ << "." << slot;
  return ss.str();
}

int Log::Create(librados::IoCtx& ioctx, const std::string& name,
    int stripe_size, SeqrClient *seqr, Log& log)
{
  if (stripe_size <= 0) {
    std::cerr << "Invalid stripe size (" << stripe_size << " <= 0)" << std::endl;
    return -EINVAL;
  }

  if (name.length() == 0) {
    std::cerr << "Invalid log name (empty string)" << std::endl;
    return -EINVAL;
  }

  // pack up config info in a buffer
  zlog_proto::MetaLog config;
  config.set_stripe_size(stripe_size);
  ceph::bufferlist bl;
  pack_msg<zlog_proto::MetaLog>(bl, config);

  // setup rados operation to create log
  librados::ObjectWriteOperation op;
  op.create(true); // exclusive create
  op.write_full(bl);
  cls_zlog_set_projection(op);

  std::string metalog_oid = Log::metalog_oid_from_name(name);
  int ret = ioctx.operate(metalog_oid, &op);
  if (ret) {
    std::cerr << "Failed to create log " << name << " ret "
      << ret << " (" << strerror(-ret) << ")" << std::endl;
    return ret;
  }

  log.ioctx_ = &ioctx;
  log.pool_ = ioctx.get_pool_name();
  log.name_ = name;
  log.metalog_oid_ = metalog_oid;
  log.stripe_size_ = stripe_size;
  log.seqr = seqr;

  ret = log.RefreshProjection();
  if (ret)
    return ret;

  ret = log.Seal(log.epoch_);
  if (ret)
    return ret;

  return 0;
}

int Log::Open(librados::IoCtx& ioctx, const std::string& name,
    SeqrClient *seqr, Log& log)
{
  if (name.length() == 0) {
    std::cerr << "Invalid log name (empty string)" << std::endl;
    return -EINVAL;
  }

  std::string metalog_oid = Log::metalog_oid_from_name(name);

  ceph::bufferlist bl;
  int ret = ioctx.read(metalog_oid, bl, 0, 0);
  if (ret < 0) {
    std::cerr << "failed to read object " << metalog_oid << " ret "
      << ret << std::endl;
    return ret;
  }

  zlog_proto::MetaLog config;
  if (!unpack_msg<zlog_proto::MetaLog>(config, bl)) {
    std::cerr << "failed to parse configuration" << std::endl;
    return -EIO;
  }

  log.ioctx_ = &ioctx;
  log.pool_ = ioctx.get_pool_name();
  log.name_ = name;
  log.metalog_oid_ = metalog_oid;
  log.stripe_size_ = config.stripe_size();
  log.seqr = seqr;

  ret = log.RefreshProjection();
  if (ret)
    return ret;

  return 0;
}

int Log::SetProjection(uint64_t *pepoch)
{
  librados::ObjectWriteOperation op;
  cls_zlog_set_projection(op);
  int ret = ioctx_->operate(metalog_oid_, &op);
  if (ret) {
    std::cerr << "failed to set projection " << ret << std::endl;
    return ret;
  }
  return GetProjection(pepoch);
}

int Log::GetProjection(uint64_t *pepoch)
{
  return cls_zlog_get_projection(*ioctx_, metalog_oid_, pepoch);
}

int Log::Seal(uint64_t epoch)
{
  for (int i = 0; i < stripe_size_; i++) {
    std::string oid = slot_to_oid(i);
    librados::ObjectWriteOperation op;
    cls_zlog_seal(op, epoch);
    int ret = ioctx_->operate(oid, &op);
    if (ret != zlog::CLS_ZLOG_OK) {
      std::cerr << "failed to seal object" << std::endl;
      return ret;
    }
  }
  return 0;
}

int Log::FindMaxPosition(uint64_t epoch, bool *pempty, uint64_t *pposition)
{
  bool log_empty = true;
  uint64_t max_position;

  for (int i = 0; i < stripe_size_; i++) {
    std::string oid = slot_to_oid(i);

    librados::bufferlist bl;
    librados::ObjectReadOperation op;
    int op_ret;
    uint64_t this_pos;

    cls_zlog_max_position(op, epoch, &this_pos, &op_ret);

    int ret = ioctx_->operate(oid, &op, &bl);
    if (ret < 0) {
      if (ret == -ENOENT) // no writes yet
        continue;
      std::cerr << "failed to find max pos " << ret << std::endl;
      return ret;
    }

    if (op_ret == zlog::CLS_ZLOG_OK) {
      if (log_empty) {
        max_position = this_pos;
        log_empty = false;
        continue;
      }
      max_position = std::max(max_position, this_pos);
    }
  }

  *pempty = log_empty;

  if (!log_empty)
    *pposition =  max_position;

  return 0;
}

int Log::RefreshProjection()
{
  for (;;) {
    uint64_t epoch;
    int ret = GetProjection(&epoch);
    if (ret) {
      std::cerr << "failed to get projection ret " << ret << std::endl;
      sleep(1);
      continue;
    }
    epoch_ = epoch;
    break;
  }
  return 0;
}

int Log::CheckTail(uint64_t *pposition, bool increment)
{
  for (;;) {
    int ret = seqr->CheckTail(epoch_, pool_, name_, pposition, increment);
    if (ret == -EAGAIN) {
      //std::cerr << "check tail ret -EAGAIN" << std::endl;
      sleep(1);
      continue;
    } else if (ret == -ERANGE) {
      //std::cerr << "check tail ret -ERANGE" << std::endl;
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }
    return ret;
  }
  assert(0);
}

int Log::CheckTail(std::vector<uint64_t>& positions, size_t count)
{
  if (count <= 0 || count > 100)
    return -EINVAL;

  for (;;) {
    std::vector<uint64_t> result;
    int ret = seqr->CheckTail(epoch_, pool_, name_, result, count);
    if (ret == -EAGAIN) {
      //std::cerr << "check tail ret -EAGAIN" << std::endl;
      sleep(1);
      continue;
    } else if (ret == -ERANGE) {
      //std::cerr << "check tail ret -ERANGE" << std::endl;
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }
    if (ret == 0)
      positions.swap(result);
    return ret;
  }
  assert(0);
}

int Log::CheckTail(const std::set<uint64_t>& stream_ids,
    std::map<uint64_t, std::vector<uint64_t>>& stream_backpointers,
    uint64_t *pposition, bool increment)
{
  for (;;) {
    int ret = seqr->CheckTail(epoch_, pool_, name_, stream_ids,
        stream_backpointers, pposition, increment);
    if (ret == -EAGAIN) {
      //std::cerr << "check tail ret -EAGAIN" << std::endl;
      sleep(1);
      continue;
    } else if (ret == -ERANGE) {
      //std::cerr << "check tail ret -ERANGE" << std::endl;
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }
    return ret;
  }
  assert(0);
}

enum AioType {
  ZLOG_AIO_APPEND,
  ZLOG_AIO_READ,
};

struct zlog::Log::AioCompletionImpl {
  std::condition_variable cond;
  std::mutex lock;
  int ref;
  bool complete;
  bool released;

  /*
   * Common
   *
   * position:
   *   - current attempt (append)
   *   - target (read)
   * bl:
   *  - data being appended (append)
   *  - temp storage for read (read)
   */
  int retval;
  Log::AioCompletion::callback_t safe_cb;
  void *safe_cb_arg;
  uint64_t position;
  ceph::bufferlist bl;
  AioType type;

  /*
   * AioAppend
   *
   * pposition:
   *  - final append position
   */
  uint64_t *pposition;

  /*
   * AioRead
   *
   * pbl:
   *  - where to put result
   */
  ceph::bufferlist *pbl;

  Log *log;
  librados::IoCtx *ioctx;
  librados::AioCompletion *c;

  AioCompletionImpl() :
    ref(1), complete(false), released(false), retval(0)
  {}

  int wait_for_complete() {
    std::unique_lock<std::mutex> l(lock);
    cond.wait(l, [&]{ return complete; });
    return 0;
  }

  int get_return_value() {
    std::lock_guard<std::mutex> l(lock);
    return retval;
  }

  void release() {
    lock.lock();
    assert(!released);
    released = true;
    put_unlock();
  }

  void put_unlock() {
    assert(ref > 0);
    int n = --ref;
    lock.unlock();
    if (!n)
      delete this;
  }

  void get() {
    std::lock_guard<std::mutex> l(lock);
    assert(ref > 0);
    ref++;
  }

  void set_callback(void *arg, zlog::Log::AioCompletion::callback_t cb) {
    std::lock_guard<std::mutex> l(lock);
    safe_cb = cb;
    safe_cb_arg = arg;
  }
};

void zlog::Log::AioCompletion::wait_for_complete()
{
  AioCompletionImpl *impl = (AioCompletionImpl*)pc;
  impl->wait_for_complete();
}

int zlog::Log::AioCompletion::get_return_value()
{
  zlog::Log::AioCompletionImpl *impl = (AioCompletionImpl*)pc;
  return impl->get_return_value();
}

void zlog::Log::AioCompletion::release()
{
  zlog::Log::AioCompletionImpl *impl = (AioCompletionImpl*)pc;
  impl->release();
  delete this;
}

void zlog::Log::AioCompletion::set_callback(void *arg,
    zlog::Log::AioCompletion::callback_t cb)
{
  zlog::Log::AioCompletionImpl *impl = (AioCompletionImpl*)pc;
  impl->set_callback(arg, cb);
}

/*
 *
 */
void aio_safe_cb(librados::completion_t cb, void *arg)
{
  zlog::Log::AioCompletionImpl *impl = (zlog::Log::AioCompletionImpl*)arg;
  librados::AioCompletion *rc = impl->c;
  bool finish = false;

  impl->lock.lock();

  int ret = rc->get_return_value();

  // done with the rados completion
  rc->release();

  assert(impl->type == ZLOG_AIO_APPEND ||
         impl->type == ZLOG_AIO_READ);

  if (ret == zlog::CLS_ZLOG_OK) {
    /*
     * Append was successful. We're done.
     */
    if (impl->type == ZLOG_AIO_APPEND && impl->pposition) {
      *impl->pposition = impl->position;
    } else if (impl->type == ZLOG_AIO_READ && impl->pbl &&
        impl->bl.length() > 0) {
      *impl->pbl = impl->bl;
    }
    ret = 0;
    finish = true;
  } else if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
    /*
     * We'll need to try again with a new epoch.
     */
    ret = impl->log->RefreshProjection();
    if (ret)
      finish = true;
  } else if (ret < 0) {
    /*
     * Encountered a RADOS error.
     */
    finish = true;
  } else if (ret == zlog::CLS_ZLOG_NOT_WRITTEN) {
    assert(impl->type == ZLOG_AIO_READ);
    ret = -ENODEV;
    finish = true;
  } else if (ret == zlog::CLS_ZLOG_INVALIDATED) {
    assert(impl->type == ZLOG_AIO_READ);
    ret = -EFAULT;
    finish = true;
  } else {
    if (impl->type == ZLOG_AIO_APPEND)
      assert(ret == zlog::CLS_ZLOG_READ_ONLY);
    else
      assert(0);
  }

  /*
   * Try append again with a new position. This can happen if above there is a
   * stale epoch that we refresh, or if the position was marked read-only.
   */
  if (!finish) {
    if (impl->type == ZLOG_AIO_APPEND) {
      // if we are appending, get a new position
      uint64_t position;
      ret = impl->log->CheckTail(&position, true);
      if (ret)
        finish = true;
      else
        impl->position = position;
    }

    // we are still good. build a new aio
    if (!finish) {
      impl->c = librados::Rados::aio_create_completion(impl, NULL, aio_safe_cb);
      assert(impl->c);
      // don't need impl->get(): reuse reference

      // build and submit new op
      std::string oid = impl->log->position_to_oid(impl->position);
      switch (impl->type) {
        case ZLOG_AIO_APPEND:
          {
            librados::ObjectWriteOperation op;
            zlog::cls_zlog_write(op, impl->log->epoch_, impl->position, impl->bl);
            ret = impl->ioctx->aio_operate(oid, impl->c, &op);
            if (ret)
              finish = true;
          }
          break;

        case ZLOG_AIO_READ:
          {
            librados::ObjectReadOperation op;
            zlog::cls_zlog_read(op, impl->log->epoch_, impl->position);
            ret = impl->ioctx->aio_operate(oid, impl->c, &op, &impl->bl);
            if (ret)
              finish = true;
          }
          break;

        default:
          assert(0);
      }
    }
  }

  // complete aio if append success, or any error
  if (finish) {
    impl->retval = ret;
    impl->complete = true;
    impl->lock.unlock();
    if (impl->safe_cb)
      impl->safe_cb(impl, impl->safe_cb_arg);
    impl->cond.notify_all();
    impl->lock.lock();
    impl->put_unlock();
    return;
  }

  impl->lock.unlock();
}

Log::AioCompletion *Log::aio_create_completion(void *arg,
    Log::AioCompletion::callback_t cb)
{
  AioCompletionImpl *impl = new AioCompletionImpl;
  impl->safe_cb = cb;
  impl->safe_cb_arg = arg;
  return new AioCompletion(impl);
}

Log::AioCompletion *Log::aio_create_completion()
{
  return aio_create_completion(NULL, NULL);
}

/*
 * The retry for AioAppend is coordinated through the aio_safe_cb callback
 * which will dispatch a new rados operation.
 */
int Log::AioAppend(AioCompletion *c, ceph::bufferlist& data,
    uint64_t *pposition)
{
  // initial position guess
  uint64_t position;
  int ret = CheckTail(&position, true);
  if (ret)
    return ret;

  AioCompletionImpl *impl = (AioCompletionImpl*)c->pc;

  impl->log = this;
  impl->bl = data;
  impl->position = position;
  impl->pposition = pposition;
  impl->ioctx = ioctx_;
  impl->type = ZLOG_AIO_APPEND;

  impl->get(); // rados aio now has a reference
  impl->c = librados::Rados::aio_create_completion(impl, NULL, aio_safe_cb);
  assert(impl->c);

  librados::ObjectWriteOperation op;
  zlog::cls_zlog_write(op, epoch_, position, data);

  std::string oid = position_to_oid(position);
  ret = ioctx_->aio_operate(oid, impl->c, &op);
  /*
   * Currently aio_operate never fails. If in the future that changes then we
   * need to make sure that references to impl and the rados completion are
   * cleaned up correctly.
   */
  assert(ret == 0);

  return ret;
}

int Log::Append(ceph::bufferlist& data, uint64_t *pposition)
{
  for (;;) {
    uint64_t position;
    int ret = CheckTail(&position, true);
    if (ret)
      return ret;

    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, epoch_, position, data);

    std::string oid = position_to_oid(position);
    ret = ioctx_->operate(oid, &op);
    if (ret < 0) {
      std::cerr << "append: failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK) {
      if (pposition)
        *pposition = position;
      return 0;
    }

    if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }

    assert(ret == zlog::CLS_ZLOG_READ_ONLY);
  }
  assert(0);
}

int Log::MultiAppend(ceph::bufferlist& data,
    const std::set<uint64_t>& stream_ids, uint64_t *pposition)
{
  for (;;) {
    /*
     * Get a new spot at the tail of the log and return a set of backpointers
     * for the specified streams. The stream ids and backpointers are stored
     * in the header of the entry being appeneded to the log.
     */
    uint64_t position;
    std::map<uint64_t, std::vector<uint64_t>> stream_backpointers;
    int ret = CheckTail(stream_ids, stream_backpointers, &position, true);
    if (ret)
      return ret;

    assert(stream_ids.size() == stream_backpointers.size());

    zlog_proto::EntryHeader hdr;
    size_t index = 0;
    for (std::set<uint64_t>::const_iterator it = stream_ids.begin();
         it != stream_ids.end(); it++) {
      uint64_t stream_id = *it;
      const std::vector<uint64_t>& backpointers = stream_backpointers[index];
      zlog_proto::StreamBackPointer *ptrs = hdr.add_stream_backpointers();
      ptrs->set_id(stream_id);
      for (std::vector<uint64_t>::const_iterator it2 = backpointers.begin();
           it2 != backpointers.end(); it2++) {
        uint64_t pos = *it2;
        ptrs->add_backpointer(pos);
      }
      index++;
    }

    ceph::bufferlist bl;
    pack_msg_hdr<zlog_proto::EntryHeader>(bl, hdr);
    bl.append(data.c_str(), data.length());

    librados::ObjectWriteOperation op;
    zlog::cls_zlog_write(op, epoch_, position, bl);

    std::string oid = position_to_oid(position);
    ret = ioctx_->operate(oid, &op);
    if (ret < 0) {
      std::cerr << "append: failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK) {
      if (pposition)
        *pposition = position;
      return 0;
    }

    if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }

    assert(ret == zlog::CLS_ZLOG_READ_ONLY);
  }
  assert(0);
}

int Log::Fill(uint64_t epoch, uint64_t position)
{
  for (;;) {
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, epoch, position);

    std::string oid = position_to_oid(position);
    int ret = ioctx_->operate(oid, &op);
    if (ret < 0) {
      std::cerr << "fill: failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK)
      return 0;

    if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }

    assert(ret == zlog::CLS_ZLOG_READ_ONLY);
    return -EROFS;
  }
}

int Log::Fill(uint64_t position)
{
  for (;;) {
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_fill(op, epoch_, position);

    std::string oid = position_to_oid(position);
    int ret = ioctx_->operate(oid, &op);
    if (ret < 0) {
      std::cerr << "fill: failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK)
      return 0;

    if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }

    assert(ret == zlog::CLS_ZLOG_READ_ONLY);
    return -EROFS;
  }
}

int Log::Trim(uint64_t position)
{
  for (;;) {
    librados::ObjectWriteOperation op;
    zlog::cls_zlog_trim(op, epoch_, position);

    std::string oid = position_to_oid(position);
    int ret = ioctx_->operate(oid, &op);
    if (ret < 0) {
      std::cerr << "trim: failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK)
      return 0;

    if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    }

    assert(0);
  }
}

int Log::AioRead(uint64_t position, AioCompletion *c,
    ceph::bufferlist *pbl)
{
  AioCompletionImpl *impl = (AioCompletionImpl*)c->pc;

  impl->log = this;
  impl->pbl = pbl;
  impl->position = position;
  impl->ioctx = ioctx_;
  impl->type = ZLOG_AIO_READ;

  impl->get(); // rados aio now has a reference
  impl->c = librados::Rados::aio_create_completion(impl, NULL, aio_safe_cb);
  assert(impl->c);

  librados::ObjectReadOperation op;
  zlog::cls_zlog_read(op, epoch_, position);

  std::string oid = position_to_oid(position);
  int ret = ioctx_->aio_operate(oid, impl->c, &op, &impl->bl);
  /*
   * Currently aio_operate never fails. If in the future that changes then we
   * need to make sure that references to impl and the rados completion are
   * cleaned up correctly.
   */
  assert(ret == 0);

  return ret;
}

int Log::Read(uint64_t epoch, uint64_t position, ceph::bufferlist& bl)
{
  for (;;) {
    librados::ObjectReadOperation op;
    zlog::cls_zlog_read(op, epoch, position);

    std::string oid = position_to_oid(position);
    int ret = ioctx_->operate(oid, &op, &bl);
    if (ret < 0) {
      std::cerr << "read failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK)
      return 0;
    else if (ret == zlog::CLS_ZLOG_NOT_WRITTEN)
      return -ENODEV;
    else if (ret == zlog::CLS_ZLOG_INVALIDATED)
      return -EFAULT;
    else if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    } else {
      std::cerr << "unknown reply";
      assert(0);
    }
  }
  assert(0);
}

int Log::Read(uint64_t position, ceph::bufferlist& bl)
{
  for (;;) {
    librados::ObjectReadOperation op;
    zlog::cls_zlog_read(op, epoch_, position);

    std::string oid = position_to_oid(position);
    int ret = ioctx_->operate(oid, &op, &bl);
    if (ret < 0) {
      std::cerr << "read failed ret " << ret << std::endl;
      return ret;
    }

    if (ret == zlog::CLS_ZLOG_OK)
      return 0;
    else if (ret == zlog::CLS_ZLOG_NOT_WRITTEN)
      return -ENODEV;
    else if (ret == zlog::CLS_ZLOG_INVALIDATED)
      return -EFAULT;
    else if (ret == zlog::CLS_ZLOG_STALE_EPOCH) {
      ret = RefreshProjection();
      if (ret)
        return ret;
      continue;
    } else {
      std::cerr << "unknown reply";
      assert(0);
    }
  }
  assert(0);
}

int Log::StreamHeader(ceph::bufferlist& bl, std::set<uint64_t>& stream_ids,
    size_t *header_size)
{
  if (bl.length() <= sizeof(uint32_t))
    return -EINVAL;

  const char *data = bl.c_str();

  uint32_t hdr_len = ntohl(*((uint32_t*)data));
  if (hdr_len > 512) // TODO something reasonable...?
    return -EINVAL;

  if ((sizeof(uint32_t) + hdr_len) > bl.length())
    return -EINVAL;

  zlog_proto::EntryHeader hdr;
  if (!hdr.ParseFromArray(data + sizeof(uint32_t), hdr_len))
    return -EINVAL;

  if (!hdr.IsInitialized())
    return -EINVAL;

  std::set<uint64_t> ids;
  for (int i = 0; i < hdr.stream_backpointers_size(); i++) {
    const zlog_proto::StreamBackPointer& ptr = hdr.stream_backpointers(i);
    ids.insert(ptr.id());
  }

  if (header_size)
    *header_size = sizeof(uint32_t) + hdr_len;

  stream_ids.swap(ids);

  return 0;
}

int Log::StreamMembership(std::set<uint64_t>& stream_ids, uint64_t position)
{
  ceph::bufferlist bl;
  int ret = Read(position, bl);
  if (ret)
    return ret;

  ret = StreamHeader(bl, stream_ids);

  return ret;
}

int Log::StreamMembership(uint64_t epoch, std::set<uint64_t>& stream_ids, uint64_t position)
{
  ceph::bufferlist bl;
  int ret = Read(epoch, position, bl);
  if (ret)
    return ret;

  ret = StreamHeader(bl, stream_ids);

  return ret;
}

struct Log::Stream::StreamImpl {
  uint64_t stream_id;
  Log *log;

  std::set<uint64_t> pos;
  std::set<uint64_t>::const_iterator prevpos;
  std::set<uint64_t>::const_iterator curpos;
};

std::vector<uint64_t> Log::Stream::History() const
{
  Log::Stream::StreamImpl *impl = this->impl;

  std::vector<uint64_t> ret;
  for (auto it = impl->pos.cbegin(); it != impl->pos.cend(); it++)
    ret.push_back(*it);
  return ret;
}

int Log::Stream::Append(ceph::bufferlist& data, uint64_t *pposition)
{
  Log::Stream::StreamImpl *impl = this->impl;
  std::set<uint64_t> stream_ids;
  stream_ids.insert(impl->stream_id);
  return impl->log->MultiAppend(data, stream_ids, pposition);
}

int Log::Stream::ReadNext(ceph::bufferlist& bl, uint64_t *pposition)
{
  Log::Stream::StreamImpl *impl = this->impl;

  if (impl->curpos == impl->pos.cend())
    return -EBADF;

  assert(!impl->pos.empty());

  uint64_t pos = *impl->curpos;

  ceph::bufferlist bl_out;
  int ret = impl->log->Read(pos, bl_out);
  if (ret)
    return ret;

  size_t header_size;
  std::set<uint64_t> stream_ids;
  ret = impl->log->StreamHeader(bl_out, stream_ids, &header_size);
  if (ret)
    return -EIO;

  assert(stream_ids.find(impl->stream_id) != stream_ids.end());

  // FIXME: how to create this view more efficiently?
  const char *data = bl_out.c_str();
  bl.append(data + header_size, bl_out.length() - header_size);

  if (pposition)
    *pposition = pos;

  impl->prevpos = impl->curpos;
  impl->curpos++;

  return 0;
}

int Log::Stream::Reset()
{
  Log::Stream::StreamImpl *impl = this->impl;
  impl->curpos = impl->pos.cbegin();
  return 0;
}

/*
 * Optimizations:
 *   - follow backpointers
 */
int Log::Stream::Sync()
{
  Log::Stream::StreamImpl *impl = this->impl;
  const uint64_t stream_id = impl->stream_id;

  /*
   * First contact the sequencer to find out what log position corresponds to
   * the tail of the stream, and then synchronize up to that position.
   */
  std::set<uint64_t> stream_ids;
  stream_ids.insert(stream_id);

  std::map<uint64_t, std::vector<uint64_t>> stream_backpointers;

  int ret = impl->log->CheckTail(stream_ids, stream_backpointers, NULL, false);
  if (ret)
    return ret;

  assert(stream_backpointers.size() == 1);
  const std::vector<uint64_t>& backpointers = stream_backpointers.at(stream_id);

  /*
   * The tail of the stream is the maximum log position handed out by the
   * sequencer for this particular stream. When the tail of a stream is
   * incremented the position is placed onto the list of backpointers. Thus
   * the max position in the backpointers set for a stream is the tail
   * position of the stream.
   *
   * If the current set of backpointers is empty, then the stream is empty and
   * there is nothing to do.
   */
  std::vector<uint64_t>::const_iterator bpit =
    std::max_element(backpointers.begin(), backpointers.end());
  if (bpit == backpointers.end()) {
    assert(impl->pos.empty());
    return 0;
  }
  uint64_t stream_tail = *bpit;

  /*
   * Avoid sync in log ranges that we've already processed by examining the
   * maximum stream position that we know about. If our local stream history
   * is empty then use the beginning of the log as the low point.
   *
   * we are going to search for stream entries between stream_tail (the last
   * position handed out by the sequencer for this stream), and the largest
   * stream position that we (the client) knows about. if we do not yet know
   * about any stream positions then we'll search down until position zero.
   */
  bool has_known = false;
  uint64_t known_stream_tail;
  if (!impl->pos.empty()) {
    auto it = impl->pos.crbegin();
    assert(it != impl->pos.crend());
    known_stream_tail = *it;
    has_known = true;
  }

  assert(!has_known || known_stream_tail <= stream_tail);
  if (has_known && known_stream_tail == stream_tail)
    return 0;

  std::set<uint64_t> updates;
  for (;;) {
    if (has_known && stream_tail == known_stream_tail)
      break;
    for (;;) {
      std::set<uint64_t> stream_ids;
      ret = impl->log->StreamMembership(stream_ids, stream_tail);
      if (ret == 0) {
        // save position if it belongs to this stream
        if (stream_ids.find(stream_id) != stream_ids.end())
          updates.insert(stream_tail);
        break;
      } else if (ret == -EINVAL) {
        // skip non-stream entries
        break;
      } else if (ret == -EFAULT) {
        // skip invalidated entries
        break;
      } else if (ret == -ENODEV) {
        // fill entries unwritten entries
        ret = impl->log->Fill(stream_tail);
        if (ret == 0) {
          // skip invalidated entries
          break;
        } else if (ret == -EROFS) {
          // retry
          continue;
        } else
          return ret;
      } else
        return ret;
    }
    if (!has_known && stream_tail == 0)
      break;
    stream_tail--;
  }

  if (updates.empty())
    return 0;

  impl->pos.insert(updates.begin(), updates.end());

  if (impl->curpos == impl->pos.cend()) {
    if (impl->prevpos == impl->pos.cend()) {
      impl->curpos = impl->pos.cbegin();
      assert(impl->curpos != impl->pos.cend());
    } else {
      impl->curpos = impl->prevpos;
      impl->curpos++;
      assert(impl->curpos != impl->pos.cend());
    }
  }

  return 0;
}

uint64_t Log::Stream::Id() const
{
  Log::Stream::StreamImpl *impl = this->impl;
  return impl->stream_id;
}

int Log::OpenStream(uint64_t stream_id, Stream& stream)
{
  assert(!stream.impl);

  Log::Stream::StreamImpl *impl = new Log::Stream::StreamImpl;
  impl->stream_id = stream_id;
  impl->log = this;

  /*
   * Previous position always points to the last position in the stream that
   * was successfully read, except on initialization when it points to the end
   * of the stream.
   */
  impl->prevpos = impl->pos.cend();

  /*
   * Current position always points to the element that is the next to be
   * read, or to the end of the stream if there are no [more] elements in the
   * stream to be read.
   */
  impl->curpos = impl->pos.cbegin();

  stream.impl = impl;

  return 0;
}

}

struct zlog_log_ctx {
  librados::IoCtx ioctx;
  zlog::SeqrClient *seqr;
  zlog::Log log;
};

extern "C" int zlog_destroy(zlog_log_t log)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  delete ctx->seqr;
  delete ctx;
  return 0;
}

/*
 *
 */
extern "C" int zlog_open(rados_ioctx_t ioctx, const char *name,
    const char *host, const char *port,
    zlog_log_t *log)
{
  zlog_log_ctx *ctx = new zlog_log_ctx;

  librados::IoCtx::from_rados_ioctx_t(ioctx, ctx->ioctx);

  ctx->seqr = new zlog::SeqrClient(host, port);
  ctx->seqr->Connect();

  int ret = zlog::Log::Open(ctx->ioctx, name,
      ctx->seqr, ctx->log);
  if (ret) {
    delete ctx->seqr;
    delete ctx;
    return ret;
  }

  *log = ctx;

  return 0;
}

/*
 *
 */
extern "C" int zlog_create(rados_ioctx_t ioctx, const char *name,
    int stripe_size, const char *host, const char *port,
    zlog_log_t *log)
{
  zlog_log_ctx *ctx = new zlog_log_ctx;

  librados::IoCtx::from_rados_ioctx_t(ioctx, ctx->ioctx);

  ctx->seqr = new zlog::SeqrClient(host, port);
  ctx->seqr->Connect();

  int ret = zlog::Log::Create(ctx->ioctx, name, stripe_size,
      ctx->seqr, ctx->log);
  if (ret) {
    delete ctx->seqr;
    delete ctx;
    return ret;
  }

  *log = ctx;

  return 0;
}

/*
 *
 */
extern "C" int zlog_open_or_create(rados_ioctx_t ioctx, const char *name,
    int stripe_size, const char *host, const char *port,
    zlog_log_t *log)
{
  zlog_log_ctx *ctx = new zlog_log_ctx;

  librados::IoCtx::from_rados_ioctx_t(ioctx, ctx->ioctx);

  ctx->seqr = new zlog::SeqrClient(host, port);
  ctx->seqr->Connect();

  int ret = zlog::Log::OpenOrCreate(ctx->ioctx, name, stripe_size,
      ctx->seqr, ctx->log);
  if (ret) {
    delete ctx->seqr;
    delete ctx;
    return ret;
  }

  *log = ctx;

  return 0;
}

/*
 *
 */
extern "C" int zlog_checktail(zlog_log_t log, uint64_t *pposition, int next)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  return ctx->log.CheckTail(pposition, next ? true : false);
}

extern "C" int zlog_checktail_batch(zlog_log_t log, uint64_t *positions,
    size_t count)
{
  if (count == 0)
    return 0;

  zlog_log_ctx *ctx = (zlog_log_ctx*)log;

  std::vector<uint64_t> result;
  int ret = ctx->log.CheckTail(result, count);
  if (ret)
    return ret;

  std::copy(result.begin(), result.end(), positions);

  return 0;
}

/*
 *
 */
extern "C" int zlog_append(zlog_log_t log, const void *data, size_t len,
    uint64_t *pposition)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  ceph::bufferlist bl;
  bl.append((char*)data, len);
  return ctx->log.Append(bl, pposition);
}

/*
 *
 */
extern "C" int zlog_multiappend(zlog_log_t log, const void *data,
    size_t data_len, const uint64_t *stream_ids, size_t stream_ids_len,
    uint64_t *pposition)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  if (stream_ids_len == 0)
    return -EINVAL;
  std::set<uint64_t> ids(stream_ids, stream_ids + stream_ids_len);
  ceph::bufferlist bl;
  bl.append((char*)data, data_len);
  return ctx->log.MultiAppend(bl, ids, pposition);
}

/*
 *
 */
extern "C" int zlog_read(zlog_log_t log, uint64_t position, void *data,
    size_t len)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  ceph::bufferlist bl;
  ceph::bufferptr bp = ceph::buffer::create_static(len, (char*)data);
  bl.push_back(bp);
  int ret = ctx->log.Read(position, bl);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    if (bl.c_str() != data)
      bl.copy(0, bl.length(), (char*)data);
    ret = bl.length();
  }
  return ret;
}

/*
 *
 */
extern "C" int zlog_fill(zlog_log_t log, uint64_t position)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  return ctx->log.Fill(position);
}

extern "C" int zlog_trim(zlog_log_t log, uint64_t position)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;
  return ctx->log.Trim(position);
}

struct zlog_stream_ctx {
  zlog::Log::Stream stream;
  zlog_log_ctx *log_ctx;
};

/*
 *
 */
extern "C" int zlog_stream_open(zlog_log_t log, uint64_t stream_id,
    zlog_stream_t *pstream)
{
  zlog_log_ctx *log_ctx = (zlog_log_ctx*)log;

  zlog_stream_ctx *stream_ctx = new zlog_stream_ctx;
  stream_ctx->log_ctx = log_ctx;

  int ret = log_ctx->log.OpenStream(stream_id, stream_ctx->stream);
  if (ret) {
    delete stream_ctx;
    return ret;
  }

  *pstream = stream_ctx;

  return 0;
}

/*
 *
 */
extern "C" int zlog_stream_append(zlog_stream_t stream, const void *data,
    size_t len, uint64_t *pposition)
{
  zlog_stream_ctx *ctx = (zlog_stream_ctx*)stream;
  ceph::bufferlist bl;
  bl.append((char*)data, len);
  return ctx->stream.Append(bl, pposition);
}

/*
 *
 */
extern "C" int zlog_stream_readnext(zlog_stream_t stream, void *data,
    size_t len, uint64_t *pposition)
{
  zlog_stream_ctx *ctx = (zlog_stream_ctx*)stream;

  ceph::bufferlist bl;
  // FIXME: below the buffer is added to avoid double copies. However, in
  // ReadNext we have to create a view of the data read to remove the header.
  // It isn't clear how to do that without the copies. As a fix for now we
  // just force the case where the bufferlist has to be resized.
#if 0
  ceph::bufferptr bp = ceph::buffer::create_static(len, (char*)data);
  bl.push_back(bp);
#endif

  int ret = ctx->stream.ReadNext(bl, pposition);

  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    if (bl.c_str() != data)
      bl.copy(0, bl.length(), (char*)data);
    ret = bl.length();
  }

  return ret;
}

/*
 *
 */
extern "C" int zlog_stream_reset(zlog_stream_t stream)
{
  zlog_stream_ctx *ctx = (zlog_stream_ctx*)stream;
  return ctx->stream.Reset();
}

/*
 *
 */
extern "C" int zlog_stream_sync(zlog_stream_t stream)
{
  zlog_stream_ctx *ctx = (zlog_stream_ctx*)stream;
  return ctx->stream.Sync();
}

/*
 *
 */
extern "C" uint64_t zlog_stream_id(zlog_stream_t stream)
{
  zlog_stream_ctx *ctx = (zlog_stream_ctx*)stream;
  return ctx->stream.Id();
}

extern "C" size_t zlog_stream_history(zlog_stream_t stream, uint64_t *pos, size_t len)
{
  zlog_stream_ctx *ctx = (zlog_stream_ctx*)stream;

  std::vector<uint64_t> history = ctx->stream.History();
  size_t size = history.size();
  if (pos && size <= len)
    std::copy(history.begin(), history.end(), pos);

  return size;
}

extern "C" int zlog_stream_membership(zlog_log_t log,
    uint64_t *stream_ids, size_t len, uint64_t position)
{
  zlog_log_ctx *ctx = (zlog_log_ctx*)log;

  std::set<uint64_t> ids;
  int ret = ctx->log.StreamMembership(ids, position);
  if (ret)
    return ret;

  size_t size = ids.size();
  if (size <= len) {
    std::vector<uint64_t> tmp;
    for (auto it = ids.begin(); it != ids.end(); it++)
      tmp.push_back(*it);
    std::copy(tmp.begin(), tmp.end(), stream_ids);
  }

  return size;
}
