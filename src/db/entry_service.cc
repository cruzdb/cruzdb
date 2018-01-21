#include "db/entry_service.h"
#include <iostream>
#include "db/cruzdb.pb.h"

namespace cruzdb {

EntryService::EntryService(zlog::Log *log, uint64_t pos,
    std::mutex *db_lock) :
  log_(log),
  pos_(pos),
  stop_(false),
  db_lock_(db_lock),
  log_reader_(std::thread(&EntryService::Run, this)),
  intention_reader_(std::thread(&EntryService::IntentionReader, this))
{
}

void EntryService::Stop()
{
  {
    std::lock_guard<std::mutex> l(lock_);
    stop_ = true;
  }

  for (auto& q : intention_queues_) {
    q->Stop();
  }

  pending_after_images_cond_.notify_one();
  log_reader_.join();
  intention_reader_.join();
}

void EntryCache::Insert(std::unique_ptr<Intention> intention)
{
  auto pos = intention->Position();
  std::lock_guard<std::mutex> lk(lock_);
  if (intentions_.size() > 10) {
    intentions_.erase(intentions_.begin());
  }
  intentions_.emplace(pos, std::move(intention));
}

// obvs this is silly to return a copy. the cache should be storing shared
// pointers or something like this.
boost::optional<Intention> EntryCache::FindIntention(uint64_t pos)
{
  std::lock_guard<std::mutex> lk(lock_);
  auto it = intentions_.find(pos);
  if (it != intentions_.end()) {
    return *(it->second);
  }
  return boost::none;
}

int EntryService::AppendIntention(std::unique_ptr<Intention> intention,
    uint64_t *pos)
{
  const auto blob = intention->Serialize();
  int ret = log_->Append(blob, pos);
  if (ret == 0) {
    intention->SetPosition(*pos);
    cache_.Insert(std::move(intention));
  }
  return ret;
}

void EntryService::IntentionReader()
{
  uint64_t pos;
  boost::optional<uint64_t> last_min_pos;

  while (true) {
    std::unique_lock<std::mutex> lk(lock_);

    if (stop_)
      return;

    if (intention_queues_.empty()) {
      last_min_pos = boost::none;
      continue;
    }

    // min pos requested by any queue
    auto min_pos = intention_queues_.front()->Position();
    for (auto& q : intention_queues_) {
      min_pos = std::min(min_pos, q->Position());
    }

    lk.unlock();

    if (!last_min_pos) {
      last_min_pos = min_pos;
      pos = min_pos;
    }

    if (min_pos < *last_min_pos) {
      last_min_pos = boost::none;
      continue;
    }

    last_min_pos = min_pos;

    // the cache may know that this pos is not an intention, and that additional
    // slots in the log can be skipped over...
    auto intention = cache_.FindIntention(pos);
    if (intention) {
      lk.lock();
      for (auto& q : intention_queues_) {
        if (pos >= q->Position()) {
          q->Push(*intention);
        }
      }
      lk.unlock();

      pos++;
      continue;
    }

    // obvs this should be populating the cache too..
    std::string data;
    int ret = log_->Read(pos, &data);
    if (ret) {
      if (ret == -ENOENT) {
        continue;
      }
      assert(0);
    }

    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());

    switch (entry.type()) {
      case cruzdb_proto::LogEntry::INTENTION:
        lk.lock();
        for (auto& q : intention_queues_) {
          if (pos >= q->Position()) {
            q->Push(Intention(entry.intention(), pos));
          }
        }
        lk.unlock();
        break;

      case cruzdb_proto::LogEntry::AFTER_IMAGE:
        break;

      default:
        assert(0);
        exit(1);
    }

    pos++;
  }
}

void EntryService::Run()
{
  while (true) {
    {
      std::lock_guard<std::mutex> l(lock_);
      if (stop_)
        return;
    }

    std::string data;

    // need to fill log positions. this is because it is important that any
    // after image that is currently the first occurence following its
    // intention, remains that way.
    int ret = log_->Read(pos_, &data);
    if (ret) {
      // TODO: be smart about reading. we shouldn't wait one second, and we
      // should sometimes fill holes. the current infrastructure won't generate
      // holes in testing, so this will work for now.
      if (ret == -ENOENT) {
        /*
         * TODO: currently we can run into a soft lockup where the log reader is
         * spinning on a position that hasn't been written. that's weird, since
         * we aren't observing any failed clients or sequencer or anything, so
         * every position should be written.
         *
         * what might be happening.. is that there is a hole, but the entity
         * assigned to fill that hole is waiting on something a bit further
         * ahead in the log, so no progress is being made...
         *
         * lets get a confirmation about the state here so we can record this
         * case. it would be an interesting case.
         *
         * do timeout waits so we can see with print statements...
         */
        continue;
      }
      assert(0);
    }

    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());

    // TODO: look into using Arena allocation in protobufs, or moving to
    // flatbuffers. we basically want to avoid all the copying here, by doing
    // something like pushing a pointer onto these lists, or using move
    // semantics.
    switch (entry.type()) {
      case cruzdb_proto::LogEntry::INTENTION:
        break;

      case cruzdb_proto::LogEntry::AFTER_IMAGE:
        {
          std::lock_guard<std::mutex> lk(*db_lock_);
          pending_after_images_.emplace_back(pos_, entry.after_image());
        }
        pending_after_images_cond_.notify_one();
        break;

      default:
        assert(0);
        exit(1);
    }

    pos_++;
  }
}

EntryService::IntentionQueue *EntryService::NewIntentionQueue(uint64_t pos)
{
  auto queue = std::make_unique<IntentionQueue>(pos);
  auto ret = queue.get();
  std::lock_guard<std::mutex> lk(lock_);
  intention_queues_.emplace_back(std::move(queue));
  return ret;
}

EntryService::IntentionQueue::IntentionQueue(uint64_t pos) :
  pos_(pos),
  stop_(false)
{
}

void EntryService::IntentionQueue::Stop()
{
  {
    std::lock_guard<std::mutex> lk(lock_);
    stop_ = true;
  }
  cond_.notify_all();
}

boost::optional<Intention> EntryService::IntentionQueue::Wait()
{
  std::unique_lock<std::mutex> lk(lock_);
  cond_.wait(lk, [this] { return !q_.empty() || stop_; });
  if (stop_) {
    return boost::none;
  }
  assert(!q_.empty());
  auto i = q_.front();
  q_.pop();
  return i;
}

uint64_t EntryService::IntentionQueue::Position() const
{
  std::lock_guard<std::mutex> lk(lock_);
  return pos_;
}

void EntryService::IntentionQueue::Push(Intention intention)
{
  std::lock_guard<std::mutex> lk(lock_);
  assert(pos_ <= intention.Position());
  pos_ = intention.Position() + 1;
  q_.emplace(intention);
  cond_.notify_one();
}

std::list<Intention>
EntryService::ReadIntentions(std::vector<uint64_t> addrs)
{
  std::list<Intention> intentions;
  assert(!addrs.empty());
  for (auto pos : addrs) {
    std::string data;
    int ret = log_->Read(pos, &data);
    assert(ret == 0);
    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());
    assert(entry.type() == cruzdb_proto::LogEntry::INTENTION);
    intentions.emplace_back(Intention(entry.intention(), pos));
  }
  return intentions;
}

}
