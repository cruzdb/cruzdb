#include "db/entry_service.h"
#include <iostream>
#include "db/cruzdb.pb.h"

namespace cruzdb {

EntryService::EntryService(zlog::Log *log) :
  log_(log),
  stop_(false),
  max_pos_(0)
{
}

void EntryService::Start(uint64_t pos)
{
  pos_ = pos;
  io_thread_ = std::thread(&EntryService::IOEntry, this);
}

void EntryService::Stop()
{
  {
    std::lock_guard<std::mutex> l(lock_);
    stop_ = true;
  }

  ai_matcher.shutdown();

  {
    std::lock_guard<std::mutex> l(lock_);
    for (auto& cond : tail_waiters_) {
      cond->notify_one();
    }
  }

  io_thread_.join();
}

uint64_t EntryService::Append(std::unique_ptr<Intention> intention)
{
  uint64_t pos;
  const auto blob = intention->Serialize();
  int ret = log_->Append(blob, &pos);
  if (ret) {
    std::cerr << "log append failed" << std::endl;
    assert(0);
    exit(1);
  }

  intention->SetPosition(pos);

  CacheEntry cache_entry;
  cache_entry.type = CacheEntry::EntryType::INTENTION;
  cache_entry.intention = std::move(intention);

  std::lock_guard<std::mutex> lk(lock_);

  entry_cache_.emplace(pos, cache_entry);
  max_pos_ = std::max(max_pos_, pos);
  for (auto& cond : tail_waiters_) {
    cond->notify_one();
  }

  return pos;
}

void EntryService::IOEntry()
{
  uint64_t next = pos_;

  while (true) {
    {
      std::lock_guard<std::mutex> lk(lock_);
      if (stop_)
        break;
    }
    auto tail = CheckTail();
    assert(next <= tail);
    if (next == tail) {
      std::this_thread::sleep_for(std::chrono::microseconds(1000));
      continue;
    }
    while (next < tail) {
      std::unique_lock<std::mutex> lk(lock_);
      auto it = entry_cache_.find(next);
      if (it == entry_cache_.end()) {
        lk.unlock();
        std::string data;
        int ret = log_->Read(next, &data);
        if (ret) {
          if (ret == -ENOENT) {
            // we aren't going to spin on the tail, but we haven't yet implemented
            // a fill policy, and really we shouldn't have holes in our
            // single-node setup, so we just spin on the hole for now...
            continue;
          }
          assert(0);
        }

        cruzdb_proto::LogEntry entry;
        assert(entry.ParseFromString(data));
        assert(entry.IsInitialized());

        CacheEntry cache_entry;

        switch (entry.type()) {
          case cruzdb_proto::LogEntry::AFTER_IMAGE:
            cache_entry.type = CacheEntry::EntryType::AFTERIMAGE;
            cache_entry.after_image =
              std::make_shared<cruzdb_proto::AfterImage>(
                  std::move(entry.after_image()));
            ai_matcher.push(entry.after_image(), next);
            break;

          case cruzdb_proto::LogEntry::INTENTION:
            cache_entry.type = CacheEntry::EntryType::INTENTION;
            cache_entry.intention = std::make_shared<Intention>(
                entry.intention(), next);
            break;

          default:
            assert(0);
            exit(1);
        }

        lk.lock();
        entry_cache_.emplace(next, cache_entry);
        max_pos_ = std::max(max_pos_, next);
        for (auto& cond : tail_waiters_) {
          cond->notify_one();
        }
        lk.unlock();
      }

      next++;
    }
  }
}

EntryService::Iterator::Iterator(
    EntryService *entry_service, uint64_t pos) :
  pos_(pos),
  stop_(false),
  entry_service_(entry_service)
{
}

boost::optional<EntryService::CacheEntry> EntryService::Iterator::Next()
{
  auto entry = entry_service_->Read(pos_);
  pos_++;
  return entry;
}

EntryService::IntentionIterator
EntryService::NewIntentionIterator(uint64_t pos)
{
  return IntentionIterator(this, pos);
}

EntryService::IntentionIterator::IntentionIterator(
    EntryService *entry_service, uint64_t pos) :
  Iterator(entry_service, pos)
{
}

boost::optional<std::shared_ptr<Intention>>
EntryService::IntentionIterator::Next()
{
  while (true) {
    auto entry = EntryService::Iterator::Next();
    if (entry) {
      if (entry->type == CacheEntry::EntryType::INTENTION)
        return entry->intention;
    } else {
      return boost::none;
    }
  }
}

boost::optional<EntryService::CacheEntry> EntryService::Read(uint64_t pos)
{
  std::unique_lock<std::mutex> lk(lock_);

  // check cache for target position
  auto it = entry_cache_.find(pos);
  if (it != entry_cache_.end()) {
    return it->second;
  }

  // if position is larger than any added to the cache so far, wait to be
  // notified when the position has been read by the log scanner.
  if (pos > max_pos_) {
    std::condition_variable cond;
    tail_waiters_.emplace_back(&cond);
    auto cit = tail_waiters_.end();
    cit--;
    cond.wait(lk, [&] { return pos <= max_pos_ || stop_; });
    tail_waiters_.erase(cit);
    if (stop_)
      return boost::none;
  }

  lk.unlock();

  // mm... still we see an occasional hole that should be temporary in the
  // current setups. this tight loop is bad. we'll be moving to a different way
  // to do io retries and filling later...
  std::string data;
  while (true) {
    int ret = log_->Read(pos, &data);
    if (ret) {
      if (ret == -ENOENT) {
        continue;
      }
      std::cerr << "read log failed " << ret << std::endl;
      assert(0);
      exit(1);
    }
    break;
  }

  cruzdb_proto::LogEntry entry;
  assert(entry.ParseFromString(data));
  assert(entry.IsInitialized());

  CacheEntry cache_entry;

  switch (entry.type()) {
    case cruzdb_proto::LogEntry::AFTER_IMAGE:
      cache_entry.type = CacheEntry::EntryType::AFTERIMAGE;
      cache_entry.after_image =
        std::make_shared<cruzdb_proto::AfterImage>(
            std::move(entry.after_image()));
      break;

    case cruzdb_proto::LogEntry::INTENTION:
      cache_entry.type = CacheEntry::EntryType::INTENTION;
      cache_entry.intention = std::make_shared<Intention>(
          entry.intention(), pos);
      break;

    default:
      assert(0);
      exit(1);
  }

  lk.lock();

  auto p = entry_cache_.emplace(pos, cache_entry);
  return p.first->second;
}

// this api expects that the addresses being read fall below the actual tail of
// the log. not that it is a problem, but it might influence the policy for hole
// filling. or it might be an error here, since why would we even have a
// reference to such an address?
std::list<std::shared_ptr<Intention>>
EntryService::ReadIntentions(std::vector<uint64_t> addrs)
{
  assert(!addrs.empty());
  std::list<std::shared_ptr<Intention>> intentions;
  std::vector<uint64_t> missing;

  std::unique_lock<std::mutex> lk(lock_);
  for (auto pos : addrs) {
    auto it = entry_cache_.find(pos);
    if (it != entry_cache_.end()) {
      assert(it->second.type == CacheEntry::EntryType::INTENTION);
      intentions.push_back(it->second.intention);
    } else {
      missing.push_back(pos);
    }
  }

  lk.unlock();
  for (auto pos : missing) {
    // TODO: dump positions into an i/o queue...
    std::string data;
    int ret = log_->Read(pos, &data);
    assert(ret == 0);

    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(data));
    assert(entry.IsInitialized());
    assert(entry.type() == cruzdb_proto::LogEntry::INTENTION);

    // this is rather inefficient. below perhaps choose
    // insert/insert_or_assign, etc... c++17
    auto intention = std::make_shared<Intention>(
        entry.intention(), pos);

    CacheEntry cache_entry;
    cache_entry.type = CacheEntry::EntryType::INTENTION;
    cache_entry.intention = intention;

    lk.lock();
    auto p = entry_cache_.emplace(pos, cache_entry);
    lk.unlock();

    intentions.emplace_back(p.first->second.intention);
  }

  return intentions;
}

EntryService::PrimaryAfterImageMatcher::PrimaryAfterImageMatcher() :
  shutdown_(false),
  matched_watermark_(0)
{
}

void EntryService::PrimaryAfterImageMatcher::watch(
    std::vector<SharedNodeRef> delta,
    std::unique_ptr<PersistentTree> intention)
{
  std::lock_guard<std::mutex> lk(lock_);

  const auto ipos = intention->Intention();

  auto it = afterimages_.find(ipos);
  if (it == afterimages_.end()) {
    afterimages_.emplace(ipos,
        PrimaryAfterImage{boost::none,
        std::move(intention),
        std::move(delta)});
  } else {
    assert(it->second.pos);
    assert(!it->second.tree);
    intention->SetAfterImage(*it->second.pos);
    it->second.pos = boost::none;
    matched_.emplace_back(std::make_pair(std::move(delta),
        std::move(intention)));
    cond_.notify_one();
  }

  gc();
}

void EntryService::PrimaryAfterImageMatcher::push(
    const cruzdb_proto::AfterImage& ai, uint64_t pos)
{
  std::lock_guard<std::mutex> lk(lock_);

  const auto ipos = ai.intention();
  if (ipos <= matched_watermark_) {
    return;
  }

  auto it = afterimages_.find(ipos);
  if (it == afterimages_.end()) {
    afterimages_.emplace(ipos, PrimaryAfterImage{pos, nullptr, {}});
  } else if (!it->second.pos && it->second.tree) {
    assert(it->second.tree->Intention() == ipos);
    it->second.tree->SetAfterImage(pos);
    matched_.emplace_back(std::make_pair(std::move(it->second.delta),
        std::move(it->second.tree)));
    cond_.notify_one();
  }

  gc();
}

std::pair<std::vector<SharedNodeRef>,
  std::unique_ptr<PersistentTree>>
EntryService::PrimaryAfterImageMatcher::match()
{
  std::unique_lock<std::mutex> lk(lock_);

  cond_.wait(lk, [&] { return !matched_.empty() || shutdown_; });

  if (shutdown_) {
    return std::make_pair(std::vector<SharedNodeRef>(), nullptr);
  }

  assert(!matched_.empty());

  auto tree = std::move(matched_.front());
  matched_.pop_front();

  return std::move(tree);
}

void EntryService::PrimaryAfterImageMatcher::shutdown()
{
  std::lock_guard<std::mutex> l(lock_);
  shutdown_ = true;
  cond_.notify_one();
}

void EntryService::PrimaryAfterImageMatcher::gc()
{
  auto it = afterimages_.begin();
  while (it != afterimages_.end()) {
    auto ipos = it->first;
    assert(matched_watermark_ < ipos);
    auto& pai = it->second;
    if (!pai.pos && !pai.tree) {
      matched_watermark_ = ipos;
      it = afterimages_.erase(it);
    } else {
      // as long as the watermark is positioned such that no unmatched intention
      // less than the water is in the index, then gc could move forward and
      // continue removing matched entries.
      break;
    }
  }
}

uint64_t EntryService::CheckTail()
{
  uint64_t pos;
  int ret = log_->CheckTail(&pos);
  if (ret) {
    std::cerr << "failed to check tail" << std::endl;
    assert(0);
    exit(1);
  }
  return pos;
}

uint64_t EntryService::Append(cruzdb_proto::Intention& intention)
{
  cruzdb_proto::LogEntry entry;
  entry.set_type(cruzdb_proto::LogEntry::INTENTION);
  entry.set_allocated_intention(&intention);
  assert(entry.IsInitialized());

  std::string blob;
  assert(entry.SerializeToString(&blob));
  entry.release_intention();

  uint64_t pos;
  int ret = log_->Append(blob, &pos);
  if (ret) {
    std::cerr << "failed to append intention" << std::endl;
    assert(0);
    exit(1);
  }
  return pos;
}

uint64_t EntryService::Append(cruzdb_proto::AfterImage& after_image)
{
  cruzdb_proto::LogEntry entry;
  entry.set_type(cruzdb_proto::LogEntry::AFTER_IMAGE);
  entry.set_allocated_after_image(&after_image);
  assert(entry.IsInitialized());

  std::string blob;
  assert(entry.SerializeToString(&blob));
  entry.release_after_image();

  uint64_t pos;
  int ret = log_->Append(blob, &pos);
  if (ret) {
    std::cerr << "failed to append after image" << std::endl;
    assert(0);
    exit(1);
  }
  return pos;
}

}
