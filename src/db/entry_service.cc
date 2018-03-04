#include "db/entry_service.h"
#include <iostream>
#include "db/cruzdb.pb.h"

namespace cruzdb {

EntryService::EntryService(const Options& options,
    Statistics *statistics, zlog::Log *log) :
  stats_(statistics),
  log_(log),
  stop_(false),
  max_pos_(0),
  cache_size_(options.entry_cache_size)
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

void EntryService::entry_cache_gc()
{
  while (entry_cache_.size() > cache_size_) {
    entry_cache_.erase(entry_cache_.begin());
  }
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

        RecordTick(stats_, LOG_READS);
        RecordTick(stats_, BYTES_READ, data.size());

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
        entry_cache_gc();
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
    auto entry = NextEntry();
    if (entry) {
      if (entry->second.type == CacheEntry::EntryType::INTENTION)
        return entry->second.intention;
    } else {
      return boost::none;
    }
  }
}

EntryService::AfterImageIterator
EntryService::NewAfterImageIterator(uint64_t pos)
{
  return AfterImageIterator(this, pos);
}

EntryService::AfterImageIterator::AfterImageIterator(
    EntryService *entry_service, uint64_t pos) :
  Iterator(entry_service, pos)
{
}

boost::optional<std::pair<uint64_t,
  std::shared_ptr<cruzdb_proto::AfterImage>>>
EntryService::AfterImageIterator::Next()
{
  while (true) {
    auto entry = NextEntry();
    if (entry) {
      if (entry->second.type == CacheEntry::EntryType::AFTERIMAGE)
        return std::make_pair(entry->first, entry->second.after_image);
    } else {
      return boost::none;
    }
  }
}

boost::optional<EntryService::CacheEntry> EntryService::Read(uint64_t pos, bool fill)
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
      if (ret == -ENODATA) {
        CacheEntry cache_entry;
        cache_entry.type = CacheEntry::EntryType::FILLED;
        RecordTick(stats_, LOG_READS_FILLED);
        lk.lock();
        auto p = entry_cache_.emplace(pos, cache_entry);
        entry_cache_gc();
        return p.first->second;
      } else if (ret == -ENOENT) {
        RecordTick(stats_, LOG_READS_UNWRITTEN);
        if (fill) {
          ret = log_->Fill(pos);
          assert(ret == 0 || ret == -EROFS);
        }
        continue;
      }
      std::cerr << "read log failed " << ret << std::endl;
      assert(0);
      exit(1);
    }
    RecordTick(stats_, LOG_READS);
    break;
  }

  RecordTick(stats_, BYTES_READ, data.size());

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
  entry_cache_gc();
  return p.first->second;
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

uint64_t EntryService::CheckTail(bool update_max_pos)
{
  uint64_t pos;
  int ret = log_->CheckTail(&pos);
  if (ret) {
    std::cerr << "failed to check tail" << std::endl;
    assert(0);
    exit(1);
  }
  if (update_max_pos) {
    std::lock_guard<std::mutex> lk(lock_);
    max_pos_ = std::max(max_pos_, pos);
    for (auto& cond : tail_waiters_) {
      cond->notify_one();
    }
  }
  return pos;
}

void EntryService::Fill(uint64_t pos) const
{
  int delay = 1;
  while (true) {
    int ret = log_->Fill(pos);
    if (ret == 0)
      return;
    if (ret == -EROFS) {
      std::cerr << "filling read-only pos" << std::endl;
      assert(0);
      exit(1);
    }
    std::cerr << "failed to fill ret " << ret << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    delay = std::min(delay*10, 1000);
  }
}

uint64_t EntryService::Append(const std::string& data) const
{
  int delay = 1;
  while (true) {
    uint64_t pos;
    int ret = log_->Append(data, &pos);
    if (ret == 0) {
      RecordTick(stats_, LOG_APPENDS);
      RecordTick(stats_, BYTES_WRITTEN, data.size());
      return pos;
    }
    std::cerr << "failed to append ret " << ret << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    delay = std::min(delay*10, 1000);
  }
}

uint64_t EntryService::Append(cruzdb_proto::Intention& intention) const
{
  cruzdb_proto::LogEntry entry;
  entry.set_type(cruzdb_proto::LogEntry::INTENTION);
  entry.set_allocated_intention(&intention);
  assert(entry.IsInitialized());

  std::string blob;
  assert(entry.SerializeToString(&blob));
  entry.release_intention();

  return Append(blob);
}

uint64_t EntryService::Append(cruzdb_proto::AfterImage& after_image) const
{
  cruzdb_proto::LogEntry entry;
  entry.set_type(cruzdb_proto::LogEntry::AFTER_IMAGE);
  entry.set_allocated_after_image(&after_image);
  assert(entry.IsInitialized());

  std::string blob;
  assert(entry.SerializeToString(&blob));
  entry.release_after_image();

  return Append(blob);
}

uint64_t EntryService::Append(std::unique_ptr<Intention> intention)
{
  const auto blob = intention->Serialize();

  const auto pos = Append(blob);
  intention->SetPosition(pos);

  CacheEntry cache_entry;
  cache_entry.type = CacheEntry::EntryType::INTENTION;
  cache_entry.intention = std::move(intention);

  std::lock_guard<std::mutex> lk(lock_);
  entry_cache_.emplace(pos, cache_entry);
  entry_cache_gc();
  max_pos_ = std::max(max_pos_, pos);
  for (auto& cond : tail_waiters_) {
    cond->notify_one();
  }

  return pos;
}

std::shared_ptr<cruzdb_proto::AfterImage>
EntryService::ReadAfterImage(const uint64_t pos)
{
  std::unique_lock<std::mutex> lk(lock_);

  // check for afterimage in the cache
  auto it = entry_cache_.find(pos);
  if (it != entry_cache_.end()) {
    assert(it->second.type ==
        CacheEntry::EntryType::AFTERIMAGE);
    return it->second.after_image;
  }

  lk.unlock();

  int delay = 1;
  while (true) {
    std::string data;
    int ret = log_->Read(pos, &data);
    if (ret) {
      if (ret == -ENODATA) {
        RecordTick(stats_, LOG_READS_FILLED);
        // a filled entry will never be an afterimage
        std::cerr << "unexpected log entry" << std::endl;
        assert(0);
        exit(1);
      }
      // try again with increasing delay
      std::cerr << "failed to read pos " << pos << " ret " << ret << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(delay));
      delay = std::min(delay*10, 1000);
      continue;
    }

    RecordTick(stats_, LOG_READS);
    RecordTick(stats_, BYTES_READ, data.size());

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
      default:
        std::cerr << "unexpected log entry" << std::endl;
        assert(0);
        exit(1);
    }

    // insert entry into the cache
    lk.lock();
    auto p = entry_cache_.emplace(pos, cache_entry);
    entry_cache_gc();
    assert(p.first->second.type ==
        CacheEntry::EntryType::AFTERIMAGE);
    return p.first->second.after_image;
  }
}

std::vector<std::shared_ptr<Intention>>
EntryService::ReadIntentions(const std::vector<uint64_t>& positions)
{
  std::vector<std::shared_ptr<Intention>> intentions;
  std::vector<uint64_t> missing_positions;

  // check cache
  std::unique_lock<std::mutex> lk(lock_);
  for (const auto pos : positions) {
    auto it = entry_cache_.find(pos);
    if (it != entry_cache_.end()) {
      assert(it->second.type == CacheEntry::EntryType::INTENTION);
      intentions.emplace_back(it->second.intention);
    } else {
      missing_positions.emplace_back(pos);
    }
  }
  lk.unlock();

  // dispatch async reads. we'll want to throttle this later in some way to deal
  // with large requests for now the sizes seem reasonable.
  std::vector<zlog::AioCompletion*> ios;
  std::vector<std::string> blobs(missing_positions.size());
  for (size_t i = 0; i < missing_positions.size(); i++) {
    auto *c = zlog::Log::aio_create_completion();
    ios.emplace_back(c);
    int ret = log_->AioRead(missing_positions[i], c, &blobs[i]);
    assert(ret == 0);
  }

  // wait for completions, and optional dispatch any retries
  int delay = 1;
  while (true) {
    bool done = true;
    for (size_t i = 0; i < ios.size(); i++) {
      auto c = ios[i];
      if (c) {
        c->WaitForComplete();
        int ioret = c->ReturnValue();
        if (ioret == 0) {
          ios[i] = nullptr;
          RecordTick(stats_, LOG_READS);
          RecordTick(stats_, BYTES_READ, blobs[i].size());
        } else if (ioret == -ENODATA) {
          std::cerr << "unexpected log entry" << std::endl;
          assert(0);
          exit(1);
        } else {
          done = false;
        }
        delete c;
      }
    }

    if (done)
      break;

    std::cerr << "batch reads failed, delay retry" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    delay = std::min(delay*10, 1000);

    for (size_t i = 0; i < ios.size(); i++) {
      auto c = ios[i];
      if (c) { // c is not valid, just a non-null flag
        auto *c = zlog::Log::aio_create_completion();
        ios[i] = c;
        int ret = log_->AioRead(missing_positions[i], c, &blobs[i]);
        assert(ret == 0);
      }
    }
  }

  for (size_t i = 0; i < blobs.size(); i++) {
    cruzdb_proto::LogEntry entry;
    assert(entry.ParseFromString(blobs[i]));
    assert(entry.IsInitialized());
    assert(entry.type() == cruzdb_proto::LogEntry::INTENTION);

    // more efficient to use the interface in c++17 that doesn't construct the
    // element being inserted into the cache unless it is being inserted.
    auto intention = std::make_shared<Intention>(
        entry.intention(), missing_positions[i]);

    CacheEntry cache_entry;
    cache_entry.type = CacheEntry::EntryType::INTENTION;
    cache_entry.intention = intention;

    lk.lock();
    auto p = entry_cache_.emplace(missing_positions[i], cache_entry);
    entry_cache_gc();
    intentions.emplace_back(p.first->second.intention);
    lk.unlock();
  }

  assert(intentions.size() == positions.size());
  return std::move(intentions);
}

}
