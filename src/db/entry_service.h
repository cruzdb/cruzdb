#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <boost/optional.hpp>
#include <zlog/log.h>
#include "db/intention.h"

namespace cruzdb {

class EntryService {
 public:
  EntryService(zlog::Log *log, uint64_t pos,
      std::mutex *db_lock);

  void Stop();

 public:
  class IntentionQueue {
   public:
    explicit IntentionQueue(uint64_t pos);

    void Stop();
    boost::optional<SafeIntention> Wait();

   private:
    friend class EntryService;

    uint64_t Position() const;
    void Push(SafeIntention intention);

    mutable std::mutex lock_;
    uint64_t pos_;
    bool stop_;
    std::queue<SafeIntention> q_;
    std::condition_variable cond_;
  };

  // create a non-owning intention queue. the queue can be used to access all
  // intentions in their log order, starting at the provided position. the queue
  // will remain valid until the entry service is destroyed.
  IntentionQueue *NewIntentionQueue(uint64_t pos);

  std::list<std::pair<uint64_t, cruzdb_proto::AfterImage>> pending_after_images_;
  std::condition_variable pending_after_images_cond_;

 private:
  void Run();
  void IntentionReader();

  zlog::Log *log_;
  uint64_t pos_;
  bool stop_;
  std::mutex lock_;

  std::mutex *db_lock_;

  std::list<std::unique_ptr<IntentionQueue>> intention_queues_;

  std::thread log_reader_;
  std::thread intention_reader_;
};

}
