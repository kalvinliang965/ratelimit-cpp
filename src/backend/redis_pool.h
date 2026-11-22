// Redis connections with exclusive checkout.
//
// At most `capacity` (REDIS_POOL_SIZE) connections are checked out at once,
// across all targets; acquire() waits for a free slot.  Idle connections are
// kept per target address and checked (Stream::idleState) before reuse; a
// closed or broken one is dropped and replaced.  Commands are never retried.
#pragma once
#include "stream.h"
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace rlc::backend {

class RedisPool {
 public:
  // New connections authenticate with `auth` unless the target address has its
  // own credentials.  unix_socket: targets are Unix socket paths.
  RedisPool(size_t capacity, double timeout, const Tls& tls, std::string auth, bool unix_socket);
  RedisPool(const RedisPool&) = delete;
  RedisPool& operator=(const RedisPool&) = delete;

  // One checked-out connection.  keep() returns it to the idle list after a
  // complete exchange; destroying a Lease that was not kept closes the
  // connection (its protocol state is unknown, e.g. after an exception) and
  // frees the slot.  Either way the slot is freed exactly once.
  class Lease {
   public:
    Lease(Lease&& other) noexcept;
    Lease& operator=(Lease&&) = delete;
    ~Lease();
    Stream& operator*() const { return *stream_; }
    Stream* operator->() const { return stream_.get(); }
    void keep();

   private:
    friend class RedisPool;
    Lease(RedisPool& pool, std::string target, std::unique_ptr<Stream> stream);
    RedisPool* pool_;  // null once kept or moved from
    std::string target_;
    std::unique_ptr<Stream> stream_;
  };

  Lease acquire(const std::string& target);

 private:
  std::unique_ptr<Stream> connect(const std::string& target);
  void release(const std::string& target, std::unique_ptr<Stream> stream);

  const size_t capacity_;
  const double timeout_;
  const Tls& tls_;
  const std::string auth_;
  const bool unix_socket_;

  std::mutex mutex_;  // guards busy_ and idle_
  std::condition_variable available_;
  size_t busy_ = 0;   // checked-out connections, including ones being dialed
  std::unordered_map<std::string, std::vector<std::unique_ptr<Stream>>> idle_;
};

}  // namespace rlc::backend
