#include "redis_pool.h"
#include "resp.h"
#include <stdexcept>
#include <utility>

namespace rlc::backend {

RedisPool::RedisPool(size_t capacity, double timeout, const Tls& tls, std::string auth, bool unix_socket)
    : capacity_(capacity), timeout_(timeout), tls_(tls), auth_(std::move(auth)), unix_socket_(unix_socket) {}

std::unique_ptr<Stream> RedisPool::connect(const std::string& target) {
  const auto address = parseAddress(target, unix_socket_);
  auto stream = std::make_unique<Stream>(address, timeout_, tls_);
  authenticate(*stream, address.auth.empty() ? auth_ : address.auth);
  if (address.db) command(*stream, {"SELECT", std::to_string(address.db)});
  return stream;
}

// Takes one capacity slot (busy_) and returns an exclusively owned connection.
// Idle connections are checked outside the mutex (this thread owns them once
// popped); a closed or broken one is dropped and the same slot moves on to the
// next idle connection or a new one.
RedisPool::Lease RedisPool::acquire(const std::string& target) {
  std::unique_lock<std::mutex> lock(mutex_);
  available_.wait(lock, [&] { return busy_ < capacity_; });
  ++busy_;
  bool dropped_closed = false;
  for (;;) {
    auto& connections = idle_[target];
    if (connections.empty()) break;
    auto stream = std::move(connections.back()); connections.pop_back();
    lock.unlock();
    const auto state = stream->idleState();
    if (state == Stream::Idle::Usable) return Lease(*this, target, std::move(stream));
    dropped_closed = dropped_closed || state == Stream::Idle::Closed;
    stream.reset();
    lock.lock();
  }
  lock.unlock();
  try { return Lease(*this, target, connect(target)); }
  catch (...) {
    release(target, nullptr);
    // Go reports a request that draws a pooled connection the server closed
    // as this EOF; report the same when that connection cannot be replaced.
    if (dropped_closed) throw std::runtime_error(kConnEofError);
    throw;
  }
}

void RedisPool::release(const std::string& target, std::unique_ptr<Stream> stream) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stream) idle_[target].push_back(std::move(stream));
  --busy_; available_.notify_one();
}

RedisPool::Lease::Lease(RedisPool& pool, std::string target, std::unique_ptr<Stream> stream)
    : pool_(&pool), target_(std::move(target)), stream_(std::move(stream)) {}

RedisPool::Lease::Lease(Lease&& other) noexcept
    : pool_(std::exchange(other.pool_, nullptr)), target_(std::move(other.target_)),
      stream_(std::move(other.stream_)) {}

RedisPool::Lease::~Lease() {
  if (pool_) pool_->release(target_, nullptr);
}

void RedisPool::Lease::keep() {
  if (pool_) std::exchange(pool_, nullptr)->release(target_, std::move(stream_));
}

}  // namespace rlc::backend
