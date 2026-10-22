#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace rlc {
struct RedisCommand {
  std::string key;
  std::vector<std::string> args;
  uint64_t* result = nullptr;
};
class Redis {
 public:
  explicit Redis(bool per_second = false);
  ~Redis();
  void pipeline(const std::vector<RedisCommand>& commands);
  bool healthy() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
struct MemcacheUpdate {
  std::string key;
  uint64_t hits;
  int64_t expiration;
  bool negative;
};
class Memcache {
 public:
  Memcache();
  ~Memcache();
  std::unordered_map<std::string, uint64_t> get(const std::vector<std::string>& keys);
  void update(std::vector<MemcacheUpdate> updates);
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
double durationSeconds(const std::string& text);
}
