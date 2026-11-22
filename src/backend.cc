// Redis backend: configuration, startup, routing (single, Sentinel, Cluster
// MOVED/ASK), and pipelined command execution.  Connections come from
// backend/redis_pool.h; transport and RESP from backend/stream.h and resp.h.
#include "backend.h"
#include "common.h"
#include "backend/redis_pool.h"
#include "backend/resp.h"
#include "backend/stream.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace rlc {
double durationSeconds(const std::string& text) {
  if (text == "0") return 0;
  double result = 0;
  size_t offset = 0;
  while (offset < text.size()) {
    size_t consumed;
    const double number = std::stod(text.substr(offset), &consumed);
    offset += consumed;
    size_t end = offset;
    while (end < text.size() && !(text[end] >= '0' && text[end] <= '9') && text[end] != '.') ++end;
    const auto unit = text.substr(offset, end-offset);
    double scale = 0;
    if (unit == "h") scale = 3600;
    else if (unit == "m") scale = 60;
    else if (unit == "s") scale = 1;
    else if (unit == "ms") scale = 1e-3;
    else if (unit == "us" || unit == "µs" || unit == "μs") scale = 1e-6;
    else if (unit == "ns") scale = 1e-9;
    else throw std::runtime_error("invalid duration: " + text);
    result += number * scale;
    offset = end;
  }
  return result;
}
namespace {
using backend::command;
using backend::encode;
using backend::parseAddress;
using backend::readReply;
using backend::RedisPool;
using backend::Reply;
using backend::split;
using backend::Stream;
using backend::Tls;
using Clock = std::chrono::steady_clock;
std::string lower(std::string text) {
  std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) { return std::tolower(c); });
  return text;
}
uint16_t slot(const std::string& original) {
  std::string key = original;
  const auto begin = key.find('{');
  if (begin != std::string::npos) {
    const auto end = key.find('}', begin+1);
    if (end != std::string::npos && end != begin+1) key = key.substr(begin+1, end-begin-1);
  }
  uint16_t crc = 0;
  for (unsigned char c : key) {
    crc ^= uint16_t(c) << 8;
    for (int i = 0; i < 8; ++i) crc = (crc << 1) ^ ((crc & 0x8000) ? 0x1021 : 0);
  }
  return crc % 16384;
}
}  // namespace

struct Redis::Impl {
  std::string prefix, kind, url, auth, sentinel_auth;
  bool unix_socket, close_readonly;
  double timeout;
  size_t pool_size;
  Tls tls;
  RedisPool pool;
  std::mutex slots_mutex;  // guards slots
  std::unordered_map<uint16_t, std::string> slots;
  // Health of the Redis component, as in Go with REDIS_HEALTH_CHECK_ACTIVE_CONNECTION
  // unset (the default): healthy once Redis answers PING at startup, and not
  // changed by later command failures (driver_impl.go NewClientImpl/poolTrace).
  std::atomic<bool> healthy{false};

  explicit Impl(bool per_second)
      : prefix(per_second ? "REDIS_PERSECOND_" : "REDIS_"),
        kind(lower(env((prefix+"TYPE").c_str(), "SINGLE"))),
        url(env((prefix+"URL").c_str(), per_second ? "/var/run/nutcracker/ratelimitpersecond.sock" : "/var/run/nutcracker/ratelimit.sock")),
        auth(env((prefix+"AUTH").c_str())), sentinel_auth(env((prefix+"SENTINEL_AUTH").c_str())),
        unix_socket(env((prefix+"SOCKET_TYPE").c_str(), "unix") == "unix"),
        close_readonly(envBool("REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR")),
        timeout(durationSeconds(env((prefix+"TIMEOUT").c_str(), "10s"))),
        pool_size(std::max<int64_t>(1, envInt((prefix+"POOL_SIZE").c_str(), 10))),
        tls(envBool((prefix+"TLS").c_str()), "REDIS_"),
        pool(pool_size, timeout, tls, auth, kind == "single" && unix_socket) {
    if (kind != "single" && kind != "cluster" && kind != "sentinel") throw std::runtime_error("Unrecognized redis type " + kind);
    if (kind == "sentinel" && split(url, ',').size() < 2)
      throw std::runtime_error("Expected master name and a list of urls for the sentinels, in the format: <redis master name>,<sentinel1>,...,<sentineln>");
    const auto behavior = lower(env((prefix+"POOL_ON_EMPTY_BEHAVIOR").c_str(), "WAIT"));
    if (behavior == "create" || behavior == "error") throw std::runtime_error(prefix+"POOL_ON_EMPTY_BEHAVIOR="+behavior+" is not supported; set WAIT");
    if (envInt((prefix+"CLUSTER_PIPELINE_PARALLELISM").c_str(), 1) < 0)
      throw std::runtime_error("redis cluster pipeline parallelism must be >= 0");
    // As in Go, startup does not report readiness until Redis answers PING.
    const auto start = Clock::now();
    double delay = durationSeconds(env("REDIS_STARTUP_INITIAL_INTERVAL", "1s"));
    const double max_delay = durationSeconds(env("REDIS_STARTUP_MAX_INTERVAL", "30s"));
    const double max_elapsed = durationSeconds(env("REDIS_STARTUP_MAX_ELAPSED_TIME", "0"));
    for (;;) {
      try {
        auto connection = pool.acquire(endpoint(""));
        if (command(*connection, {"PING"}).text != "PONG") throw std::runtime_error("unexpected PING response");
        connection.keep(); healthy = true; break;
      } catch (const std::exception& e) {
        const auto elapsed = std::chrono::duration<double>(Clock::now()-start).count();
        if (max_elapsed > 0 && elapsed >= max_elapsed) throw;
        std::cerr << "Redis connection unavailable; retrying: " << e.what() << '\n';
        std::this_thread::sleep_for(std::chrono::duration<double>(std::max(0.001, delay)));
        delay = std::min(max_delay, delay*2);
      }
    }
  }

  // The address for a key: the URL (single), the slot's known node (cluster),
  // or the current master reported by the first reachable Sentinel.
  std::string endpoint(const std::string& key) {
    if (kind == "single") return url;
    const auto addresses = split(url, ',');
    if (kind == "cluster") {
      std::lock_guard<std::mutex> lock(slots_mutex);
      const auto found = slots.find(slot(key));
      return found == slots.end() ? addresses.front() : found->second;
    }
    std::string last = "no reachable sentinel";
    for (size_t i = 1; i < addresses.size(); ++i) {
      try {
        auto address = parseAddress(addresses[i], false);
        Stream sentinel(address, timeout, tls);
        backend::authenticate(sentinel, address.auth.empty() ? sentinel_auth : address.auth);
        const auto reply = command(sentinel, {"SENTINEL", "get-master-addr-by-name", addresses.front()});
        if (reply.array.size() != 2) throw std::runtime_error("sentinel master not found");
        return "[" + reply.array[0].text + "]:" + reply.array[1].text;
      } catch (const std::exception& e) { last = e.what(); }
    }
    throw std::runtime_error(last);
  }

  // Sends commands for one target as a single pipeline.  Cluster MOVED/ASK
  // replies redirect the whole group (Redis did not execute it); any other
  // failure is returned to the caller without retrying.
  void group(const std::vector<const RedisCommand*>& commands) {
    std::string target = endpoint(commands.front()->key);
    bool asking = false;
    for (int attempt = 0; attempt < 8; ++attempt) {
      // If anything below throws, the Lease closes the connection.
      auto connection = pool.acquire(target);
      if (asking) command(*connection, {"ASKING"});
      std::string buffer;
      for (const auto* c : commands) buffer += encode(c->args);
      connection->write(buffer);
      std::vector<Reply> replies;
      for (size_t i = 0; i < commands.size(); ++i) replies.push_back(readReply(*connection));
      std::string error;
      for (const auto& reply : replies) if (reply.type == '-') { error = reply.text; break; }
      bool discard = error.rfind("READONLY", 0) == 0 && (close_readonly || kind == "sentinel");
      if (!discard) connection.keep();
      if (!error.empty()) {
        if (kind == "cluster" && (error.rfind("MOVED ", 0) == 0 || error.rfind("ASK ", 0) == 0)) {
          const auto fields = split(error, ' ');
          if (fields.size() == 3) {
            asking = fields[0] == "ASK";
            target = fields[2];
            if (!asking) { std::lock_guard<std::mutex> lock(slots_mutex); slots[static_cast<uint16_t>(std::stoi(fields[1]))] = target; }
            continue;
          }
        }
        throw std::runtime_error(error);
      }
      for (size_t i = 0; i < commands.size(); ++i) if (commands[i]->result) {
        if (replies[i].type == '_') *commands[i]->result = 0;
        else {
          if (!replies[i].text.empty() && replies[i].text[0] == '-')
            throw std::runtime_error("cannot decode negative Redis value into uint64");
          *commands[i]->result = std::stoull(replies[i].text);
        }
      }
      return;
    }
    throw std::runtime_error("too many Redis cluster redirects");
  }
  void pipeline(const std::vector<RedisCommand>& commands) {
    if (commands.empty()) return;
    std::vector<std::vector<const RedisCommand*>> groups;
    if (kind == "cluster") {
      std::unordered_map<std::string, size_t> indices;
      for (const auto& c : commands) {
        auto [it, inserted] = indices.emplace(c.key, groups.size());
        if (inserted) groups.emplace_back();
        groups[it->second].push_back(&c);
      }
    } else {
      groups.emplace_back(); for (const auto& c : commands) groups.back().push_back(&c);
    }
    for (const auto& g : groups) group(g);
  }
};
Redis::Redis(bool per_second) : impl_(std::make_unique<Impl>(per_second)) {}
Redis::~Redis() = default;
void Redis::pipeline(const std::vector<RedisCommand>& commands) { impl_->pipeline(commands); }
bool Redis::healthy() const { return impl_->healthy; }
}  // namespace rlc
