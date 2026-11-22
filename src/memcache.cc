// Memcached backend (BACKEND_TYPE=memcache): reads are synchronous; increments
// run on one worker thread, as in Go's asynchronous memcache updates.
#include "backend.h"
#include "common.h"
#include "backend/stream.h"
#include <arpa/nameser.h>
#include <resolv.h>
#include <algorithm>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <ctime>
#include <deque>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace rlc {
namespace {
using backend::parseAddress;
using backend::split;
using backend::Stream;
using backend::Tls;
using Clock = std::chrono::steady_clock;

std::vector<std::string> resolveSrv(const std::string& name) {
  if (name.empty() || name.front() != '_' || name.find("._") == std::string::npos)
    throw std::runtime_error("could not parse " + name + " to SRV parts");
  unsigned char answer[65536];
  int len = res_query(name.c_str(), ns_c_in, ns_t_srv, answer, sizeof answer);
  if (len < 0) throw std::runtime_error("Unable to fetch servers from SRV");
  ns_msg message;
  if (ns_initparse(answer, len, &message) < 0) throw std::runtime_error("Unable to parse SRV reply");
  std::vector<std::string> result;
  for (int i = 0; i < ns_msg_count(message, ns_s_an); ++i) {
    ns_rr rr;
    if (ns_parserr(&message, ns_s_an, i, &rr) < 0 || ns_rr_type(rr) != ns_t_srv || ns_rr_rdlen(rr) < 7) continue;
    const auto* data = ns_rr_rdata(rr);
    char host[NS_MAXDNAME];
    if (dn_expand(answer, answer+len, data+6, host, sizeof host) < 0) continue;
    result.push_back(std::string(host)+".:"+std::to_string(ns_get16(data+4)));
  }
  if (result.empty()) throw std::runtime_error("no servers returned from SRV");
  std::sort(result.begin(), result.end()); return result;
}
// Go's gomemcache ServerList uses crc32.ChecksumIEEE(key) to choose a shard.
uint32_t crc32(const std::string& key) {
  uint32_t value = 0xffffffff;
  for (unsigned char c : key) {
    value ^= c;
    for (int i = 0; i < 8; ++i) value = (value >> 1) ^ ((value & 1) ? 0xedb88320 : 0);
  }
  return ~value;
}
bool validMemcacheKey(const std::string& key) {
  if (key.empty() || key.size() > 250) return false;
  for (unsigned char c : key) if (c <= ' ' || c == 0x7f) return false;
  return true;
}
}  // namespace

struct Memcache::Impl {
  std::vector<std::string> servers;
  std::string srv = env("MEMCACHE_SRV");
  double refresh = durationSeconds(env("MEMCACHE_SRV_REFRESH", "0"));
  Clock::time_point last_refresh = Clock::now();
  Tls tls{envBool("MEMCACHE_TLS"), "MEMCACHE_"};
  std::mutex io_mutex, queue_mutex;
  std::condition_variable pending;
  std::deque<std::vector<MemcacheUpdate>> queue;
  bool stopping = false;
  std::thread worker;
  std::unordered_map<std::string, std::unique_ptr<Stream>> connections;
  Impl() {
    const auto hosts = env("MEMCACHE_HOST_PORT");
    if (!hosts.empty() && !srv.empty()) throw std::runtime_error("Both MEMCACHE_HOST_PORT and MEMCACHE_SRV are set");
    if (!srv.empty()) servers = resolveSrv(srv);
    else if (!hosts.empty()) servers = split(hosts, ',');
    worker = std::thread([this] {
      for (;;) {
        std::vector<MemcacheUpdate> updates;
        {
          std::unique_lock<std::mutex> lock(queue_mutex);
          pending.wait(lock, [&] { return stopping || !queue.empty(); });
          if (stopping && queue.empty()) return;
          updates = std::move(queue.front()); queue.pop_front();
        }
        for (const auto& update : updates) {
          try { apply(update); }
          catch (const std::exception& e) { std::cerr << "Memcached update: " << e.what() << '\n'; }
        }
      }
    });
  }
  ~Impl() {
    { std::lock_guard<std::mutex> lock(queue_mutex); stopping = true; }
    pending.notify_all(); worker.join();
  }
  std::string server(const std::string& key) {
    if (!validMemcacheKey(key)) throw std::runtime_error("malformed: key is too long or contains invalid characters");
    if (!srv.empty() && refresh > 0 && std::chrono::duration<double>(Clock::now()-last_refresh).count() >= refresh) {
      try { servers = resolveSrv(srv); } catch (const std::exception& e) { std::cerr << "Memcached SRV refresh: " << e.what() << '\n'; }
      last_refresh = Clock::now();
    }
    if (servers.empty()) throw std::runtime_error("memcache: no servers configured or available");
    return servers[crc32(key) % servers.size()];
  }
  Stream& connect(const std::string& target) {
    auto& stream = connections[target];
    if (!stream) stream = std::make_unique<Stream>(parseAddress(target, !target.empty() && target[0] == '/'), 0.5, tls);
    return *stream;
  }
  void apply(const MemcacheUpdate& update) {
    std::lock_guard<std::mutex> lock(io_mutex);
    const auto target = server(update.key);
    try {
      auto& stream = connect(target);
      const auto increment = (update.negative ? "decr " : "incr ") + update.key + " " + std::to_string(update.hits) + "\r\n";
      stream.write(increment);
      auto line = stream.line();
      if (line == "NOT_FOUND" && !update.negative) {
        const auto value = std::to_string(update.hits);
        int64_t expiry = update.expiration;
        if (expiry > 30*24*3600) expiry += std::time(nullptr);
        stream.write("add " + update.key + " 0 " + std::to_string(static_cast<int32_t>(expiry)) + " " + std::to_string(value.size()) + "\r\n" + value + "\r\n");
        line = stream.line();
        if (line == "NOT_STORED") { stream.write(increment); line = stream.line(); }
      }
      if (line.rfind("ERROR", 0) == 0 || line.rfind("CLIENT_ERROR", 0) == 0 || line.rfind("SERVER_ERROR", 0) == 0)
        throw std::runtime_error(line);
    } catch (...) { connections.erase(target); throw; }
  }
  std::unordered_map<std::string, uint64_t> get(const std::vector<std::string>& keys) {
    std::unordered_map<std::string, uint64_t> values;
    std::lock_guard<std::mutex> lock(io_mutex);
    std::unordered_map<std::string, std::vector<std::string>> groups;
    try { for (const auto& key : keys) groups[server(key)].push_back(key); }
    catch (const std::exception& e) { std::cerr << "Memcached get: " << e.what() << '\n'; return values; }
    for (const auto& [target, group] : groups) {
      try {
        auto& stream = connect(target);
        std::string request = "get";
        for (const auto& key : group) request += " " + key;
        stream.write(request + "\r\n");
        for (;;) {
          const auto line = stream.line();
          if (line == "END") break;
          const auto fields = split(line, ' ');
          if (fields.size() < 4 || fields[0] != "VALUE") throw std::runtime_error(line);
          const auto data = stream.bytes(std::stoull(fields[3]));
          if (stream.bytes(2) != "\r\n") throw std::runtime_error("invalid Memcached value terminator");
          try {
            size_t end; const auto value = std::stoll(data, &end);
            if (end == data.size() && value >= INT32_MIN && value <= INT32_MAX) values[fields[1]] = static_cast<uint64_t>(value);
          } catch (const std::exception&) { /* Go treats non-numeric values as zero. */ }
        }
      } catch (const std::exception& e) { connections.erase(target); std::cerr << "Memcached get: " << e.what() << '\n'; }
    }
    return values;
  }
};
Memcache::Memcache() : impl_(std::make_unique<Impl>()) {}
Memcache::~Memcache() = default;
std::unordered_map<std::string, uint64_t> Memcache::get(const std::vector<std::string>& keys) { return impl_->get(keys); }
void Memcache::update(std::vector<MemcacheUpdate> updates) {
  { std::lock_guard<std::mutex> lock(impl_->queue_mutex); impl_->queue.push_back(std::move(updates)); }
  impl_->pending.notify_one();
}
}  // namespace rlc
