#include "backend.h"
#include "common.h"
#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <resolv.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>

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
using Clock = std::chrono::steady_clock;
std::vector<std::string> split(const std::string& text, char delimiter) {
  std::vector<std::string> out;
  size_t pos = 0, end;
  do {
    end = text.find(delimiter, pos);
    out.push_back(text.substr(pos, end == std::string::npos ? end : end-pos));
    pos = end + 1;
  } while (end != std::string::npos);
  return out;
}
std::string lower(std::string text) {
  std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) { return std::tolower(c); });
  return text;
}
struct Address {
  std::string host, port, path, auth;
  int db = 0;
};
Address parseAddress(std::string value, bool unix_socket) {
  Address a;
  if (value.rfind("unix://", 0) == 0) { unix_socket = true; value.erase(0, 7); }
  if (unix_socket) { a.path = value; return a; }
  if (value.rfind("redis://", 0) == 0) value.erase(0, 8);
  const auto slash = value.find('/');
  if (slash != std::string::npos) {
    const auto db = value.substr(slash+1);
    if (!db.empty()) a.db = std::stoi(db);
    value.resize(slash);
  }
  const auto at = value.rfind('@');
  if (at != std::string::npos) { a.auth = value.substr(0, at); value.erase(0, at+1); }
  if (!value.empty() && value.front() == '[') {
    const auto bracket = value.find(']');
    if (bracket == std::string::npos) throw std::runtime_error("invalid Redis address");
    a.host = value.substr(1, bracket-1);
    a.port = bracket+1 < value.size() ? value.substr(bracket+2) : "6379";
  } else {
    const auto colon = value.rfind(':');
    a.host = value.substr(0, colon);
    a.port = colon == std::string::npos ? "6379" : value.substr(colon+1);
  }
  return a;
}
std::string sslError() {
  char buffer[256];
  ERR_error_string_n(ERR_get_error(), buffer, sizeof buffer);
  return buffer;
}
struct Tls {
  SSL_CTX* ctx = nullptr;
  bool skip_hostname = false;
  Tls(bool enabled, const std::string& prefix) {
    if (!enabled) return;
    ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) throw std::runtime_error("TLS: " + sslError());
    try {
      SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
      SSL_CTX_set_default_verify_paths(ctx);
      skip_hostname = envBool((prefix+"TLS_SKIP_HOSTNAME_VERIFICATION").c_str());
      const auto ca = env((prefix+"TLS_CACERT").c_str());
      if (!ca.empty() && !SSL_CTX_load_verify_locations(ctx, ca.c_str(), nullptr))
        throw std::runtime_error("TLS CA: " + sslError());
      const auto cert = env((prefix+"TLS_CLIENT_CERT").c_str());
      const auto key = env((prefix+"TLS_CLIENT_KEY").c_str());
      if (!cert.empty() && !key.empty()) {
        if (SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx, key.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(ctx) != 1) throw std::runtime_error("TLS client key pair: " + sslError());
      }
    } catch (...) { SSL_CTX_free(ctx); ctx = nullptr; throw; }
  }
  ~Tls() { if (ctx) SSL_CTX_free(ctx); }
};

// One bounded, buffered connection. Blocking operations use the configured I/O
// timeout, and the connect timeout also covers TCP establishment.
class Stream {
 public:
  Stream(const Address& a, double timeout, const Tls& tls) {
    auto connectFd = [&](int socket_fd, const sockaddr* sa, socklen_t length) {
      if (socket_fd < 0) return false;
      const int flags = fcntl(socket_fd, F_GETFL, 0);
      fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
      int ret = ::connect(socket_fd, sa, length);
      if (ret < 0 && errno == EINPROGRESS) {
        pollfd p{socket_fd, POLLOUT, 0};
        ret = ::poll(&p, 1, timeout > 0 ? static_cast<int>(timeout*1000) : -1);
        if (ret > 0) {
          int error = 0; socklen_t len = sizeof(error);
          if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) ret = -1;
          else if (error) { errno = error; ret = -1; }
          else ret = 0;
        } else { errno = ETIMEDOUT; ret = -1; }
      }
      if (ret < 0) { ::close(socket_fd); return false; }
      fcntl(socket_fd, F_SETFL, flags);
      fd_ = socket_fd;
      return true;
    };
    if (!a.path.empty()) {
      sockaddr_un sa{}; sa.sun_family = AF_UNIX;
      if (a.path.size() >= sizeof sa.sun_path) throw std::runtime_error("unix socket path too long");
      std::memcpy(sa.sun_path, a.path.c_str(), a.path.size()+1);
      connectFd(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0), reinterpret_cast<sockaddr*>(&sa), sizeof sa);
    } else {
      addrinfo hints{}, *addresses = nullptr;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_family = AF_UNSPEC;
      const int error = getaddrinfo(a.host.c_str(), a.port.c_str(), &hints, &addresses);
      if (error) throw std::runtime_error("lookup " + a.host + ": " + gai_strerror(error));
      for (auto* ai = addresses; ai && fd_ < 0; ai = ai->ai_next)
        connectFd(::socket(ai->ai_family, SOCK_STREAM | SOCK_CLOEXEC, ai->ai_protocol), ai->ai_addr, ai->ai_addrlen);
      freeaddrinfo(addresses);
    }
    if (fd_ < 0) throw std::runtime_error("backend connection: " + std::string(std::strerror(errno)));
    int one = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (timeout > 0) {
      timeval tv{static_cast<time_t>(timeout), static_cast<suseconds_t>((timeout-static_cast<int64_t>(timeout))*1e6)};
      setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
      setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    }
    if (tls.ctx) {
      ssl_ = SSL_new(tls.ctx);
      SSL_set_fd(ssl_, fd_);
      SSL_set_tlsext_host_name(ssl_, a.host.c_str());
      if (!tls.skip_hostname) {
        unsigned char bytes[16];
        if (inet_pton(AF_INET, a.host.c_str(), bytes) == 1 || inet_pton(AF_INET6, a.host.c_str(), bytes) == 1)
          X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl_), a.host.c_str());
        else SSL_set1_host(ssl_, a.host.c_str());
      }
      if (SSL_connect(ssl_) != 1) {
        const std::string message = "TLS handshake: " + sslError();
        SSL_free(ssl_); ssl_ = nullptr; ::close(fd_); fd_ = -1;
        throw std::runtime_error(message);
      }
    }
  }
  ~Stream() { if (ssl_) SSL_free(ssl_); if (fd_ >= 0) ::close(fd_); }
  void write(const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
      const size_t count = std::min<size_t>(data.size()-sent, std::numeric_limits<int>::max());
      auto n = ssl_ ? SSL_write(ssl_, data.data()+sent, static_cast<int>(count)) :
          ::send(fd_, data.data()+sent, count, MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) throw std::runtime_error("backend write: " + std::string(std::strerror(errno)));
      sent += n;
    }
  }
  std::string line() {
    for (;;) {
      const auto end = input_.find("\r\n", offset_);
      if (end != std::string::npos) {
        std::string result = input_.substr(offset_, end-offset_); offset_ = end+2; return result;
      }
      fill();
      if (input_.size()-offset_ > 16*1024*1024) throw std::runtime_error("backend protocol line too long");
    }
  }
  std::string bytes(size_t count) {
    while (input_.size()-offset_ < count) fill();
    auto result = input_.substr(offset_, count); offset_ += count; return result;
  }
 private:
  void fill() {
    if (offset_) { input_.erase(0, offset_); offset_ = 0; }
    char buffer[16384];
    int64_t n;
    do { n = ssl_ ? SSL_read(ssl_, buffer, sizeof buffer) : ::recv(fd_, buffer, sizeof buffer, 0); }
    while (n < 0 && errno == EINTR);
    // radix, the Go service's Redis client, returns this wrapped error to the
    // rate-limit handler when a pooled connection is closed while awaiting a
    // reply.  It becomes the observable gRPC UNKNOWN details, so preserve the
    // same boundary rather than exposing the transport's bare EOF.
    if (n == 0) throw std::runtime_error("response returned from Conn: EOF");
    if (n < 0) throw std::runtime_error("backend read: " + std::string(std::strerror(errno)));
    input_.append(buffer, n);
  }
  int fd_ = -1;
  SSL* ssl_ = nullptr;
  std::string input_;
  size_t offset_ = 0;
};

struct Reply {
  char type;
  std::string text;
  std::vector<Reply> array;
};
Reply readReply(Stream& stream, unsigned depth = 0) {
  if (depth > 64) throw std::runtime_error("Redis protocol nesting limit");
  auto line = stream.line();
  if (line.empty()) throw std::runtime_error("empty Redis reply");
  Reply r{line[0], line.substr(1), {}};
  if (r.type == '$') {
    const int64_t size = std::stoll(r.text);
    if (size == -1) { r.type = '_'; r.text.clear(); }
    else {
      if (size < 0 || size > 512*1024*1024) throw std::runtime_error("invalid Redis bulk size");
      r.text = stream.bytes(size);
      if (stream.bytes(2) != "\r\n") throw std::runtime_error("invalid Redis bulk terminator");
    }
  } else if (r.type == '*') {
    const int64_t count = std::stoll(r.text);
    if (count < -1 || count > 1024*1024) throw std::runtime_error("invalid Redis array size");
    for (int64_t i = 0; i < count; ++i) r.array.push_back(readReply(stream, depth+1));
  } else if (r.type != '+' && r.type != '-' && r.type != ':') throw std::runtime_error("invalid Redis reply type");
  return r;
}
std::string encode(const std::vector<std::string>& args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& arg : args) out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
  return out;
}
Reply command(Stream& stream, const std::vector<std::string>& args) {
  stream.write(encode(args));
  auto reply = readReply(stream);
  if (reply.type == '-') throw std::runtime_error(reply.text);
  return reply;
}
void authenticate(Stream& stream, const std::string& auth) {
  if (auth.empty()) return;
  const auto colon = auth.find(':');
  if (colon == std::string::npos) command(stream, {"AUTH", auth});
  else command(stream, {"AUTH", auth.substr(0, colon), auth.substr(colon+1)});
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
}

struct Redis::Impl {
  std::string prefix, kind, url, auth, sentinel_auth;
  bool unix_socket, close_readonly;
  double timeout;
  size_t pool_size;
  Tls tls;
  mutable std::mutex mutex;
  std::condition_variable available;
  size_t busy = 0;
  std::unordered_map<std::string, std::vector<std::unique_ptr<Stream>>> idle;
  std::unordered_map<uint16_t, std::string> slots;
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
        tls(envBool((prefix+"TLS").c_str()), "REDIS_") {
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
        const auto target = endpoint("");
        auto stream = connect(target);
        if (command(*stream, {"PING"}).text != "PONG") throw std::runtime_error("unexpected PING response");
        idle[target].push_back(std::move(stream)); healthy = true; break;
      } catch (const std::exception& e) {
        const auto elapsed = std::chrono::duration<double>(Clock::now()-start).count();
        if (max_elapsed > 0 && elapsed >= max_elapsed) throw;
        std::cerr << "Redis connection unavailable; retrying: " << e.what() << '\n';
        std::this_thread::sleep_for(std::chrono::duration<double>(std::max(0.001, delay)));
        delay = std::min(max_delay, delay*2);
      }
    }
  }

  std::string endpoint(const std::string& key) {
    if (kind == "single") return url;
    const auto addresses = split(url, ',');
    if (kind == "cluster") {
      std::lock_guard<std::mutex> lock(mutex);
      const auto found = slots.find(slot(key));
      return found == slots.end() ? addresses.front() : found->second;
    }
    std::string last = "no reachable sentinel";
    for (size_t i = 1; i < addresses.size(); ++i) {
      try {
        auto address = parseAddress(addresses[i], false);
        Stream sentinel(address, timeout, tls);
        authenticate(sentinel, address.auth.empty() ? sentinel_auth : address.auth);
        const auto reply = command(sentinel, {"SENTINEL", "get-master-addr-by-name", addresses.front()});
        if (reply.array.size() != 2) throw std::runtime_error("sentinel master not found");
        return "[" + reply.array[0].text + "]:" + reply.array[1].text;
      } catch (const std::exception& e) { last = e.what(); }
    }
    throw std::runtime_error(last);
  }
  std::unique_ptr<Stream> connect(const std::string& target) {
    const auto address = parseAddress(target, kind == "single" && unix_socket);
    auto stream = std::make_unique<Stream>(address, timeout, tls);
    authenticate(*stream, address.auth.empty() ? auth : address.auth);
    if (address.db) command(*stream, {"SELECT", std::to_string(address.db)});
    return stream;
  }
  std::unique_ptr<Stream> acquire(const std::string& target) {
    std::unique_lock<std::mutex> lock(mutex);
    available.wait(lock, [&] { return busy < pool_size; });
    ++busy;
    auto& connections = idle[target];
    if (!connections.empty()) {
      auto stream = std::move(connections.back()); connections.pop_back(); return stream;
    }
    lock.unlock();
    try { return connect(target); }
    catch (...) { release(target, nullptr); throw; }
  }
  void release(const std::string& target, std::unique_ptr<Stream> stream) {
    std::lock_guard<std::mutex> lock(mutex);
    if (stream) idle[target].push_back(std::move(stream));
    --busy; available.notify_one();
  }
  void group(const std::vector<const RedisCommand*>& commands) {
    std::string target = endpoint(commands.front()->key);
    bool asking = false;
    for (int attempt = 0; attempt < 8; ++attempt) {
      auto stream = acquire(target);
      std::vector<Reply> replies;
      try {
        if (asking) command(*stream, {"ASKING"});
        std::string buffer;
        for (const auto* c : commands) buffer += encode(c->args);
        stream->write(buffer);
        for (size_t i = 0; i < commands.size(); ++i) replies.push_back(readReply(*stream));
      } catch (...) { healthy = false; release(target, nullptr); throw; }
      std::string error;
      for (const auto& reply : replies) if (reply.type == '-') { error = reply.text; break; }
      bool discard = error.rfind("READONLY", 0) == 0 && (close_readonly || kind == "sentinel");
      release(target, discard ? nullptr : std::move(stream));
      if (!error.empty()) {
        if (kind == "cluster" && (error.rfind("MOVED ", 0) == 0 || error.rfind("ASK ", 0) == 0)) {
          const auto fields = split(error, ' ');
          if (fields.size() == 3) {
            asking = fields[0] == "ASK";
            target = fields[2];
            if (!asking) { std::lock_guard<std::mutex> lock(mutex); slots[static_cast<uint16_t>(std::stoi(fields[1]))] = target; }
            continue;
          }
        }
        healthy = !discard; throw std::runtime_error(error);
      }
      for (size_t i = 0; i < commands.size(); ++i) if (commands[i]->result) {
        if (replies[i].type == '_') *commands[i]->result = 0;
        else {
          if (!replies[i].text.empty() && replies[i].text[0] == '-')
            throw std::runtime_error("cannot decode negative Redis value into uint64");
          *commands[i]->result = std::stoull(replies[i].text);
        }
      }
      healthy = true; return;
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

namespace {
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
}
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
}
