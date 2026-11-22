// Client connections shared by the Redis and Memcached backends: address
// parsing, client-side TLS (the REDIS_TLS_* / MEMCACHE_TLS_* settings; unrelated
// to the gRPC server's TLS proxy), and a buffered stream over TCP, a Unix
// socket, or TLS.
#pragma once
#include <openssl/ssl.h>
#include <cstddef>
#include <string>
#include <vector>

namespace rlc::backend {

std::vector<std::string> split(const std::string& text, char delimiter);

// The error radix (the Go service's Redis client) returns for a pooled
// connection whose peer closed it; it becomes the gRPC UNKNOWN details.
extern const char* const kConnEofError;

struct Address {
  std::string host, port, path, auth;
  int db = 0;
};
Address parseAddress(std::string value, bool unix_socket);

// Client TLS context from <prefix>TLS_CACERT, TLS_CLIENT_CERT/KEY and
// TLS_SKIP_HOSTNAME_VERIFICATION.  No context when TLS is disabled.
struct Tls {
  SSL_CTX* ctx = nullptr;
  bool skip_hostname = false;
  Tls(bool enabled, const std::string& prefix);
  ~Tls();
  Tls(const Tls&) = delete;
  Tls& operator=(const Tls&) = delete;
};

// One bounded, buffered connection. Blocking operations use the configured I/O
// timeout, and the connect timeout also covers TCP establishment.
class Stream {
 public:
  Stream(const Address& a, double timeout, const Tls& tls);
  ~Stream();
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;

  // State of a connection that has been idle in the pool.  Checked without
  // blocking and without consuming data, so a usable connection is unchanged.
  // Best effort: Redis can still close it right after this check.
  enum class Idle { Usable, Closed, Broken };
  Idle idleState();

  void write(const std::string& data);
  std::string line();
  std::string bytes(size_t count);

 private:
  Idle tlsIdleState();
  void fill();
  int fd_ = -1;
  SSL* ssl_ = nullptr;
  std::string input_;
  size_t offset_ = 0;
};

}  // namespace rlc::backend
