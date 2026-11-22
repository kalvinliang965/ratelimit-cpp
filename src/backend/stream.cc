#include "stream.h"
#include "common.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/err.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace rlc::backend {

const char* const kConnEofError = "response returned from Conn: EOF";

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

namespace {
std::string sslError() {
  char buffer[256];
  ERR_error_string_n(ERR_get_error(), buffer, sizeof buffer);
  return buffer;
}
}  // namespace

Tls::Tls(bool enabled, const std::string& prefix) {
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

Tls::~Tls() { if (ctx) SSL_CTX_free(ctx); }

Stream::Stream(const Address& a, double timeout, const Tls& tls) {
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

Stream::~Stream() { if (ssl_) SSL_free(ssl_); if (fd_ >= 0) ::close(fd_); }

Stream::Idle Stream::idleState() {
  // An idle connection has no reply bytes left over; any would mean the
  // reply stream is out of step with the commands.
  if (offset_ < input_.size() || (ssl_ && SSL_pending(ssl_) > 0)) return Idle::Broken;
  char byte;
  ssize_t n;
  do { n = ::recv(fd_, &byte, 1, MSG_PEEK | MSG_DONTWAIT); } while (n < 0 && errno == EINTR);
  if (n == 0) return Idle::Closed;
  if (n < 0) return errno == EAGAIN || errno == EWOULDBLOCK ? Idle::Usable : Idle::Broken;
  // Redis sends nothing unsolicited to an idle RESP2 connection.  Over TLS,
  // the readable bytes may instead be TLS records such as TLS 1.3 session
  // tickets, which carry no application data.
  return ssl_ ? tlsIdleState() : Idle::Broken;
}

void Stream::write(const std::string& data) {
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

std::string Stream::line() {
  for (;;) {
    const auto end = input_.find("\r\n", offset_);
    if (end != std::string::npos) {
      std::string result = input_.substr(offset_, end-offset_); offset_ = end+2; return result;
    }
    fill();
    if (input_.size()-offset_ > 16*1024*1024) throw std::runtime_error("backend protocol line too long");
  }
}

std::string Stream::bytes(size_t count) {
  while (input_.size()-offset_ < count) fill();
  auto result = input_.substr(offset_, count); offset_ += count; return result;
}

// Process pending TLS records without blocking: SSL_peek reads records but
// leaves application data in place.
Stream::Idle Stream::tlsIdleState() {
  const int flags = fcntl(fd_, F_GETFL, 0);
  if (flags < 0 || fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) return Idle::Broken;
  ERR_clear_error();
  char byte;
  const int n = SSL_peek(ssl_, &byte, 1);
  const int error = n > 0 ? SSL_ERROR_NONE : SSL_get_error(ssl_, n);
  ERR_clear_error();
  fcntl(fd_, F_SETFL, flags);
  if (n > 0) return Idle::Broken;                          // unsolicited application data
  if (error == SSL_ERROR_WANT_READ) return Idle::Usable;   // only non-application records
  if (error == SSL_ERROR_ZERO_RETURN) return Idle::Closed; // close_notify
  return Idle::Broken;
}

void Stream::fill() {
  if (offset_) { input_.erase(0, offset_); offset_ = 0; }
  char buffer[16384];
  int64_t n;
  do { n = ssl_ ? SSL_read(ssl_, buffer, sizeof buffer) : ::recv(fd_, buffer, sizeof buffer, 0); }
  while (n < 0 && errno == EINTR);
  // radix, the Go service's Redis client, returns this wrapped error to the
  // rate-limit handler when a pooled connection is closed while awaiting a
  // reply.  It becomes the observable gRPC UNKNOWN details, so preserve the
  // same boundary rather than exposing the transport's bare EOF.
  if (n == 0) throw std::runtime_error(kConnEofError);
  if (n < 0) throw std::runtime_error("backend read: " + std::string(std::strerror(errno)));
  input_.append(buffer, n);
}

}  // namespace rlc::backend
