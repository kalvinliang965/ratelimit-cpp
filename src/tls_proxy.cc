#include "tls_proxy.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

namespace rlc {
namespace {

std::string sslError() {
  const auto code = ERR_get_error();
  if (code == 0) return "unknown OpenSSL error";
  char buffer[256]{};
  ERR_error_string_n(code, buffer, sizeof(buffer));
  return buffer;
}

bool validPathnameUnixSocket(const std::string& path) {
  return !path.empty() && path.find('\0') == std::string::npos &&
         path.size() < sizeof(sockaddr_un::sun_path);
}

int createTcpListener(const std::string& host, uint16_t port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  addrinfo* addresses = nullptr;
  const auto result = getaddrinfo(host.empty() ? nullptr : host.c_str(),
                                  std::to_string(port).c_str(), &hints, &addresses);
  if (result != 0) throw std::runtime_error("TLS proxy listener lookup: " + std::string(gai_strerror(result)));

  int listener = -1;
  for (auto* address = addresses; address; address = address->ai_next) {
    listener = ::socket(address->ai_family, address->ai_socktype | SOCK_CLOEXEC,
                        address->ai_protocol);
    if (listener < 0) continue;
    int enabled = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    if (::bind(listener, address->ai_addr, address->ai_addrlen) == 0 &&
        ::listen(listener, SOMAXCONN) == 0) {
      break;
    }
    ::close(listener);
    listener = -1;
  }
  freeaddrinfo(addresses);
  if (listener < 0) {
    throw std::runtime_error(std::string("TLS proxy cannot bind listener: ") + std::strerror(errno));
  }
  return listener;
}

int createUnixListener(const std::string& path) {
  if (!validPathnameUnixSocket(path)) {
    throw std::runtime_error("TLS proxy listen_unix_socket is not a valid pathname socket");
  }
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  const auto listener = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (listener < 0) {
    throw std::runtime_error(std::string("TLS proxy cannot create Unix listener: ") + std::strerror(errno));
  }
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0 &&
      ::listen(listener, SOMAXCONN) == 0) {
    return listener;
  }
  const auto error = errno;
  ::close(listener);
  throw std::runtime_error(std::string("TLS proxy cannot bind Unix listener: ") + std::strerror(error));
}

int connectTcpUpstream(const std::string& host, uint16_t port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* addresses = nullptr;
  const auto result = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &addresses);
  if (result != 0) return -1;
  int socket = -1;
  for (auto* address = addresses; address; address = address->ai_next) {
    socket = ::socket(address->ai_family, address->ai_socktype | SOCK_CLOEXEC,
                      address->ai_protocol);
    if (socket >= 0 && ::connect(socket, address->ai_addr, address->ai_addrlen) == 0) break;
    if (socket >= 0) ::close(socket);
    socket = -1;
  }
  freeaddrinfo(addresses);
  return socket;
}

int connectUnixUpstream(const std::string& path) {
  // This proxy intentionally supports pathname sockets only. An abstract
  // socket has no filesystem permissions, which would defeat the private
  // directory/socket boundary used by the client-SAN service path.
  if (!validPathnameUnixSocket(path)) {
    return -1;
  }
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  const auto socket = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (socket < 0) return -1;
  if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0) {
    return socket;
  }
  ::close(socket);
  return -1;
}

int connectUpstream(const TlsProxyConfig& config) {
  if (!config.upstream_unix_socket.empty()) return connectUnixUpstream(config.upstream_unix_socket);
  return connectTcpUpstream(config.upstream_host, config.upstream_port);
}

bool clientSanMatches(X509* certificate, const std::string& expected) {
  unsigned char address[16];
  if (::inet_pton(AF_INET, expected.c_str(), address) == 1 ||
      ::inet_pton(AF_INET6, expected.c_str(), address) == 1) {
    return X509_check_ip_asc(certificate, expected.c_str(), 0) == 1;
  }
  // Go's x509 verifier treats DNS SANs as authoritative and does not fall
  // back to a common name when an extension is absent.  No partial wildcard
  // matches also mirrors Go's normal hostname matching rules.
  return X509_check_host(certificate, expected.c_str(), expected.size(),
                         X509_CHECK_FLAG_NEVER_CHECK_SUBJECT |
                             X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS,
                         nullptr) == 1;
}

int verifyClientCertificate(int preverified, X509_STORE_CTX* store) {
  if (preverified != 1) return 0;
  if (X509_STORE_CTX_get_error_depth(store) != 0) return 1;
  auto* ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(
      store, SSL_get_ex_data_X509_STORE_CTX_idx()));
  if (!ssl) return 0;
  auto* expected = static_cast<const std::string*>(
      SSL_CTX_get_app_data(SSL_get_SSL_CTX(ssl)));
  if (!expected || expected->empty()) return 1;
  if (clientSanMatches(X509_STORE_CTX_get_current_cert(store), *expected)) return 1;
  // Tell OpenSSL this is a certificate-name validation failure rather than a
  // generic callback failure. This yields the bad-certificate alert that Go's
  // VerifyPeerCertificate path exposes to a gRPC client.
  X509_STORE_CTX_set_error(store, X509_V_ERR_HOSTNAME_MISMATCH);
  return 0;
}

int selectHttp2(SSL*, const unsigned char** output, unsigned char* output_length,
                const unsigned char* input, unsigned int input_length, void*) {
  static const unsigned char h2[] = {2, 'h', '2'};
  unsigned char* selected = nullptr;
  const auto outcome = SSL_select_next_proto(&selected, output_length, h2, sizeof(h2), input, input_length);
  if (outcome != OPENSSL_NPN_NEGOTIATED) return SSL_TLSEXT_ERR_NOACK;
  *output = selected;
  return SSL_TLSEXT_ERR_OK;
}

bool sendAll(int socket, const char* data, size_t length) {
  size_t sent = 0;
  while (sent < length) {
    const auto count = ::send(socket, data + sent, length - sent, MSG_NOSIGNAL);
    if (count > 0) {
      sent += static_cast<size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

bool sslWriteAll(SSL* ssl, const char* data, size_t length) {
  size_t sent = 0;
  while (sent < length) {
    const auto count = SSL_write(ssl, data + sent, static_cast<int>(length - sent));
    if (count > 0) {
      sent += static_cast<size_t>(count);
      continue;
    }
    const auto error = SSL_get_error(ssl, count);
    if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) continue;
    return false;
  }
  return true;
}

int sslRead(SSL* ssl, char* data, size_t length) {
  while (true) {
    const auto count = SSL_read(ssl, data, static_cast<int>(length));
    if (count > 0) return count;
    const auto error = SSL_get_error(ssl, count);
    if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) continue;
    return -1;
  }
}

}  // namespace

struct TlsProxy::State {
  explicit State(TlsProxyConfig configuration) : config(std::move(configuration)) {}
  ~State() { if (context) SSL_CTX_free(context); }

  TlsProxyConfig config;
  SSL_CTX* context = nullptr;
  std::atomic<bool> stopping{false};
  std::mutex mutex;
  int listener = -1;
  uint16_t bound_port = 0;
  // A public pathname listener belongs to the proxy, unlike the private
  // upstream which main.cc owns. Keep its identity so shutdown only unlinks
  // the socket we bound and never a replacement at the configured path.
  bool listener_is_unix = false;
  dev_t listener_unix_device = 0;
  ino_t listener_unix_inode = 0;
  std::unordered_set<int> sockets;
  // Workers are detached so finished connections do not leave one joinable
  // std::thread object behind for the lifetime of the process.  The counter is
  // protected by mutex and lets stop() wait until shutdown has drained every
  // worker before it releases the TLS context.
  size_t active_sessions = 0;
  std::condition_variable sessions_drained;
};

namespace {

void recordUnixListener(const std::shared_ptr<TlsProxy::State>& state) {
  if (state->config.listen_unix_socket.empty()) return;
  struct stat pathname {};
  if (::lstat(state->config.listen_unix_socket.c_str(), &pathname) != 0 ||
      !S_ISSOCK(pathname.st_mode)) {
    throw std::runtime_error("TLS proxy Unix listener path could not be verified");
  }
  // Linux reports a distinct anonymous inode for fstat() on an AF_UNIX file
  // descriptor, so retain the pathname inode for safe later cleanup instead.
  state->listener_is_unix = true;
  state->listener_unix_device = pathname.st_dev;
  state->listener_unix_inode = pathname.st_ino;
}

void removeUnixListener(const std::shared_ptr<TlsProxy::State>& state) noexcept {
  if (!state->listener_is_unix) return;
  struct stat pathname {};
  if (::lstat(state->config.listen_unix_socket.c_str(), &pathname) == 0 &&
      S_ISSOCK(pathname.st_mode) && pathname.st_dev == state->listener_unix_device &&
      pathname.st_ino == state->listener_unix_inode) {
    ::unlink(state->config.listen_unix_socket.c_str());
  }
  state->listener_is_unix = false;
}

void closeListener(const std::shared_ptr<TlsProxy::State>& state) noexcept {
  if (state->listener >= 0) {
    ::shutdown(state->listener, SHUT_RDWR);
    ::close(state->listener);
    state->listener = -1;
  }
  removeUnixListener(state);
}

void registerSocket(const std::shared_ptr<TlsProxy::State>& state, int socket) {
  std::lock_guard<std::mutex> lock(state->mutex);
  state->sockets.insert(socket);
}

void closeSocket(const std::shared_ptr<TlsProxy::State>& state, int& socket) {
  if (socket < 0) return;
  std::lock_guard<std::mutex> lock(state->mutex);
  state->sockets.erase(socket);
  ::close(socket);
  socket = -1;
}

void stopSockets(const std::shared_ptr<TlsProxy::State>& state) {
  std::lock_guard<std::mutex> lock(state->mutex);
  for (const auto socket : state->sockets) ::shutdown(socket, SHUT_RDWR);
}

void finishSession(const std::shared_ptr<TlsProxy::State>& state) {
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    --state->active_sessions;
  }
  state->sessions_drained.notify_all();
}

void relay(const std::shared_ptr<TlsProxy::State>& state, int client) {
  SSL* ssl = SSL_new(state->context);
  int upstream = -1;
  if (!ssl) {
    closeSocket(state, client);
    return;
  }
  SSL_set_fd(ssl, client);
  if (SSL_accept(ssl) != 1 || state->stopping) {
    SSL_free(ssl);
    closeSocket(state, client);
    return;
  }
  upstream = connectUpstream(state->config);
  if (upstream < 0) {
    SSL_shutdown(ssl);
    SSL_free(ssl);
    closeSocket(state, client);
    return;
  }
  registerSocket(state, upstream);

  std::atomic<bool> closing{false};
  auto closeBoth = [&] {
    if (!closing.exchange(true)) {
      ::shutdown(client, SHUT_RDWR);
      ::shutdown(upstream, SHUT_RDWR);
    }
  };
  std::thread upstream_to_client([&] {
    char buffer[16384];
    while (!state->stopping) {
      const auto count = ::recv(upstream, buffer, sizeof(buffer), 0);
      if (count <= 0 || !sslWriteAll(ssl, buffer, static_cast<size_t>(count))) break;
    }
    closeBoth();
  });

  char buffer[16384];
  while (!state->stopping) {
    const auto count = sslRead(ssl, buffer, sizeof(buffer));
    if (count <= 0 || !sendAll(upstream, buffer, static_cast<size_t>(count))) break;
  }
  closeBoth();
  upstream_to_client.join();
  SSL_shutdown(ssl);
  SSL_free(ssl);
  closeSocket(state, upstream);
  closeSocket(state, client);
}

void acceptLoop(const std::shared_ptr<TlsProxy::State>& state) {
  while (!state->stopping) {
    const auto client = ::accept4(state->listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (client < 0) {
      if (errno == EINTR) continue;
      if (state->stopping) break;
      continue;
    }
    registerSocket(state, client);
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->stopping) {
        state->sockets.erase(client);
        ::close(client);
        break;
      }
      ++state->active_sessions;
    }
    try {
      std::thread([state, client] {
        // relay() only uses C/OpenSSL and socket APIs in its normal path.  Do
        // not let an unexpected allocation/system error strand stop() while
        // it waits for the active-session count to reach zero.
        try { relay(state, client); } catch (...) {}
        finishSession(state);
      }).detach();
    } catch (...) {
      int socket = client;
      closeSocket(state, socket);
      finishSession(state);
    }
  }
}

SSL_CTX* makeContext(TlsProxy::State* state) {
  const auto& config = state->config;
  if (config.server_certificate.empty() || config.server_private_key.empty()) {
    throw std::runtime_error("TLS proxy requires server_certificate and server_private_key");
  }
  if (!config.listen_unix_socket.empty()) {
    if (config.listen_port != 0 || !validPathnameUnixSocket(config.listen_unix_socket)) {
      throw std::runtime_error("TLS proxy listen_unix_socket is not a valid standalone pathname listener");
    }
  }
  if (config.upstream_unix_socket.empty()) {
    if (config.upstream_port == 0) throw std::runtime_error("TLS proxy requires an upstream_port or upstream_unix_socket");
  } else {
    if (config.upstream_port != 0) {
      throw std::runtime_error("TLS proxy accepts either upstream_port or upstream_unix_socket, not both");
    }
    if (!validPathnameUnixSocket(config.upstream_unix_socket)) {
      throw std::runtime_error("TLS proxy upstream_unix_socket is not a valid pathname socket");
    }
  }
  if (!config.required_client_san.empty() && config.client_ca_certificate.empty()) {
    throw std::runtime_error("TLS proxy requires client_ca_certificate to verify required_client_san");
  }
  SSL_CTX* context = SSL_CTX_new(TLS_server_method());
  if (!context) throw std::runtime_error("TLS proxy context: " + sslError());
  try {
    if (SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1 ||
        SSL_CTX_use_certificate_chain_file(context, config.server_certificate.c_str()) != 1 ||
        SSL_CTX_use_PrivateKey_file(context, config.server_private_key.c_str(), SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_check_private_key(context) != 1) {
      throw std::runtime_error("TLS proxy certificate: " + sslError());
    }
    SSL_CTX_set_mode(context, SSL_MODE_AUTO_RETRY);
    SSL_CTX_set_alpn_select_cb(context, selectHttp2, nullptr);
    if (!config.client_ca_certificate.empty()) {
      SSL_CTX_set_default_verify_paths(context);
      if (SSL_CTX_load_verify_locations(context, config.client_ca_certificate.c_str(), nullptr) != 1) {
        throw std::runtime_error("TLS proxy client CA: " + sslError());
      }
      SSL_CTX_set_app_data(context, &state->config.required_client_san);
      SSL_CTX_set_verify(context, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                         verifyClientCertificate);
    }
  } catch (...) {
    SSL_CTX_free(context);
    throw;
  }
  return context;
}

}  // namespace

TlsProxy::TlsProxy(TlsProxyConfig config) : config_(std::move(config)) {}
TlsProxy::~TlsProxy() { stop(); }

void TlsProxy::start() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (state_) throw std::runtime_error("TLS proxy is already running");
  OPENSSL_init_ssl(0, nullptr);
  auto state = std::make_shared<State>(config_);
  state->context = makeContext(state.get());
  try {
    if (!state->config.listen_unix_socket.empty()) {
      state->listener = createUnixListener(state->config.listen_unix_socket);
      recordUnixListener(state);
    } else {
      state->listener = createTcpListener(state->config.listen_host, state->config.listen_port);
      sockaddr_storage address{};
      socklen_t length = sizeof(address);
      if (::getsockname(state->listener, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        throw std::runtime_error("TLS proxy getsockname: " + std::string(std::strerror(errno)));
      }
      if (address.ss_family == AF_INET) {
        state->bound_port = ntohs(reinterpret_cast<sockaddr_in*>(&address)->sin_port);
      } else if (address.ss_family == AF_INET6) {
        state->bound_port = ntohs(reinterpret_cast<sockaddr_in6*>(&address)->sin6_port);
      } else {
        throw std::runtime_error("TLS proxy listener has unsupported address family");
      }
    }
    state_ = state;
    accept_thread_ = std::thread([state] { acceptLoop(state); });
  } catch (...) {
    state_ = nullptr;
    closeListener(state);
    throw;
  }
}

void TlsProxy::stop() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (!state_) return;
  auto state = state_;
  state->stopping = true;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    closeListener(state);
  }
  if (accept_thread_.joinable()) accept_thread_.join();
  stopSockets(state);
  {
    std::unique_lock<std::mutex> lock(state->mutex);
    state->sessions_drained.wait(lock, [&] { return state->active_sessions == 0; });
  }
  state_ = nullptr;
}

uint16_t TlsProxy::listenPort() const {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  return state_ ? state_->bound_port : 0;
}

}  // namespace rlc
