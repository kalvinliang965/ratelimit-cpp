#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace rlc {

// Configuration for a TLS front end which forwards decrypted HTTP/2 bytes to
// an internal, insecure gRPC listener.  `client_ca_certificate` turns on mTLS;
// `required_client_san` is checked by OpenSSL while the TLS handshake is in
// progress, before any HTTP/2 or gRPC bytes reach the upstream listener.
struct TlsProxyConfig {
  std::string listen_host = "0.0.0.0";
  uint16_t listen_port = 0;
  // A pathname Unix listener can replace the public TCP listener. This is
  // used when GRPC_UDS and GRPC_CLIENT_TLS_SAN are configured together.
  std::string listen_unix_socket;
  // Exactly one upstream transport is selected. The TCP fields preserve the
  // proxy's standalone use; the service's client-SAN path uses a pathname
  // Unix socket so decrypted gRPC traffic has no loopback listener.
  std::string upstream_host = "127.0.0.1";
  uint16_t upstream_port = 0;
  std::string upstream_unix_socket;
  std::string server_certificate;
  std::string server_private_key;
  std::string client_ca_certificate;
  std::string required_client_san;
};

// A small TLS terminator intended for the GRPC_CLIENT_TLS_SAN compatibility
// path.  It does not parse HTTP/2: after OpenSSL accepts the client connection,
// it copies plaintext bytes in both directions to the configured upstream.
class TlsProxy {
 public:
  // Defined privately in the implementation; public only so the standalone
  // relay helpers can retain shared ownership without exposing its contents.
  struct State;

  explicit TlsProxy(TlsProxyConfig config);
  ~TlsProxy();

  TlsProxy(const TlsProxy&) = delete;
  TlsProxy& operator=(const TlsProxy&) = delete;

  // Starts accepting connections. A listen_port of zero selects an ephemeral
  // port, retrievable through listenPort() after this succeeds.
  void start();
  void stop();
  uint16_t listenPort() const;

 private:
  TlsProxyConfig config_;
  mutable std::mutex lifecycle_mutex_;
  std::shared_ptr<State> state_;
  std::thread accept_thread_;
};

}  // namespace rlc
