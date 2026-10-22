# TLS client-SAN proxy

The grpc++ server credentials available to this service can require and verify
a client certificate, but cannot reject an otherwise valid certificate when its
DNS or IP SAN differs from `GRPC_CLIENT_TLS_SAN`. For the combination below,
the C++ service starts `TlsProxy` as its public gRPC listener:

```text
GRPC_SERVER_USE_TLS=true
GRPC_CLIENT_TLS_CACERT=/path/to/client-ca.pem
GRPC_CLIENT_TLS_SAN=client.example
```

The proxy loads `GRPC_SERVER_TLS_CERT` and `GRPC_SERVER_TLS_KEY`, requires a
certificate signed by the configured client CA, verifies the requested DNS/IP
SAN during the TLS handshake, and negotiates ALPN `h2`. A mismatched SAN ends
the TLS handshake before HTTP/2 or gRPC application bytes are accepted. With a
SAN setting but no client CA, the service keeps the normal native grpc++ TLS
path, matching the Go service's behavior.

## Public listener and private plaintext hop

The proxy owns the only public gRPC listener in this mode. It normally binds
the configured `GRPC_HOST:GRPC_PORT` TCP endpoint. When `GRPC_UDS` is set, it
instead binds that pathname Unix socket and carries the same TLS/mTLS handshake
over the Unix connection. `TlsProxyConfig::listen_unix_socket` validates that
pathname and keeps its device/inode identity, so shutdown unlinks only the
socket created by this proxy.

Before the internal gRPC server starts, `main.cc` creates a unique `mkdtemp`
directory named
`ratelimit-cpp-tls-proxy-XXXXXX`, verifies it is owned by the service user, and
sets it to mode `0700`. The internal grpc++ server binds only to:

```text
unix:<private-directory>/grpc.sock
```

After `BuildAndStart()` creates that socket, the service verifies that it is an
owned Unix socket and changes its mode to `0600`. Only then does `TlsProxy`
start accepting public TLS connections. The proxy receives the pathname through
`TlsProxyConfig::upstream_unix_socket` and opens it with `AF_UNIX`; it does not
create or connect to an internal loopback TCP port. `TlsProxyConfig` accepts
either this Unix pathname or its legacy TCP `upstream_host`/`upstream_port`,
never both.

After a successful handshake, the proxy copies HTTP/2 bytes in both directions
without parsing or reserializing them. This preserves gRPC framing, metadata,
compression, and trailers while keeping decrypted traffic inside the
permission-restricted filesystem namespace.

On shutdown the proxy stops before the internal gRPC server and removes its
public `GRPC_UDS` pathname when one was configured. Once the server has shut
down and `Server::Wait()` returns, the service unlinks `grpc.sock` and removes
its private directory. The owner objects also perform their respective cleanup
on startup failures and exception unwinding.
