# C++ Envoy Rate Limit Service

This is a standalone C++17 implementation of the adjacent `ratelimit` Go
service. It keeps the Envoy v3 RateLimitService gRPC wire API, the policy YAML
format, Redis key layout, counter updates, and environment-variable interface.
It can replace the `ratelimit` container in `ratelimit-lab` by changing only the
image or executable.

## Build and run

On a Debian/Ubuntu host with C++ development packages installed:

```sh
sudo apt-get install build-essential protobuf-compiler protobuf-compiler-grpc \
  libgrpc++-dev libprotobuf-dev libyaml-cpp-dev libssl-dev
make -j"$(nproc)"
```

If system package installation is unavailable, `make deps` downloads the gRPC
and Redis development packages into `.deps/` without root access. It does not
modify the host. The host still needs the standard compiler, Protobuf,
yaml-cpp, OpenSSL, and package downloader.

The service accepts the same normal runtime layout as Go:

```sh
RUNTIME_ROOT=$PWD/../ratelimit-lab/messaging \
RUNTIME_SUBDIRECTORY=. \
RUNTIME_APPDIRECTORY=. \
RUNTIME_WATCH_ROOT=false \
REDIS_SOCKET_TYPE=tcp REDIS_URL=127.0.0.1:6379 \
./build/ratelimit-cpp
```

For the existing lab compose files, mount the policy at
`/data/ratelimit/config/policy.yaml` and use:

```text
CONFIG_TYPE=FILE
RUNTIME_ROOT=/data
RUNTIME_SUBDIRECTORY=ratelimit
RUNTIME_APPDIRECTORY=config
RUNTIME_WATCH_ROOT=false
GRPC_PORT=8081
```

The default listeners match the Go service: HTTP/JSON on `8080`, gRPC on
`8081`, and debug endpoints on `6070`. `POST /json`, `/healthcheck`,
`/rlconfig`, and `/stats` are provided. The gRPC service supports TCP or
`GRPC_UDS`, optional server TLS/mTLS, and the standard Envoy RLS method:

```text
/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit
```

`CONFIG_TYPE=GRPC_XDS_SOTW` enables an ADS SotW client for
`RateLimitConfig` resources. It supports `CONFIG_GRPC_XDS_*`, including TLS,
node identity, metadata headers, ACK/NACK, reconnecting, and graceful stream
cancellation.

## Compatibility coverage

The implementation supports file-policy reloads, hierarchical descriptors,
exact/default/middle/trailing wildcards, `share_threshold`, descriptor
overrides, unlimited/shadow/quota modes, named replacements, metadata,
response/request/per-unit headers, all rate units and opt-in calendar months,
hits addends and negative refunds.

Redis supports TCP and Unix sockets, Redis URLs/ACL credentials, TLS,
Sentinel discovery, Cluster MOVED/ASK redirection, separate per-second Redis,
pooling, pipelining, timeouts, local over-limit cache, jitter, and
`STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`. Memcached is also supported with
the Go-compatible asynchronous-update semantics.

## Verify

`make test` runs the policy unit tests and 20 live C++ Redis/gRPC scenarios.
It needs `redis-server` and Python packages `grpcio` and `protobuf`.

`make test-parity` also builds the adjacent Go reference and compares every
serialized gRPC response, gRPC error, and final Redis counters. It tests 199
request comparisons across the policy and cache behaviors listed above.

```sh
make test
make test-parity
```

The malformed-wire assertion remains byte-exact. Go protobuf v1.36.11
deliberately chooses either an ASCII space or U+00A0 after `proto:` from a
hash of the running Go executable. Therefore, an independently rebuilt Go
reference can report a real status-detail mismatch even when every normal API
behavior agrees; the result is not normalized. The pinned Docker reference
currently emits U+00A0, which is the value used by this C++ build.

The checked-in `proto/reference.pb` pins the exact API descriptors imported by
the Go reference. `scripts/vendor_protos.py` creates the C++ schemas from that
descriptor set. It is a maintenance operation, not part of the normal build.
