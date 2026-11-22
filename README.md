# C++ Envoy Rate Limit Service

This is a standalone C++17 implementation of the upstream `ratelimit` Go
service. It keeps the Envoy v3 RateLimitService gRPC wire API, the policy YAML
format, Redis key layout, counter updates, and environment-variable interface.
It can replace the `ratelimit` container in `ratelimit-lab` by changing only the
image or executable.

## Build and run

On a Debian/Ubuntu host with C++ development packages installed:

```sh
sudo apt-get install build-essential protobuf-compiler protobuf-compiler-grpc \
  libgrpc++-dev libprotobuf-dev libyaml-cpp-dev libssl-dev nlohmann-json3-dev
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

## Wire codec

The rate-limit wire codec can be built and tested without starting Redis or
gRPC. Run `make test-wire`; see [the wire codec guide](docs/ratelimit_wire.md)
for the JSON fixtures, output files, and standalone library.

## Verify

The Python test environment is pinned in `pyproject.toml` and `uv.lock` (no
installable package). The Makefile uses [uv](https://docs.astral.sh/uv/) when
it is installed and otherwise `python3`, which then needs `grpcio`, `protobuf`
and `pytest`; set `PYTHON=...` to choose another interpreter. Redis tests need
`redis-server`.

The Go reference is pinned as a submodule at `third_party/ratelimit`. Clone with
`--recurse-submodules`, or run `git submodule update --init --recursive` in an
existing checkout to get the pinned revision.

```sh
uv sync --locked          # once: create .venv from uv.lock
make test                 # quick: config unit test, wire codec, 28 C++ smoke scenarios
make test-parity          # builds the Go reference; byte-compares every response,
                          # gRPC error and final Redis state (28 scenarios, 220 comparisons)
make test-all             # every suite except Docker backend modes and sanitizers
make test-parity PYTEST_ARGS='-k metadata -v'   # one scenario
```

`make help` lists the individual suites (`test-load`, `test-integration`,
`test-go-reference-vectors`, `test-redis-pool`, `test-dpo-client`), the
Docker-based `test-backend-modes`, and the sanitizer builds.

The suites are pytest modules (`tests/test_*.py`, one test per scenario), and
`--generated` points at the protobuf modules `make python-protos` writes to
`build/python`; they are not Python dependencies. pytest can be run directly
after building; tests whose binaries or files are missing are skipped with the
reason:

```sh
uv run --locked pytest -v                                    # C++ only
uv run --locked pytest --go=build/ratelimit-go -v            # plus C++/Go parity and integration
uv run --locked pytest tests/test_parity.py --go=build/ratelimit-go -k metadata -v
uv run --locked pytest --go=build/ratelimit-go --docker-backends -k backend_mode   # Cluster/Sentinel
```

Write these options as `--option=value` (for example
`--dpo-root=../smartnic-generic-rpc`): pytest reads command-line paths before
it loads the options from `tests/conftest.py`, so a separate value outside the
repository would be taken as a test path.

Comparisons are byte-exact except for two formatting choices Go's protobuf
library makes per binary: the whitespace in the protobuf text of
`dynamic_metadata` descriptor limits, and an ASCII space or U+00A0 after
`proto:` in error details. `tests/parity_suite/comparison.py` handles only
those two, so the result does not depend on how the Go reference was built.

The checked-in `proto/reference.pb` pins the exact API descriptors imported by
the Go reference. `scripts/vendor_protos.py` creates the C++ schemas from that
descriptor set. It is a maintenance operation, not part of the normal build.
