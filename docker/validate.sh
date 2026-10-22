#!/usr/bin/env bash
# Run only through Docker: it deliberately does not use a host binary, host
# Redis, host Python modules, or host-generated protobuf files.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_dir=$(cd -- "$script_dir/.." && pwd)
compose=(docker compose -f "$project_dir/docker-compose.validation.yml")

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
"${compose[@]}" build --no-cache
"${compose[@]}" up -d --wait redis-cpp redis-go ratelimit-cpp ratelimit-go

# The client is a third container on the Compose network.  It makes ordinary,
# sequential, multi-descriptor, error, and malformed-wire calls over gRPC and
# verifies the resulting live Redis state.
"${compose[@]}" run --rm client --mode single \
  --target ratelimit-cpp:8081 --redis redis-cpp:6379

# Keep counters in Redis, stop the service with Docker's normal SIGTERM path,
# require a clean exit, then start it again and make real gRPC requests against
# the preserved quota state.
cpp_container=$("${compose[@]}" ps -q ratelimit-cpp)
"${compose[@]}" kill -s SIGTERM ratelimit-cpp
for _ in $(seq 1 50); do
  state=$(docker inspect -f '{{.State.Status}}' "$cpp_container")
  [ "$state" = "exited" ] && break
  sleep 0.1
done
[ "$(docker inspect -f '{{.State.Status}}' "$cpp_container")" = "exited" ]
[ "$(docker inspect -f '{{.State.ExitCode}}' "$cpp_container")" = "0" ]
[ "$(docker inspect -f '{{.State.OOMKilled}}' "$cpp_container")" = "false" ]
"${compose[@]}" up -d --wait ratelimit-cpp
"${compose[@]}" run --rm client --mode persistence \
  --target ratelimit-cpp:8081 --redis redis-cpp:6379

# The comparator starts both implementations from fresh, equivalent Redis
# state.  Every deterministic protobuf byte, gRPC error, response metadata,
# descriptor order, header field, Redis value, and TTL must match exactly.
"${compose[@]}" run --rm client --mode compare \
  --cpp-target ratelimit-cpp:8081 --go-target ratelimit-go:8081 \
  --cpp-redis redis-cpp:6379 --go-redis redis-go:6379

# Application logs are retained in command output for review and hard-fail on
# process failures.  Redis emits its normal startup notices separately, so only
# the two application logs are checked here.
"${compose[@]}" logs --no-color ratelimit-cpp ratelimit-go
if "${compose[@]}" logs --no-color ratelimit-cpp ratelimit-go | grep -Eqi 'panic|fatal|segmentation fault|core dumped|crash|retrying'; then
  echo 'unexpected application failure text in Docker logs' >&2
  exit 1
fi

echo 'Docker validation passed: clean build, external gRPC client, Redis, restart, Go/C++ byte parity, and application log review.'
