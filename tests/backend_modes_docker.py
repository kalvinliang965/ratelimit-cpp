#!/usr/bin/env python3
"""Black-box Redis Cluster and Sentinel parity validation.

This test deliberately uses Docker only for Redis fixtures.  The C++ and Go
rate-limit binaries run as independent host processes and are called over real
gRPC sockets.  Each implementation gets its own equivalent Cluster or
Sentinel-backed Redis instance, receives simultaneous requests, and must
produce byte-identical protobuf replies and identical Redis key/value/TTL
snapshots.  No application internals are imported or called.

The topology and authentication baseline comes from the upstream Go integration
fixtures: TestBasicAuthConfigWithRedisCluster and
TestBasicAuthConfigWithRedisSentinel in ratelimit/test/integration/
integration_test.go, plus the Redis setup in ratelimit/Makefile tests_with_redis.
This probe covers their non-per-second, no-local-cache path while adding a
black-box C++/Go response and Redis-state comparison.
"""

import argparse
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import grpc
from google.protobuf import json_format


METHOD = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit"
REDIS_PASSWORD = "password123"


def free_port():
    """Reserve a currently-unused local TCP port long enough to select it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def run(command, *, timeout=60, cwd=None):
    result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError("command failed (%d): %s\n%s" %
                           (result.returncode, " ".join(map(str, command)), result.stdout))
    return result.stdout


def redis_command(port, *arguments, password=""):
    """Small RESP client, avoiding host redis-py/redis-cli dependencies."""
    with socket.create_connection(("127.0.0.1", int(port)), timeout=2) as connection:
        stream = connection.makefile("rb")

        def read():
            line = stream.readline()
            if not line:
                raise RuntimeError("Redis disconnected")
            kind, body = line[:1], line[1:-2]
            if kind == b"-":
                raise RuntimeError(body.decode(errors="replace"))
            if kind == b"+":
                return body.decode()
            if kind == b":":
                return int(body)
            if kind == b"$":
                size = int(body)
                if size < 0:
                    return None
                value = stream.read(size)
                if stream.read(2) != b"\r\n":
                    raise RuntimeError("malformed Redis bulk reply")
                return value.decode(errors="replace")
            if kind == b"*":
                size = int(body)
                if size < 0:
                    return None
                return [read() for _ in range(size)]
            raise RuntimeError("unknown Redis reply %r" % kind)

        def command(*values):
            encoded = [str(value).encode() for value in values]
            connection.sendall(
                b"*%d\r\n" % len(encoded) + b"".join(
                    b"$%d\r\n" % len(value) + value + b"\r\n" for value in encoded))
            return read()

        if password and command("AUTH", password) != "OK":
            raise RuntimeError("Redis AUTH failed")
        return command(*arguments)


def wait_for(predicate, description, timeout=15):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
            last = result
        except (OSError, RuntimeError, grpc.RpcError) as error:
            last = error
        time.sleep(0.05)
    raise AssertionError("timed out waiting for %s: %r" % (description, last))


def clean_environment():
    prefixes = (
        "BACKEND_", "CACHE_", "CONFIG_", "DEBUG_", "DISABLE_", "DOG_STATSD_",
        "EXPIRATION_", "FORCE_", "GOSTATS_", "GRPC_", "HOST", "LIMIT_", "LOCAL_",
        "LOG_", "MEMCACHE_", "MERGE_", "NEAR_", "PORT", "QUOTA_", "REDIS_",
        "RUNTIME_", "SHADOW_", "STATSD_", "STOP_", "TRACING_", "USE_",
    )
    return {key: value for key, value in os.environ.items()
            if not key.startswith(prefixes)}


def metadata(values):
    if values is None:
        return ()
    return tuple((key, value if isinstance(value, str) else base64.b64encode(value).decode())
                 for key, value in values)


def decode_pinned_result(value):
    """Restore the JSON-safe pinned-client result to the local representation."""
    output = dict(value)
    for field in ("initial_metadata", "trailing_metadata"):
        output[field] = tuple(tuple(item) for item in output.get(field, ()))
    if output.get("kind") == "response":
        output["payload"] = base64.b64decode(output["payload"], validate=True)
    return output


def pinned_pair_call(args, services, request, name, compression=""):
    """Strictly compare one paired RPC inside the pinned grpcio Docker client."""
    encoded = base64.b64encode(request.SerializeToString(deterministic=True)).decode()
    command = [
        "docker", "run", "--rm", "--network", "host",
        "--label", "ratelimit.backend-mode-validation=1",
        "--entrypoint", "python", args.client_image, "/client/backend_modes_client.py",
        "--cpp-target", "127.0.0.1:%d" % services[0].grpc_port,
        "--go-target", "127.0.0.1:%d" % services[1].grpc_port,
        "--request-base64", encoded,
    ]
    if compression:
        command.extend(["--compression", compression])
    completed = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=45)
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError("%s pinned gRPC client did not return JSON (exit %d):\n%s" %
                             (name, completed.returncode, completed.stdout)) from error
    if result.get("client_grpcio") != "1.62.3":
        raise AssertionError("%s used unexpected pinned client grpcio %r" %
                             (name, result.get("client_grpcio")))
    values = [decode_pinned_result(result.get("cpp", {})),
              decode_pinned_result(result.get("go", {}))]
    # Keep a local exact comparison too: the helper has already made this
    # comparison in grpcio 1.62.3, and this guards its wire protocol.
    if not result.get("equal") or values[0] != values[1]:
        raise AssertionError("%s gRPC result differs in pinned grpcio 1.62.3 client\nC++: %s\nGo:  %s" %
                             (name, json.dumps(describe(values[0]), sort_keys=True),
                              json.dumps(describe(values[1]), sort_keys=True)))
    if completed.returncode:
        raise AssertionError("%s pinned client failed despite equal result:\n%s" %
                             (name, completed.stdout))
    return values


class DockerFixture:
    """Owns short-lived, host-networked Redis containers for one test side."""

    def __init__(self, name, directory):
        self.name = name
        self.directory = Path(directory)
        self.containers = []

    def start(self, suffix, image_arguments, mounts=()):
        container = "%s-%s" % (self.name, suffix)
        command = ["docker", "run", "--detach", "--rm", "--name", container,
                   "--label", "ratelimit.backend-mode-validation=1", "--network", "host"]
        for source, target in mounts:
            command.extend(["--volume", "%s:%s" % (source, target)])
        command.extend(["redis:7-alpine", *image_arguments])
        run(command, timeout=30)
        self.containers.append(container)
        return container

    def exec(self, container, *arguments, timeout=45):
        return run(["docker", "exec", container, *map(str, arguments)], timeout=timeout)

    def logs(self):
        output = []
        for container in self.containers:
            result = subprocess.run(["docker", "logs", container], text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output.append("[%s]\n%s" % (container, result.stdout))
        return "\n".join(output)

    def close(self):
        for container in reversed(self.containers):
            # --rm means this is only test-owned state.  Ignore a container
            # that has already exited and been removed by Docker.
            subprocess.run(["docker", "rm", "--force", container], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=False)
        self.containers.clear()


class ClusterFixture(DockerFixture):
    """A three-primary Redis Cluster reachable from the host through host networking."""

    def __init__(self, name, directory):
        super().__init__(name, directory)
        self.ports = []

    def start_cluster(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        # Keep ordinary Redis and the cluster-bus port apart.  Host networking
        # lets cluster nodes announce 127.0.0.1 while still reaching peers.
        while len(self.ports) < 3:
            candidate = free_port()
            if candidate + 10000 < 65535 and candidate not in self.ports:
                self.ports.append(candidate)
        nodes = []
        for index, port in enumerate(self.ports):
            state = self.directory / ("node-%d" % index)
            state.mkdir()
            state.chmod(0o777)
            node = self.start(
                "node-%d" % index,
                ["redis-server", "--bind", "127.0.0.1", "--port", str(port),
                 "--protected-mode", "no", "--save", "", "--appendonly", "no",
                 "--requirepass", REDIS_PASSWORD,
                 "--loglevel", "warning", "--cluster-enabled", "yes",
                 "--cluster-config-file", "/data/nodes.conf", "--cluster-node-timeout", "5000",
                 "--cluster-announce-ip", "127.0.0.1", "--cluster-announce-port", str(port),
                 "--cluster-announce-bus-port", str(port + 10000)],
                [(state, "/data")])
            nodes.append(node)
            wait_for(lambda port=port: redis_command(port, "PING", password=REDIS_PASSWORD) == "PONG",
                     "Redis Cluster node %d PING" % index)
        endpoints = ["127.0.0.1:%d" % port for port in self.ports]
        self.exec(nodes[0], "redis-cli", "--cluster", "create", "-a", REDIS_PASSWORD, *endpoints,
                  "--cluster-replicas", "0", "--cluster-yes", timeout=60)
        for port in self.ports:
            wait_for(lambda port=port: "cluster_state:ok" in redis_command(
                port, "CLUSTER", "INFO", password=REDIS_PASSWORD),
                     "Redis Cluster state")
        return ",".join(endpoints)

    def snapshot(self):
        values = {}
        for port in self.ports:
            for key in sorted(redis_command(port, "KEYS", "*", password=REDIS_PASSWORD)):
                if key in values:
                    raise AssertionError("duplicate Cluster key in snapshot: %s" % key)
                values[key] = (redis_command(port, "GET", key, password=REDIS_PASSWORD),
                               redis_command(port, "TTL", key, password=REDIS_PASSWORD))
        return dict(sorted(values.items()))


class SentinelFixture(DockerFixture):
    """Authenticated primary/replica Redis with three quorum-two Sentinels."""

    def __init__(self, name, directory):
        super().__init__(name, directory)
        self.master_port = None
        self.replica_port = None
        self.sentinel_ports = []

    def start_sentinel(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.master_port = free_port()
        master = self.start(
            "master",
            ["redis-server", "--bind", "127.0.0.1", "--port", str(self.master_port),
             "--protected-mode", "no", "--save", "", "--appendonly", "no", "--loglevel", "warning",
             "--requirepass", REDIS_PASSWORD])
        wait_for(lambda: redis_command(self.master_port, "PING", password=REDIS_PASSWORD) == "PONG",
                 "Sentinel master PING")
        self.replica_port = free_port()
        self.start(
            "replica",
            ["redis-server", "--bind", "127.0.0.1", "--port", str(self.replica_port),
             "--protected-mode", "no", "--save", "", "--appendonly", "no", "--loglevel", "warning",
             "--requirepass", REDIS_PASSWORD, "--masterauth", REDIS_PASSWORD,
             "--replicaof", "127.0.0.1", str(self.master_port)])
        wait_for(lambda: redis_command(self.replica_port, "PING", password=REDIS_PASSWORD) == "PONG",
                 "Sentinel replica PING")
        wait_for(lambda: "master_link_status:up" in redis_command(
            self.replica_port, "INFO", "replication", password=REDIS_PASSWORD),
                 "Sentinel replica replication")
        for index in range(3):
            port = free_port()
            self.sentinel_ports.append(port)
            state = self.directory / ("sentinel-%d" % index)
            state.mkdir()
            state.chmod(0o777)
            (state / "sentinel.conf").write_text(
                "port %d\n"
                "bind 127.0.0.1\n"
                "protected-mode no\n"
                "dir /data\n"
                "logfile \"\"\n"
                "sentinel monitor mymaster 127.0.0.1 %d 2\n"
                "sentinel auth-pass mymaster %s\n"
                "sentinel down-after-milliseconds mymaster 1000\n"
                "sentinel failover-timeout mymaster 5000\n"
                "sentinel parallel-syncs mymaster 1\n" % (port, self.master_port, REDIS_PASSWORD))
            self.start("sentinel-%d" % index,
                       ["redis-server", "/data/sentinel.conf", "--sentinel"], [(state, "/data")])
            wait_for(lambda port=port: redis_command(port, "PING") == "PONG",
                     "Redis Sentinel %d PING" % index)
            wait_for(lambda port=port: redis_command(port, "SENTINEL", "get-master-addr-by-name", "mymaster") ==
                     ["127.0.0.1", str(self.master_port)], "Sentinel master discovery")
        return "mymaster," + ",".join("127.0.0.1:%d" % port for port in self.sentinel_ports)

    def snapshot(self):
        values = {}
        for key in sorted(redis_command(self.master_port, "KEYS", "*", password=REDIS_PASSWORD)):
            values[key] = (redis_command(self.master_port, "GET", key, password=REDIS_PASSWORD),
                           redis_command(self.master_port, "TTL", key, password=REDIS_PASSWORD))
        return values


class Service:
    def __init__(self, label, binary, directory, backend_type, backend_url, policy):
        self.label = label
        self.binary = str(Path(binary).resolve())
        self.directory = Path(directory)
        self.backend_type = backend_type
        self.backend_url = backend_url
        self.grpc_port = free_port()
        self.http_port = free_port()
        self.debug_port = free_port()
        self.process = self.output = self.channel = self.rpc = None
        config_directory = self.directory / "runtime" / "ratelimit" / "config"
        config_directory.mkdir(parents=True)
        (config_directory / "policy.yaml").write_text(json.dumps(policy))

    def start(self):
        environment = clean_environment()
        environment.update({
            "BACKEND_TYPE": "redis", "CACHE_KEY_PREFIX": "", "CONFIG_TYPE": "FILE",
            "DEBUG_HOST": "127.0.0.1", "DEBUG_PORT": str(self.debug_port),
            "DISABLE_STATS": "true", "EXPIRATION_JITTER_MAX_SECONDS": "0",
            "GRPC_HOST": "127.0.0.1", "GRPC_PORT": str(self.grpc_port),
            "HOST": "127.0.0.1", "LOCAL_CACHE_SIZE_IN_BYTES": "0", "LOG_LEVEL": "error",
            "PORT": str(self.http_port), "REDIS_CLUSTER_PIPELINE_PARALLELISM": "1",
            "REDIS_AUTH": REDIS_PASSWORD, "REDIS_POOL_SIZE": "10", "REDIS_SOCKET_TYPE": "tcp",
            "REDIS_STARTUP_INITIAL_INTERVAL": "50ms", "REDIS_STARTUP_MAX_INTERVAL": "100ms",
            "REDIS_STARTUP_MAX_ELAPSED_TIME": "8s", "REDIS_TIMEOUT": "1s",
            "REDIS_TYPE": self.backend_type, "REDIS_URL": self.backend_url,
            "RUNTIME_APPDIRECTORY": "config", "RUNTIME_IGNOREDOTFILES": "true",
            "RUNTIME_ROOT": str(self.directory / "runtime"), "RUNTIME_SUBDIRECTORY": "ratelimit",
            "RUNTIME_WATCH_ROOT": "false", "USE_DOG_STATSD": "false",
            "USE_PROMETHEUS": "false", "USE_STATSD": "false",
        })
        if self.backend_type == "CLUSTER":
            # Matches configRedisCluster in the upstream Go integration suite.
            environment["REDIS_PIPELINE_WINDOW"] = "150us"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.output = open(self.directory / "server.log", "wb")
        self.process = subprocess.Popen([self.binary], env=environment, stdout=self.output,
                                        stderr=subprocess.STDOUT)

        def ready():
            if self.process.poll() is not None:
                raise AssertionError("%s exited with %s:\n%s" %
                                     (self.label, self.process.returncode, self.log()))
            try:
                with socket.create_connection(("127.0.0.1", self.grpc_port), timeout=0.2):
                    return True
            except OSError:
                return False

        wait_for(ready, "%s gRPC listener" % self.label, timeout=15)
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % self.grpc_port)
        grpc.channel_ready_future(self.channel).result(timeout=10)
        self.rpc = self.channel.unary_unary(
            METHOD,
            request_serializer=lambda message: message.SerializeToString(deterministic=True),
            response_deserializer=rls.RateLimitResponse.FromString)

    def call(self, request):
        try:
            response, call = self.rpc.with_call(request, timeout=8)
            return {
                "kind": "response",
                "payload": response.SerializeToString(deterministic=True),
                "initial_metadata": metadata(call.initial_metadata()),
                "trailing_metadata": metadata(call.trailing_metadata()),
            }
        except grpc.RpcError as error:
            return {
                "kind": "error", "code": error.code().name, "details": error.details(),
                "initial_metadata": metadata(error.initial_metadata()),
                "trailing_metadata": metadata(error.trailing_metadata()),
            }

    def log(self):
        path = self.directory / "server.log"
        return path.read_text(errors="replace") if path.exists() else ""

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.output:
            self.output.close()
            self.output = None


def policy_document():
    return {
        "domain": "backend", "descriptors": [
            {"key": "user", "rate_limit": {"unit": "minute", "requests_per_unit": 3}},
            {"key": "service", "value": "api", "descriptors": [
                {"key": "user", "rate_limit": {"unit": "minute", "requests_per_unit": 2}}]},
            {"key": "zero", "rate_limit": {"unit": "minute", "requests_per_unit": 0}},
        ],
    }


def make_request(*descriptor_entries, hits=0):
    request = rls.RateLimitRequest(domain="backend", hits_addend=hits)
    for entries in descriptor_entries:
        descriptor = request.descriptors.add()
        for key, value in entries:
            descriptor.entries.add(key=key, value=value)
    return request


def describe(result):
    if result["kind"] == "error":
        return result
    response = rls.RateLimitResponse.FromString(result["payload"])
    return {
        "response": json_format.MessageToDict(response, preserving_proto_field_name=True),
        "wire_base64": base64.b64encode(result["payload"]).decode(),
        "initial_metadata": result["initial_metadata"],
        "trailing_metadata": result["trailing_metadata"],
    }


def begin_second():
    """Start a paired RPC early in a second; results are still compared raw."""
    fraction = time.time() % 1
    if fraction > 0.08:
        time.sleep(1.015 - fraction)


def pair_call(args, services, request, name):
    begin_second()
    values = pinned_pair_call(args, services, request, name)
    if values[0]["kind"] != "response":
        raise AssertionError("%s unexpectedly failed: %s" % (name, describe(values[0])))
    return rls.RateLimitResponse.FromString(values[0]["payload"])


def pair_compressed_call(args, services, request, name):
    """Verify the real Go binary's gzip behavior exactly matches C++."""
    values = pinned_pair_call(args, services, request, name, compression="gzip")
    result = values[0]
    if result["kind"] != "response":
        raise AssertionError("%s unexpectedly failed: %s" % (name, describe(result)))
    response = rls.RateLimitResponse.FromString(result["payload"])
    assert_response(response, rls.RateLimitResponse.OK, [3], [2], name)
    print("PASS %s: exact accepted gzip response and metadata" % name, flush=True)


def assert_response(response, overall, limits, remaining, label):
    assert response.overall_code == overall, (label, response)
    assert [status.current_limit.requests_per_unit for status in response.statuses] == limits, (label, response)
    assert [status.limit_remaining for status in response.statuses] == remaining, (label, response)


def pair_snapshot(fixtures, label):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        values = list(executor.map(lambda fixture: fixture.snapshot(), fixtures))
    if values[0] != values[1]:
        raise AssertionError("%s Redis state differs\nC++: %r\nGo:  %r" %
                             (label, values[0], values[1]))
    return values[0]


def host_metadata_diagnostic(services, mode):
    """Record grpcio 1.80's header presentation without using it for parity."""
    probe = make_request((("host-metadata-probe", "x"),))
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        values = list(executor.map(lambda service: service.call(probe), services))
    print("DIAGNOSTIC ONLY host grpcio %s metadata presentation (%s): C++=%s Go=%s" %
          (grpc.__version__, mode, json.dumps(values[0]["initial_metadata"]),
           json.dumps(values[1]["initial_metadata"])), flush=True)


def run_mode(args, mode):
    fixture_type = ClusterFixture if mode == "cluster" else SentinelFixture
    directory = Path(tempfile.mkdtemp(prefix="ratelimit-backend-%s-" % mode))
    fixtures = []
    services = []
    passed = False
    try:
        urls = []
        for label in ("cpp", "go"):
            fixture = fixture_type("ratelimit-%s-%s-%d-%d" %
                                   (mode, label, os.getpid(), random.randrange(1 << 20)), directory / label / "redis")
            fixtures.append(fixture)
            urls.append(fixture.start_cluster() if mode == "cluster" else fixture.start_sentinel())
        for label, binary, url in zip(("cpp", "go"), (args.cpp, args.go), urls):
            service = Service(label, binary, directory / label / "service", mode.upper(), url,
                              policy_document())
            service.start()
            services.append(service)

        ok, over = rls.RateLimitResponse.OK, rls.RateLimitResponse.OVER_LIMIT
        first = pair_call(args, services, make_request((("user", "alice"),)), mode + " sequential first")
        assert_response(first, ok, [3], [2], mode + " sequential first")
        second = pair_call(args, services, make_request((("user", "alice"),)), mode + " sequential second")
        assert_response(second, ok, [3], [1], mode + " sequential second")
        multiple = pair_call(args, services,
                             make_request((("user", "bob"),),
                                          (("service", "api"), ("user", "carol"))),
                             mode + " ordered multi-descriptor")
        assert_response(multiple, ok, [3, 2], [2, 1], mode + " ordered multi-descriptor")
        over_limit = pair_call(args, services, make_request((("user", "alice"),), hits=2),
                               mode + " hits-addend over-limit")
        assert_response(over_limit, over, [3], [0], mode + " hits-addend over-limit")
        zero = pair_call(args, services, make_request((("zero", "x"),)), mode + " zero limit")
        assert_response(zero, over, [0], [0], mode + " zero limit")
        pair_compressed_call(args, services, make_request((("user", "gzip-probe"),)),
                             mode + " gzip request")

        snapshot = pair_snapshot(fixtures, mode + " exact snapshot")
        assert len(snapshot) == 5, (mode, snapshot)
        assert sorted(value for value, _ in snapshot.values()) == ["1", "1", "1", "1", "4"], snapshot
        assert all(ttl > 0 for _, ttl in snapshot.values()), snapshot
        host_metadata_diagnostic(services, mode)
        passed = True
        print("PASS redis-%s: exact gRPC protobuf/metadata and Redis key/value/TTL parity" % mode,
              flush=True)
    finally:
        for service in reversed(services):
            service.close()
        for fixture in reversed(fixtures):
            fixture.close()
        if passed and not args.keep_logs:
            shutil.rmtree(directory, ignore_errors=True)
        else:
            print("backend %s logs retained at %s" % (mode, directory), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", required=True)
    parser.add_argument("--go", required=True)
    parser.add_argument("--generated", default="build/python")
    parser.add_argument("--filter", choices=("cluster", "sentinel", ""), default="")
    parser.add_argument("--keep-logs", action="store_true")
    parser.add_argument("--client-image", default=os.environ.get(
        "BACKEND_MODES_CLIENT_IMAGE", "ratelimit-cpp-validation-client:latest"),
        help="pinned grpcio client image used for strict gRPC metadata comparison")
    args = parser.parse_args()
    args.cpp, args.go = str(Path(args.cpp).resolve()), str(Path(args.go).resolve())
    for binary in (args.cpp, args.go):
        if not os.access(binary, os.X_OK):
            parser.error("not an executable: %s" % binary)
    if not shutil.which("docker"):
        parser.error("docker is required for Redis Cluster/Sentinel fixtures")
    sys.path.insert(0, str(Path(args.generated).resolve()))
    global rls
    from envoy.service.ratelimit.v3 import rls_pb2 as rls
    modes = [args.filter] if args.filter else ["cluster", "sentinel"]
    for mode in modes:
        run_mode(args, mode)
    print("backend mode validation: %d real Docker Redis backend modes passed" % len(modes), flush=True)


if __name__ == "__main__":
    main()
