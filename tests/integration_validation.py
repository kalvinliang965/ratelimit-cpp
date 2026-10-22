#!/usr/bin/env python3
"""Black-box C++/Go integration validation using real gRPC and Redis processes.

The two implementations use isolated Redis instances, receive the same policy
and gRPC requests, and are compared without response-field normalization:
successful replies use deterministic protobuf bytes, failures use the exact
gRPC (code, details) pair, and Redis snapshots include keys, values, and TTLs.
"""

import argparse
import concurrent.futures
import contextlib
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import queue
import re
import shutil
import select
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time

import grpc


TEST_DIRECTORY = Path(__file__).resolve().parent
WORKSPACE = TEST_DIRECTORY.parents[1]
GO_STATSD_VECTORS = TEST_DIRECTORY / "go_statsd_reference_vectors.json"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def redis_command(port, *args):
    """Minimal RESP client; keep the test independent of redis-py."""
    with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
        values = [str(value).encode() for value in args]
        sock.sendall(b"*%d\r\n" % len(values) + b"".join(
            b"$%d\r\n" % len(value) + value + b"\r\n" for value in values))
        stream = sock.makefile("rb")

        def read():
            line = stream.readline()
            if not line:
                raise RuntimeError("Redis disconnected")
            kind, value = line[:1], line[1:-2]
            if kind == b"-":
                raise RuntimeError(value.decode())
            if kind == b":":
                return int(value)
            if kind == b"$":
                length = int(value)
                if length < 0:
                    return None
                body = stream.read(length)
                stream.read(2)
                return body.decode()
            if kind == b"*":
                return [read() for _ in range(int(value))]
            return value.decode()

        return read()


def wait_for(predicate, description, timeout=10):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
            last = value
        except (OSError, RuntimeError, grpc.RpcError) as error:
            last = error
        time.sleep(0.05)
    raise AssertionError("timed out waiting for %s: %r" % (description, last))


def process_socket_inodes(pid):
    """Return kernel socket inodes held by one Linux process."""
    fd_directory = Path("/proc") / str(pid) / "fd"
    if not fd_directory.is_dir():
        raise AssertionError("Linux /proc fd inspection is required for the TLS proxy test")
    inodes = set()
    for descriptor in fd_directory.iterdir():
        try:
            target = os.readlink(descriptor)
        except OSError:
            # A worker can close an unrelated descriptor while the directory
            # is scanned. It cannot affect the persistent listener FDs.
            continue
        match = re.fullmatch(r"socket:\[(\d+)\]", target)
        if match:
            inodes.add(match.group(1))
    return inodes


def process_listener_ports(pid):
    """Return TCP LISTEN ports owned by one Linux process."""
    inodes = process_socket_inodes(pid)
    ports = set()
    for table in ("tcp", "tcp6"):
        try:
            lines = (Path("/proc") / str(pid) / "net" / table).read_text().splitlines()[1:]
        except OSError as error:
            raise AssertionError("cannot inspect %s listeners: %s" % (table, error))
        for line in lines:
            fields = line.split()
            # /proc/net/tcp: local_address is field 1, TCP_LISTEN is 0A in
            # field 3, and the socket inode is field 9.
            if len(fields) < 10 or fields[3] != "0A" or fields[9] not in inodes:
                continue
            ports.add(int(fields[1].rsplit(":", 1)[1], 16))
    return ports


def private_unix_listener_paths(pid):
    """Return pathname Unix listener sockets held by one Linux process."""
    inodes = process_socket_inodes(pid)
    try:
        lines = (Path("/proc") / str(pid) / "net" / "unix").read_text().splitlines()[1:]
    except OSError as error:
        raise AssertionError("cannot inspect Unix listeners: %s" % error)
    paths = []
    for line in lines:
        fields = line.split(maxsplit=7)
        # /proc/net/unix gives the inode in field 6 and a pathname only for
        # filesystem sockets. Match the process's actual FDs, rather than all
        # sockets in the shared network namespace.
        if len(fields) == 8 and fields[6] in inodes:
            paths.append(Path(fields[7]))
    return paths


def assert_private_tls_proxy_upstream(service):
    """Verify the C++ SAN proxy has no plaintext TCP upstream."""
    expected_tcp = {service.http_port, service.debug_port}
    if not service.grpc_uds:
        expected_tcp.add(service.grpc_port)
    try:
        process_listener_ports(service.process.pid)
    except PermissionError:
        # Some hardened runners deny /proc/<pid>/fd inspection even to the
        # parent test process. The socket namespace remains observable through
        # its intentionally private on-disk directory, so retain permission
        # and cleanup coverage there. Ordinary Linux runs also assert the
        # complete TCP listener set below.
        candidates = [path / "grpc.sock"
                      for path in Path(tempfile.gettempdir()).glob("ratelimit-cpp-tls-proxy-*")
                      if (path / "grpc.sock").exists()]
    else:
        wait_for(lambda: expected_tcp.issubset(process_listener_ports(service.process.pid)),
                 "C++ TLS proxy public listeners")
        listeners = process_listener_ports(service.process.pid)
        assert listeners == expected_tcp, (
            "C++ TLS proxy exposed unexpected TCP listener(s): %r (expected %r)" %
            (listeners, expected_tcp))
        candidates = [path for path in private_unix_listener_paths(service.process.pid)
                      if path.name == "grpc.sock" and
                      path.parent.name.startswith("ratelimit-cpp-tls-proxy-")]
    assert len(candidates) == 1, "expected one private TLS proxy Unix listener, got %r" % candidates
    upstream = candidates[0]
    socket_mode = stat.S_IMODE(os.lstat(upstream).st_mode)
    directory_mode = stat.S_IMODE(os.lstat(upstream.parent).st_mode)
    assert stat.S_ISSOCK(os.lstat(upstream).st_mode), "proxy upstream is not a Unix socket: %s" % upstream
    assert socket_mode == 0o600, "proxy Unix socket mode is %o, expected 600" % socket_mode
    assert directory_mode == 0o700, "proxy Unix socket directory mode is %o, expected 700" % directory_mode
    return upstream


def clean_environment():
    prefixes = ("REDIS_", "RUNTIME_", "LIMIT_", "GRPC_", "CONFIG_", "MEMCACHE_",
                "STATSD_", "TRACING_", "PROMETHEUS_", "SHADOW_", "QUOTA_", "DOG_STATSD_",
                "GOSTATS_", "EXTRA_")
    return {key: value for key, value in os.environ.items()
            if not key.startswith(prefixes)}


def policy(*descriptors, domain="integration"):
    return {"domain": domain, "descriptors": list(descriptors)}


def rule(key, requests, unit="minute", value=None):
    result = {"key": key, "rate_limit": {"unit": unit, "requests_per_unit": requests}}
    if value is not None:
        result["value"] = value
    return result


class RedisProcess:
    def __init__(self, directory, port):
        self.directory, self.port = Path(directory), port
        self.process = self.output = None

    def start(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.output = open(self.directory / "redis.log", "wb")
        self.process = subprocess.Popen(
            ["redis-server", "--bind", "127.0.0.1", "--port", str(self.port),
             "--save", "", "--appendonly", "no", "--loglevel", "warning"],
            stdout=self.output, stderr=subprocess.STDOUT)
        wait_for(lambda: redis_command(self.port, "PING") == "PONG", "Redis PING")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.output:
            self.output.close()
            self.output = None


class StatsdCapture:
    """A tiny real TCP StatsD receiver used to inspect emitted counter wire data."""
    def __init__(self):
        self.port = free_port()
        self.listener = self.thread = None
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.counters = {}

    def start(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", self.port))
        self.listener.listen()
        self.listener.setblocking(False)
        self.thread = threading.Thread(target=self._receive, daemon=True)
        self.thread.start()

    def _record(self, line):
        try:
            name, payload = line.decode().split(":", 1)
            value, kind = payload.split("|", 1)
            if kind != "c":
                return
            with self.lock:
                self.counters[name] = self.counters.get(name, 0) + int(value)
        except (UnicodeDecodeError, ValueError):
            raise AssertionError("invalid StatsD counter line: %r" % line)

    def _receive(self):
        buffers, connections = {}, []
        while not self.stopping.is_set():
            try:
                readable, _, _ = select.select([self.listener] + connections, [], [], 0.1)
            except (OSError, ValueError):
                if self.stopping.is_set():
                    break
                raise
            for source in readable:
                if source is self.listener:
                    try:
                        connection, _ = self.listener.accept()
                    except OSError:
                        if self.stopping.is_set():
                            break
                        raise
                    connection.setblocking(False)
                    connections.append(connection)
                    buffers[connection] = b""
                    continue
                chunk = source.recv(65536)
                if not chunk:
                    remainder = buffers.pop(source, b"")
                    if remainder:
                        self._record(remainder)
                    connections.remove(source)
                    source.close()
                    continue
                buffers[source] += chunk
                while b"\n" in buffers[source]:
                    line, buffers[source] = buffers[source].split(b"\n", 1)
                    if line:
                        self._record(line)
        for connection in connections:
            connection.close()

    def snapshot(self):
        with self.lock:
            return dict(self.counters)

    def stop(self):
        self.stopping.set()
        if self.listener:
            self.listener.close()
        if self.thread:
            self.thread.join(timeout=2)


class ServiceProcess:
    def __init__(self, label, binary, directory, config, extra_env=None, create_config=True,
                 client_tls=None, use_grpc_uds=False):
        self.label, self.binary, self.directory = label, str(Path(binary).resolve()), Path(directory)
        self.config_dir = self.directory / "runtime" / "ratelimit" / "config"
        self.grpc_uds = self.directory / "ratelimit.grpc.sock" if use_grpc_uds else None
        self.grpc_port, self.http_port, self.debug_port, self.redis_port = (free_port() for _ in range(4))
        self.extra_env = dict(extra_env or {})
        self.client_tls = dict(client_tls or {})
        self.process = self.output = self.channel = self.rpc = None
        if create_config:
            self.config_dir.mkdir(parents=True)
            values = config if isinstance(config, list) else [config]
            for index, value in enumerate(values):
                pending = self.config_dir / (".policy%d.pending" % index)
                pending.write_text(json.dumps(value))
                pending.replace(self.config_dir / ("policy%d.yaml" % index))

    def environment(self, redis_port=None):
        env = clean_environment()
        env.update({
            "BACKEND_TYPE": "redis", "CACHE_KEY_PREFIX": "", "CONFIG_TYPE": "FILE",
            "DEBUG_HOST": "127.0.0.1", "DEBUG_PORT": str(self.debug_port),
            "DISABLE_STATS": "true", "EXPIRATION_JITTER_MAX_SECONDS": "0",
            "FORCE_START_WITHOUT_INITIAL_CONFIG": "false", "GRPC_HOST": "127.0.0.1",
            "GRPC_PORT": str(self.grpc_port), "HOST": "127.0.0.1",
            "LOCAL_CACHE_SIZE_IN_BYTES": "0", "LOG_LEVEL": "error", "PORT": str(self.http_port),
            "REDIS_SOCKET_TYPE": "tcp", "REDIS_STARTUP_INITIAL_INTERVAL": "100ms",
            "REDIS_STARTUP_MAX_INTERVAL": "100ms", "REDIS_STARTUP_MAX_ELAPSED_TIME": "3s",
            "REDIS_TIMEOUT": "500ms", "REDIS_TYPE": "SINGLE",
            "REDIS_URL": "127.0.0.1:%d" % (self.redis_port if redis_port is None else redis_port),
            "RUNTIME_APPDIRECTORY": "config", "RUNTIME_IGNOREDOTFILES": "true",
            "RUNTIME_ROOT": str(self.directory / "runtime"), "RUNTIME_SUBDIRECTORY": "ratelimit",
            "RUNTIME_WATCH_ROOT": "false", "USE_DOG_STATSD": "false", "USE_PROMETHEUS": "false",
            "USE_STATSD": "false",
        })
        env.update(self.extra_env)
        if self.grpc_uds:
            env["GRPC_UDS"] = str(self.grpc_uds)
        return env

    def launch(self, redis_port=None):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.output = open(self.directory / "server.log", "wb")
        self.process = subprocess.Popen([self.binary], env=self.environment(redis_port),
                                        stdout=self.output, stderr=subprocess.STDOUT)

    def start(self, redis_port=None):
        self.launch(redis_port)

        def ready():
            if self.process.poll() is not None:
                raise AssertionError("%s exited during startup with status %s\n%s" %
                                     (self.label, self.process.returncode, self.log()))
            if self.grpc_uds:
                try:
                    return stat.S_ISSOCK(os.lstat(self.grpc_uds).st_mode)
                except OSError:
                    return False
            try:
                with socket.create_connection(("127.0.0.1", self.grpc_port), timeout=0.2):
                    pass
                return True
            except OSError:
                return False

        wait_for(ready, "%s gRPC listener" % self.label, timeout=12)
        self.channel = self.make_channel(self.client_tls)
        self.rpc = self.channel.unary_unary(
            "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
            request_serializer=rls.RateLimitRequest.SerializeToString,
            response_deserializer=rls.RateLimitResponse.FromString)
        self.health_check = self.channel.unary_unary(
            "/grpc.health.v1.Health/Check",
            request_serializer=health.HealthCheckRequest.SerializeToString,
            response_deserializer=health.HealthCheckResponse.FromString)

    def make_channel(self, tls=None):
        tls = tls or {}
        endpoint = ("unix://" + str(self.grpc_uds) if self.grpc_uds
                    else "127.0.0.1:%d" % self.grpc_port)
        if not tls:
            return grpc.insecure_channel(endpoint)
        credentials = grpc.ssl_channel_credentials(
            root_certificates=tls["root_certificates"],
            private_key=tls.get("private_key"),
            certificate_chain=tls.get("certificate_chain"))
        options = ()
        if tls.get("server_name"):
            options = (("grpc.ssl_target_name_override", tls["server_name"]),)
        return grpc.secure_channel(endpoint, credentials, options=options)

    def call(self, request):
        try:
            return self.rpc(request, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())

    def call_with_tls(self, request, tls):
        """Invoke the real service with independently supplied TLS credentials."""
        channel = self.make_channel(tls)
        rpc = channel.unary_unary(
            "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
            request_serializer=rls.RateLimitRequest.SerializeToString,
            response_deserializer=rls.RateLimitResponse.FromString)
        try:
            return rpc(request, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())
        finally:
            channel.close()

    def check_health(self, service):
        try:
            return self.health_check(health.HealthCheckRequest(service=service), timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())

    def stop_service(self):
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

    def restart(self):
        self.stop_service()
        self.start()

    def stop(self):
        self.stop_service()

    def log(self):
        file = self.directory / "server.log"
        return file.read_text(errors="replace") if file.exists() else ""

    def redis_snapshot(self):
        keys = sorted(redis_command(self.redis_port, "KEYS", "*"))
        return {key: (redis_command(self.redis_port, "GET", key),
                      redis_command(self.redis_port, "TTL", key)) for key in keys}


def encoded(value):
    return repr(value).encode() if isinstance(value, tuple) else value.SerializeToString(deterministic=True)


def compare(values, label):
    if encoded(values[0]) != encoded(values[1]):
        raise AssertionError("%s differs\nC++: %r\nGo:  %r" % (label, values[0], values[1]))


def settle_second():
    # DurationUntilReset has whole-second precision. Do not compare a pair
    # emitted across a boundary, while preserving every emitted field.
    fraction = time.time() % 1
    if fraction > 0.65:
        time.sleep(1.04 - fraction)


class Pair:
    def __init__(self, args, directory, config, extra_env=None, create_config=True, client_tls=None,
                 use_grpc_uds=False):
        def environment_for(label):
            return extra_env(label) if callable(extra_env) else extra_env
        self.services = [
            ServiceProcess("cpp", args.cpp, directory / "cpp", config, environment_for("cpp"), create_config, client_tls, use_grpc_uds),
            ServiceProcess("go", args.go, directory / "go", config, environment_for("go"), create_config, client_tls, use_grpc_uds),
        ]
        self.redis = [RedisProcess(service.directory, service.redis_port) for service in self.services]

    def start(self):
        for process in self.redis:
            process.start()
        for service in self.services:
            service.start()

    def call(self, request, label):
        settle_second()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            values = list(executor.map(lambda service: service.call(request), self.services))
        compare(values, label)
        return values[0]

    def snapshot(self, label):
        # TTLs are intentionally exact: expiration jitter is disabled and
        # requests are paired, so a difference is evidence rather than noise.
        values = [service.redis_snapshot() for service in self.services]
        if values[0] != values[1]:
            raise AssertionError("%s Redis state differs\nC++: %r\nGo:  %r" %
                                 (label, values[0], values[1]))
        return values[0]

    def health(self, service, label):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            values = list(executor.map(lambda implementation: implementation.check_health(service), self.services))
        compare(values, label)
        return values[0]

    def stop_redis(self):
        for process in self.redis:
            process.stop()

    def restart_redis(self):
        for process in self.redis:
            process.start()

    def close(self):
        for service in reversed(self.services):
            service.stop()
        for process in reversed(self.redis):
            process.stop()


@contextlib.contextmanager
def paired_servers(args, name, config, extra_env=None, create_config=True, client_tls=None,
                   start=True, use_grpc_uds=False):
    directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-integration-%s-" % name))
    pair, passed = Pair(args, directory, config, extra_env, create_config, client_tls, use_grpc_uds), False
    try:
        if start:
            pair.start()
        yield pair
        passed = True
    finally:
        pair.close()
        if passed and not args.keep_logs:
            shutil.rmtree(directory)
        else:
            print("logs retained at %s" % directory, flush=True)


def make_request(domain, *descriptors, hits=1):
    return rls.RateLimitRequest(
        domain=domain,
        descriptors=[rate_limit.RateLimitDescriptor(entries=[
            rate_limit.RateLimitDescriptor.Entry(key=key, value=value) for key, value in entries])
            for entries in descriptors], hits_addend=hits)


def source_request(spec):
    """Build one neutral fixture request without importing Go internals."""
    return make_request(spec["domain"], tuple(tuple(entry) for entry in spec["entries"]),
                        hits=int(spec.get("hits_addend", 1)))


def stage_source_configs(pair, paths):
    """Stage checked-in Go configuration bytes into isolated runtime roots."""
    sources = []
    for relative in paths:
        source = (WORKSPACE / relative).resolve()
        if WORKSPACE not in source.parents or not source.is_file():
            raise AssertionError("source configuration is not a workspace file: %s" % relative)
        sources.append(source)
    for service in pair.services:
        service.config_dir.mkdir(parents=True, exist_ok=True)
        for number, source in enumerate(sources):
            shutil.copyfile(source, service.config_dir / ("%02d-" % number + source.name))


def statsd_environment(captures):
    def environment(label):
        return {
            "DISABLE_STATS": "false", "USE_STATSD": "true", "USE_DOG_STATSD": "false",
            "USE_PROMETHEUS": "false", "STATSD_HOST": "127.0.0.1",
            "STATSD_PORT": str(captures[label].port), "STATSD_PROTOCOL": "tcp",
            "STATS_FLUSH_INTERVAL": "50ms",
        }
    return environment


def expected_rate_metrics(metric_hits):
    prefix = "ratelimit.service.rate_limit."
    expected = {}
    for key, hits in metric_hits.items():
        # The source cases stay below each configured near-limit threshold, so
        # the externally emitted behavior is precisely total_hits + within_limit.
        expected[prefix + key + ".total_hits"] = hits
        expected[prefix + key + ".within_limit"] = hits
    return expected


def run_source_statsd_case(args, case):
    """Run one Go-source-mapped metric case through two real gRPC servers."""
    captures = {label: StatsdCapture() for label in ("cpp", "go")}
    source_files = case.get("config_files")
    expected = expected_rate_metrics(case["metric_hits"])
    try:
        for capture in captures.values():
            capture.start()
        # Inline Go config tests cannot drive either standalone binary
        # directly.  Their exact config/request/expected-key data lives in
        # go_statsd_reference_vectors.json and is passed unchanged to both.
        with paired_servers(args, "go-statsd-" + case["id"], case.get("policy"),
                            statsd_environment(captures),
                            create_config=source_files is None, start=False) as pair:
            if source_files:
                stage_source_configs(pair, source_files)
            pair.start()

            # The checked-in share-threshold fixture contains 23 requests.
            # Keep that whole source sequence within one minute so its exact
            # Redis stems have one deterministic bucket suffix.
            bucket = None
            if "expected_redis_stems" in case:
                remaining = 60 - (time.time() % 60)
                if remaining < 15:
                    time.sleep(remaining + 0.15)
                bucket = int(time.time() // 60) * 60

            for number, request_spec in enumerate(case["requests"], 1):
                response = pair.call(source_request(request_spec),
                                     "%s source request %d" % (case["id"], number))
                assert not isinstance(response, tuple), (
                    "%s request %d returned gRPC error %r" %
                    (case["id"], number, response))

            snapshot = pair.snapshot(case["id"] + " Redis state")
            if "expected_redis_stems" in case:
                assert bucket is not None
                expected_keys = {stem + "_" + str(bucket)
                                 for stem in case["expected_redis_stems"]}
                assert set(snapshot) == expected_keys, (
                    "%s Redis keys: %r != %r" % (case["id"], set(snapshot), expected_keys))
                assert {value for value, _ in snapshot.values()} == {"1", "2", "3"}

            def counters_arrived():
                return all(all(capture.snapshot().get(name) == value
                               for name, value in expected.items())
                           for capture in captures.values())

            wait_for(counters_arrived, case["id"] + " StatsD counters", timeout=10)
            prefix = "ratelimit.service.rate_limit."
            observed = {
                label: {name: value for name, value in capture.snapshot().items()
                        if name.startswith(prefix)}
                for label, capture in captures.items()
            }
            assert observed["cpp"] == expected, "%s C++ StatsD: %r" % (case["id"], observed["cpp"])
            assert observed["go"] == expected, "%s Go StatsD: %r" % (case["id"], observed["go"])
            assert observed["cpp"] == observed["go"], "%s StatsD parity: %r" % (case["id"], observed)
    finally:
        for capture in captures.values():
            capture.stop()


def test_statsd_go_source_metric_matrix(args):
    """Reuse Go metric source cases over real gRPC, Redis, and TCP StatsD."""
    document = json.loads(GO_STATSD_VECTORS.read_text())
    assert document.get("format") == "ratelimit-go-statsd-black-box-v1"
    for case in document["cases"]:
        run_source_statsd_case(args, case)


def next_utc_month_start(now):
    current = datetime.fromtimestamp(now, timezone.utc)
    if current.month == 12:
        return int(datetime(current.year + 1, 1, 1, tzinfo=timezone.utc).timestamp())
    return int(datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc).timestamp())


def utc_month_start(now):
    current = datetime.fromtimestamp(now, timezone.utc)
    return int(datetime(current.year, current.month, 1, tzinfo=timezone.utc).timestamp())


def safe_calendar_test_second(calendar_enabled):
    """Return a second safely inside both relevant public service windows."""
    divider = 30 * 24 * 60 * 60
    for _ in range(3):
        now = int(time.time())
        next_boundary = next_utc_month_start(now) if calendar_enabled else (now // divider + 1) * divider
        # No fixed-date clock injection exists in either public binary. Avoid
        # accepting a request close enough to a live boundary to be ambiguous.
        if next_boundary - now <= 8:
            time.sleep(next_boundary - now + 1.1)
            continue
        settle_second()
        now = int(time.time())
        next_boundary = next_utc_month_start(now) if calendar_enabled else (now // divider + 1) * divider
        if next_boundary - now > 6:
            return now
    raise AssertionError("could not reserve a safe calendar rate-limit second")


def test_calendar_month_current_window_keys(args):
    """Verify live UTC month and legacy 30-day Redis-key behavior exactly.

    This is the public-process counterpart of Go's injected-clock month tests.
    It intentionally uses the original basic_config.yaml policy rather than a
    recreated month limit, then verifies the actual Redis key and TTL from
    each binary without normalizing the C++/Go comparison.
    """
    divider = 30 * 24 * 60 * 60
    request = make_request("test-domain", (("key8", "month"),))
    for calendar_enabled in (True, False):
        environment = {"USE_CALENDAR_MONTH_RATE_LIMIT": str(calendar_enabled).lower()}
        label = "calendar-month-%s" % ("enabled" if calendar_enabled else "legacy")
        with paired_servers(args, label, None, environment, create_config=False, start=False) as pair:
            stage_source_configs(pair, ["ratelimit/test/config/basic_config.yaml"])
            pair.start()
            now = safe_calendar_test_second(calendar_enabled)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                responses = list(executor.map(lambda service: service.call(request), pair.services))
            compare(responses, label + " response")
            response = responses[0]
            assert not isinstance(response, tuple), "%s gRPC error: %r" % (label, response)
            status = response.statuses[0]
            assert status.current_limit.requests_per_unit == 200
            assert status.current_limit.unit == rls.RateLimitResponse.RateLimit.MONTH
            assert status.limit_remaining == 199

            if calendar_enabled:
                bucket = utc_month_start(now)
                expected_redis_ttl = next_utc_month_start(now) - now
                expected_reset = expected_redis_ttl
            else:
                bucket = now // divider * divider
                # Go's legacy mode stores a full fixed-window TTL but reports
                # only the remainder of that window in DurationUntilReset.
                expected_redis_ttl = divider
                expected_reset = divider - now % divider
            expected_key = "test-domain_key8_month_%d" % bucket
            snapshot = pair.snapshot(label + " Redis state")
            assert set(snapshot) == {expected_key}, "%s Redis key: %r" % (label, snapshot)
            value, ttl = snapshot[expected_key]
            assert value == "1", "%s Redis value: %r" % (label, value)
            # Redis TTL is sampled after the rate-limit response, so it can
            # trail its freshly computed integer value by one second.
            assert expected_redis_ttl - 2 <= ttl <= expected_redis_ttl, (
                "%s Redis TTL %r outside expected public window %r" %
                (label, ttl, expected_redis_ttl))
            assert expected_reset - 2 <= status.duration_until_reset.seconds <= expected_reset, (
                "%s reset %r outside expected public window %r" %
                (label, status.duration_until_reset.seconds, expected_reset))


def replace_runtime_policy(services, config):
    """Atomically replace a live FILE-provider policy in every runtime root."""
    for service in services:
        service.config_dir.mkdir(parents=True, exist_ok=True)
        pending = service.config_dir / ".integration-policy.pending"
        pending.write_text(json.dumps(config))
        pending.replace(service.config_dir / "policy.yaml")


def remove_runtime_policy(services):
    """Leave a real FILE provider with an empty configuration directory."""
    for service in services:
        target = service.config_dir / "policy.yaml"
        if target.exists():
            target.unlink()


def wait_for_health(pair, wanted, label):
    """Wait for both public health endpoints, then byte-compare their replies.

    FILE-provider events are asynchronous in both implementations. A valid
    transition may therefore be observed one polling interval earlier by one
    process; that is not a response normalization. Once both have reached the
    requested state, the final wire responses are still compared exactly.
    """
    last = []

    def is_wanted():
        nonlocal last
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            last = list(executor.map(lambda service: service.check_health("ratelimit"), pair.services))
        return all(not isinstance(result, tuple) and result.status == wanted for result in last)

    wait_for(is_wanted, label, timeout=12)
    result = pair.health("ratelimit", label + " final exact comparison")
    assert result.status == wanted, "%s: got %r" % (label, result)


def test_health_requires_loaded_config_transitions(args):
    """Recreate Go TestServiceHealthStatusAtLeastOneConfigLoaded over gRPC.

    The Go unit test feeds a mocked provider an empty configuration, a valid
    configuration, then an empty configuration. This version drives the same
    state sequence through real FILE providers: an initially empty runtime
    directory, an atomic valid policy add, and policy removal.
    """
    valid = policy(rule("gated", 2), domain="health-gate")
    environment = {"HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED": "true"}
    with paired_servers(args, "health-config-transitions", None, environment,
                        create_config=False, start=False) as pair:
        # Both binaries must see an existing-but-empty directory at initial
        # load. That represents Go's `IsEmptyDomains() == true` event.
        for service in pair.services:
            service.config_dir.mkdir(parents=True, exist_ok=True)
        pair.start()
        wait_for_health(pair, health.HealthCheckResponse.NOT_SERVING,
                        "health with no loaded configuration")

        replace_runtime_policy(pair.services, valid)
        wait_for_health(pair, health.HealthCheckResponse.SERVING,
                        "health after valid configuration reload")
        response = pair.call(make_request("health-gate", (("gated", "value"),)),
                             "health-gate rate-limit response")
        assert response.overall_code == rls.RateLimitResponse.OK
        assert response.statuses[0].limit_remaining == 1
        pair.snapshot("health-gate Redis state")

        # Match the invalid-reload behavior exercised by Go's integration
        # suite: a bad replacement must not unload the last known good policy
        # or make a loaded-config-gated service unhealthy.
        invalid = policy(rule("gated", 9, unit="not-a-unit"), domain="health-gate")
        replace_runtime_policy(pair.services, invalid)
        time.sleep(0.6)  # The FILE watchers poll asynchronously in both binaries.
        wait_for_health(pair, health.HealthCheckResponse.SERVING,
                        "health after invalid configuration reload")
        retained = pair.call(make_request("health-gate", (("gated", "value"),)),
                             "invalid reload retains last valid rate-limit policy")
        assert retained.overall_code == rls.RateLimitResponse.OK
        assert retained.statuses[0].current_limit.requests_per_unit == 2
        assert retained.statuses[0].limit_remaining == 0
        pair.snapshot("invalid health-gate reload retains Redis state")

        remove_runtime_policy(pair.services)
        wait_for_health(pair, health.HealthCheckResponse.NOT_SERVING,
                        "health after empty configuration reload")


def quota_descriptor(backend, model, bucket, hits):
    descriptor = rate_limit.RateLimitDescriptor(entries=[
        rate_limit.RateLimitDescriptor.Entry(key="backend_name", value=backend),
        rate_limit.RateLimitDescriptor.Entry(key="model_name_override", value=model),
        rate_limit.RateLimitDescriptor.Entry(key=bucket, value=bucket),
    ])
    descriptor.hits_addend.value = hits
    return descriptor


def test_quota_same_model_different_backends(args):
    """Exercise Go TestQuotaMetadataSameModelDifferentBackends on the wire."""
    def backend_rule(backend):
        return {"key": "backend_name", "value": backend, "descriptors": [{
            "key": "model_name_override", "value": "gpt-4o-mini", "descriptors": [{
                "key": "rule-0-match--1", "value": "rule-0-match--1", "quota_mode": True,
                "rate_limit": {"unit": "minute", "requests_per_unit": 100},
            }],
        }]}

    config = policy(backend_rule("ns/backend-a"), backend_rule("ns/backend-b"),
                    domain="quota-domain")
    request = rls.RateLimitRequest(
        domain="quota-domain", hits_addend=1,
        descriptors=[
            quota_descriptor("ns/backend-a", "gpt-4o-mini", "rule-0-match--1", 101),
            quota_descriptor("ns/backend-b", "gpt-4o-mini", "rule-0-match--1", 1),
        ])
    with paired_servers(args, "quota-same-model-different-backends", config, {
            "QUOTA_MODE": "true", "RESPONSE_DYNAMIC_METADATA": "true"}) as pair:
        response = pair.call(request, "same-model distinct-backend quota response")
        assert response.overall_code == rls.RateLimitResponse.OK
        assert [status.code for status in response.statuses] == [
            rls.RateLimitResponse.OVER_LIMIT, rls.RateLimitResponse.OK]
        assert [status.current_limit.requests_per_unit for status in response.statuses] == [100, 100]
        assert [status.limit_remaining for status in response.statuses] == [0, 99]
        passed = response.dynamic_metadata.fields["passedBackends"].list_value.values
        assert [(value.struct_value.fields["backend_name"].string_value,
                 value.struct_value.fields["model_name_override"].string_value)
                for value in passed] == [("ns/backend-b", "gpt-4o-mini")]
        snapshot = pair.snapshot("same-model distinct-backend Redis state")
        assert len(snapshot) == 2
        assert sorted(value for value, _ in snapshot.values()) == ["1", "101"]


def test_sequential_multi_descriptor_ttl(args):
    config = policy(rule("account", 4, "minute"), rule("ip", 2, "hour"), domain="live")
    request = make_request("live", (("account", "a"),), (("ip", "127.0.0.1"),))
    with paired_servers(args, "sequential", config) as pair:
        for number in range(1, 4):
            response = pair.call(request, "sequential multi-descriptor request %d" % number)
            assert [status.limit_remaining for status in response.statuses] == [4 - number, max(2 - number, 0)]
            assert [status.current_limit.requests_per_unit for status in response.statuses] == [4, 2]
        snapshot = pair.snapshot("sequential multi-descriptor")
        assert len(snapshot) == 2
        assert all(value == "3" and ttl > 0 for value, ttl in snapshot.values())


def test_expired_key(args):
    config, request = policy(rule("short", 1, "second"), domain="expiry"), make_request("expiry", (("short", "x"),))
    with paired_servers(args, "expiry", config) as pair:
        assert pair.call(request, "expiry initial request").overall_code == rls.RateLimitResponse.OK
        before = pair.snapshot("expiry before expiration")
        assert len(before) == 1 and next(iter(before.values()))[1] >= 0
        for service in pair.services:
            wait_for(lambda service=service: not service.redis_snapshot(),
                     "%s Redis key expiration" % service.label, timeout=5)
        response = pair.call(request, "expiry after key expiration")
        assert response.overall_code == rls.RateLimitResponse.OK and response.statuses[0].limit_remaining == 0
        pair.snapshot("expiry after reinsertion")


def test_service_restart_preserves_redis_quota(args):
    config, request = policy(rule("restart", 4, "minute"), domain="restart"), make_request("restart", (("restart", "x"),))
    with paired_servers(args, "service-restart", config) as pair:
        assert pair.call(request, "service restart request 1").statuses[0].limit_remaining == 3
        assert pair.call(request, "service restart request 2").statuses[0].limit_remaining == 2
        before = pair.snapshot("service restart state before stop")
        for service in pair.services:
            service.restart()
        assert pair.call(request, "service restart request after restart").statuses[0].limit_remaining == 1
        after = pair.snapshot("service restart state after restart")
        for key, (value, ttl) in before.items():
            assert key in after and value == "2" and after[key][0] == "3"
            assert ttl > 0 and after[key][1] > 0


def test_redis_restart_and_recovery(args):
    config, request = policy(rule("backend", 3, "minute"), domain="redis-restart"), make_request("redis-restart", (("backend", "x"),))
    # Use one real pooled Redis connection on both sides. This removes a race
    # between Go's asynchronous ten-connection pool warm-up and a restart,
    # while still exercising a live socket close, exact backend error, and
    # reconnect through each service's production client.
    with paired_servers(args, "redis-restart", config, {"REDIS_POOL_SIZE": "1"}) as pair:
        assert pair.call(request, "Redis restart initial request").statuses[0].limit_remaining == 2
        pair.stop_redis()
        failure = pair.call(request, "Redis unavailable gRPC error")
        assert isinstance(failure, tuple), "an unavailable Redis must fail the RPC"
        pair.restart_redis()
        response = pair.call(request, "Redis restart recovery request")
        assert not isinstance(response, tuple), "service did not recover after Redis restart"
        assert response.statuses[0].limit_remaining == 2
        pair.snapshot("Redis restart recovered Redis state")


def startup_failure(args, name, config, create_config, start_redis, extra_env):
    directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-integration-%s-" % name))
    services = [
        ServiceProcess("cpp", args.cpp, directory / "cpp", config, extra_env, create_config),
        ServiceProcess("go", args.go, directory / "go", config, extra_env, create_config),
    ]
    redis = [RedisProcess(service.directory, service.redis_port) for service in services] if start_redis else []
    try:
        for process in redis:
            process.start()
        for service in services:
            service.launch()
        for service in services:
            wait_for(lambda service=service: service.process.poll() is not None,
                     "%s startup failure" % service.label, timeout=10)
            assert service.process.returncode != 0, "%s exited cleanly for %s" % (service.label, name)
    finally:
        for service in services:
            service.stop()
        for process in redis:
            process.stop()
        if not args.keep_logs:
            shutil.rmtree(directory)
        else:
            print("logs retained at %s" % directory, flush=True)


def test_missing_and_unavailable_startup(args):
    # With Redis available, a missing configuration directory must itself make
    # both binaries reject startup before serving a gRPC listener.
    startup_failure(args, "missing-config", policy(rule("a", 1)), False, True, {})
    # With a finite startup deadline, an unavailable Redis must reject startup.
    startup_failure(args, "redis-unavailable", policy(rule("a", 1)), True, False, {
        "REDIS_STARTUP_MAX_ELAPSED_TIME": "1s", "REDIS_TIMEOUT": "100ms"})


def test_invalid_initial_config(args):
    invalid = {"domain": "invalid", "descriptors": [{
        "key": "bad", "rate_limit": {"unit": "not-a-unit", "requests_per_unit": 1}}]}
    request = make_request("invalid", (("bad", "x"),))
    with paired_servers(args, "invalid-config", invalid) as pair:
        result = pair.call(request, "invalid initial config gRPC error")
        assert isinstance(result, tuple) and result[0] == "UNKNOWN"


def test_grpc_health(args):
    config = policy(rule("health", 2), domain="health")
    with paired_servers(args, "grpc-health", config) as pair:
        named = pair.health("ratelimit", "named gRPC health status")
        default = pair.health("", "default gRPC health status")
        unknown = pair.health("not-configured", "unknown gRPC health status")
        assert named.status == health.HealthCheckResponse.SERVING
        assert default.status == health.HealthCheckResponse.SERVING
        assert unknown == ("NOT_FOUND", "unknown service")


def test_zero_and_uint32_max_limits(args):
    maximum = 4294967295
    config = policy(rule("zero", 0), rule("maximum", maximum), domain="bounds")
    zero = make_request("bounds", (("zero", "x"),))
    first = make_request("bounds", (("maximum", "x"),), hits=1)
    fill = make_request("bounds", (("maximum", "x"),), hits=maximum - 1)
    over = make_request("bounds", (("maximum", "x"),), hits=1)
    with paired_servers(args, "numeric-bounds", config) as pair:
        zero_response = pair.call(zero, "zero limit response")
        assert zero_response.overall_code == rls.RateLimitResponse.OVER_LIMIT
        assert zero_response.statuses[0].current_limit.requests_per_unit == 0
        assert zero_response.statuses[0].limit_remaining == 0
        first_response = pair.call(first, "uint32 maximum first hit")
        assert first_response.overall_code == rls.RateLimitResponse.OK
        assert first_response.statuses[0].limit_remaining == maximum - 1
        filled_response = pair.call(fill, "uint32 maximum exact fill")
        assert filled_response.overall_code == rls.RateLimitResponse.OK
        assert filled_response.statuses[0].limit_remaining == 0
        over_response = pair.call(over, "uint32 maximum overflow request")
        assert over_response.overall_code == rls.RateLimitResponse.OVER_LIMIT
        assert over_response.statuses[0].limit_remaining == 0
        pair.snapshot("zero and uint32 maximum Redis state")


def test_statsd_detailed_metric(args):
    # This is the exact policy shape and request used by the repository's
    # upstream Envoy/StatsD integration test: its Prometheus mapping expects
    # `rl.unspec_unspecified_value.over_limit`.  The additional value_to_metric
    # and dotted detailed entries cover the same names that config resolution
    # supplies to Go's gostats manager.
    config = policy(
        {"key": "unspec", "detailed_metric": True,
         "rate_limit": {"unit": "minute", "requests_per_unit": 2}},
        {"key": "value", "value_to_metric": True,
         "rate_limit": {"unit": "minute", "requests_per_unit": 1}},
        {"key": "dotted", "detailed_metric": True,
         "rate_limit": {"unit": "minute", "requests_per_unit": 1}},
        domain="rl")
    directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-integration-statsd-"))
    captures = [StatsdCapture(), StatsdCapture()]
    services = []
    redis = []
    try:
        for capture in captures:
            capture.start()
        for label, binary, capture in zip(("cpp", "go"), (args.cpp, args.go), captures):
            env = {
                "DISABLE_STATS": "false", "USE_STATSD": "true", "USE_DOG_STATSD": "false",
                "USE_PROMETHEUS": "false", "STATSD_HOST": "127.0.0.1",
                "STATSD_PORT": str(capture.port), "STATSD_PROTOCOL": "tcp",
                "STATS_FLUSH_INTERVAL": "50ms", "SANITIZE_DESCRIPTOR_METRIC_DOTS": "true",
            }
            service = ServiceProcess(label, binary, directory / label, config, env)
            services.append(service)
            redis.append(RedisProcess(service.directory, service.redis_port))
        for process in redis:
            process.start()
        for service in services:
            service.start()

        requests = [
            make_request("rl", (("unspec", "unspecified_value"),)),
            make_request("rl", (("unspec", "unspecified_value"),)),
            make_request("rl", (("unspec", "unspecified_value"),)),
            make_request("rl", (("value", "actual_value"),)),
            make_request("rl", (("dotted", "10.2.3.4"),)),
        ]
        for index, request in enumerate(requests):
            settle_second()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                replies = list(executor.map(lambda service: service.call(request), services))
            compare(replies, "StatsD source request %d" % (index + 1))

        prefix = "ratelimit.service.rate_limit."
        expected = {
            "ratelimit.service.rate_limit.rl.unspec_unspecified_value.total_hits": 3,
            "ratelimit.service.rate_limit.rl.unspec_unspecified_value.within_limit": 2,
            "ratelimit.service.rate_limit.rl.unspec_unspecified_value.near_limit": 1,
            "ratelimit.service.rate_limit.rl.unspec_unspecified_value.over_limit": 1,
            "ratelimit.service.rate_limit.rl.value_actual_value.total_hits": 1,
            "ratelimit.service.rate_limit.rl.value_actual_value.near_limit": 1,
            "ratelimit.service.rate_limit.rl.value_actual_value.within_limit": 1,
            "ratelimit.service.rate_limit.rl.dotted_10_2_3_4.total_hits": 1,
            "ratelimit.service.rate_limit.rl.dotted_10_2_3_4.near_limit": 1,
            "ratelimit.service.rate_limit.rl.dotted_10_2_3_4.within_limit": 1,
        }

        def complete():
            return all(all(capture.snapshot().get(name) == value for name, value in expected.items())
                       for capture in captures)
        wait_for(complete, "C++ and Go StatsD counters", timeout=10)
        observed = [{name: value for name, value in capture.snapshot().items() if name.startswith(prefix)}
                    for capture in captures]
        assert observed[0] == expected, "C++ StatsD names/counters: %r" % observed[0]
        assert observed[1] == expected, "Go StatsD names/counters: %r" % observed[1]
        assert observed[0] == observed[1], "C++ and Go StatsD differ: %r" % observed
    finally:
        for service in reversed(services):
            service.stop()
        for process in reversed(redis):
            process.stop()
        for capture in captures:
            capture.stop()
        if not args.keep_logs:
            shutil.rmtree(directory)
        else:
            print("logs retained at %s" % directory, flush=True)


def run_openssl(*arguments):
    completed = subprocess.run(["openssl", *arguments], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    if completed.returncode:
        raise AssertionError("openssl %s failed:\n%s" %
                             (" ".join(arguments), completed.stderr))


def create_tls_material(directory):
    """Create a small CA plus server/client certificates for real gRPC TLS."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    ca_cert, ca_key = directory / "ca.pem", directory / "ca-key.pem"
    run_openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256",
                "-keyout", str(ca_key), "-out", str(ca_cert), "-days", "2",
                "-subj", "/CN=ratelimit-integration-ca",
                "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign,cRLSign")

    serial = directory / "ca.srl"

    def issue(name, common_name, san, usage):
        key, csr, cert, extension = (directory / (name + suffix)
                                     for suffix in ("-key.pem", ".csr", ".pem", ".ext"))
        run_openssl("req", "-newkey", "rsa:2048", "-nodes", "-sha256",
                    "-keyout", str(key), "-out", str(csr), "-subj", "/CN=" + common_name)
        extension.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=%s\nsubjectAltName=DNS:%s\n" % (usage, san))
        serial_arguments = ("-CAcreateserial",) if not serial.exists() else ("-CAserial", str(serial))
        run_openssl("x509", "-req", "-in", str(csr), "-CA", str(ca_cert), "-CAkey", str(ca_key),
                    *serial_arguments, "-out", str(cert), "-days", "2", "-sha256",
                    "-extfile", str(extension))
        return key, cert

    server_key, server_cert = issue("server", "localhost", "localhost", "serverAuth")
    client_key, client_cert = issue("client", "client.integration.test", "client.integration.test",
                                    "clientAuth")
    wrong_client_key, wrong_client_cert = issue("wrong-client", "wrong.integration.test",
                                                "wrong.integration.test", "clientAuth")
    return {"ca": ca_cert, "server_key": server_key, "server_cert": server_cert,
            "client_key": client_key, "client_cert": client_cert,
            "wrong_client_key": wrong_client_key, "wrong_client_cert": wrong_client_cert}


def tls_client_credentials(material, with_client_certificate, prefix="client"):
    credentials = {"root_certificates": Path(material["ca"]).read_bytes(), "server_name": "localhost"}
    if with_client_certificate:
        credentials.update({"private_key": Path(material[prefix + "_key"]).read_bytes(),
                            "certificate_chain": Path(material[prefix + "_cert"]).read_bytes()})
    return credentials


def test_grpc_tls_and_mtls(args):
    """Compare real TLS/mTLS, client-SAN handshake validation, and proxy relay traffic."""
    certificate_directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-integration-tls-certs-"))
    try:
        material = create_tls_material(certificate_directory)
        request = make_request("tls", (("client", "trusted"),))
        config = policy(rule("client", 2, "minute"), domain="tls")
        server_tls = {
            "GRPC_SERVER_USE_TLS": "true",
            "GRPC_SERVER_TLS_CERT": str(material["server_cert"]),
            "GRPC_SERVER_TLS_KEY": str(material["server_key"]),
        }
        tls_only_client = tls_client_credentials(material, False)
        with paired_servers(args, "grpc-tls", config, server_tls,
                            client_tls=tls_only_client) as pair:
            response = pair.call(request, "TLS rate-limit reply")
            assert response.overall_code == rls.RateLimitResponse.OK
            assert response.statuses[0].limit_remaining == 1
            serving = pair.health("ratelimit", "TLS gRPC health status")
            assert serving.status == health.HealthCheckResponse.SERVING
            pair.snapshot("TLS Redis state")

        # Go does not request a client certificate without a client CA, so a
        # SAN setting alone must not turn ordinary server TLS into mTLS.
        san_without_client_ca = dict(server_tls)
        san_without_client_ca["GRPC_CLIENT_TLS_SAN"] = "client.integration.test"
        with paired_servers(args, "grpc-tls-san-without-ca", config, san_without_client_ca,
                            client_tls=tls_only_client) as pair:
            response = pair.call(request, "TLS client-SAN without client CA reply")
            assert response.overall_code == rls.RateLimitResponse.OK
            assert response.statuses[0].limit_remaining == 1
            pair.snapshot("TLS client-SAN without client CA Redis state")

        mtls = dict(server_tls)
        mtls.update({"GRPC_CLIENT_TLS_CACERT": str(material["ca"]),
                     "GRPC_CLIENT_TLS_SAN": "client.integration.test"})
        mtls_client = tls_client_credentials(material, True)
        private_proxy_socket = None
        with paired_servers(args, "grpc-mtls", config, mtls, client_tls=mtls_client) as pair:
            private_proxy_socket = assert_private_tls_proxy_upstream(pair.services[0])
            first = pair.call(request, "mTLS rate-limit reply")
            assert first.overall_code == rls.RateLimitResponse.OK
            assert first.statuses[0].limit_remaining == 1
            pair.health("ratelimit", "mTLS gRPC health status")
            pair.snapshot("mTLS Redis state")

            # Exercise several proxy connections at once. Each descriptor is
            # distinct so every response is deterministic while the relay has
            # concurrent bidirectional HTTP/2 traffic to carry.
            concurrent_requests = [make_request("tls", (("client", "trusted-concurrent-%d" % index),))
                                   for index in range(12)]
            settle_second()
            gate = threading.Event()
            def call_all(service):
                gate.wait()
                with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    return list(executor.map(service.call, concurrent_requests))
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(call_all, service) for service in pair.services]
                gate.set()
                concurrent_replies = [future.result() for future in futures]
            for index, (cpp_reply, go_reply) in enumerate(zip(*concurrent_replies)):
                compare((cpp_reply, go_reply), "mTLS concurrent proxy reply %d" % index)
                assert cpp_reply.overall_code == rls.RateLimitResponse.OK
                assert cpp_reply.statuses[0].limit_remaining == 1
            pair.snapshot("mTLS concurrent proxy Redis state")

            # Client certificates are required by the real listener.  The
            # transport's human-readable TLS error is implementation-specific,
            # so compare the gRPC status code while retaining each error detail
            # in a failure report.
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                rejected = list(executor.map(
                    lambda service: service.call_with_tls(request, tls_only_client), pair.services))
            assert all(isinstance(value, tuple) for value in rejected), rejected
            assert rejected[0][0] == rejected[1][0] == "UNAVAILABLE", rejected
        assert private_proxy_socket is not None
        assert not private_proxy_socket.exists(), "TLS proxy Unix socket survived shutdown: %s" % private_proxy_socket
        assert not private_proxy_socket.parent.exists(), (
            "TLS proxy private directory survived shutdown: %s" % private_proxy_socket.parent)
    finally:
        shutil.rmtree(certificate_directory, ignore_errors=True)


def test_grpc_client_san_rejection_parity(args):
    """Compare the complete raw transport error for a valid-but-wrong client SAN."""
    certificate_directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-integration-wrong-san-certs-"))
    try:
        material = create_tls_material(certificate_directory)
        request = make_request("tls", (("client", "wrong-san"),))
        config = policy(rule("client", 2, "minute"), domain="tls")
        mtls = {
            "GRPC_SERVER_USE_TLS": "true",
            "GRPC_SERVER_TLS_CERT": str(material["server_cert"]),
            "GRPC_SERVER_TLS_KEY": str(material["server_key"]),
            "GRPC_CLIENT_TLS_CACERT": str(material["ca"]),
            "GRPC_CLIENT_TLS_SAN": "client.integration.test",
        }
        good_client = tls_client_credentials(material, True)
        wrong_san_client = tls_client_credentials(material, True, "wrong_client")
        # The gRPC transport includes the endpoint in its error text. Run the
        # two independent services sequentially on one test-owned port rather
        # than normalizing that observable text away.
        with paired_servers(args, "grpc-wrong-client-san", config, mtls,
                            client_tls=good_client, start=False) as pair:
            cpp, go = pair.services
            go.grpc_port = cpp.grpc_port
            for redis in pair.redis:
                redis.start()
            cpp.start()
            cpp_rejected = cpp.call_with_tls(request, wrong_san_client)
            cpp.stop_service()
            go.start()
            go_rejected = go.call_with_tls(request, wrong_san_client)
            assert isinstance(cpp_rejected, tuple) and isinstance(go_rejected, tuple)
            compare((cpp_rejected, go_rejected), "mTLS wrong-client-SAN rejection")
            pair.snapshot("mTLS wrong-client-SAN leaves Redis untouched")
    finally:
        shutil.rmtree(certificate_directory, ignore_errors=True)


def test_grpc_tls_mtls_uds(args):
    """Compare TLS/mTLS client-SAN service behavior over real pathname UDSes."""
    certificate_directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-integration-tls-uds-certs-"))
    try:
        material = create_tls_material(certificate_directory)
        request = make_request("tls-uds", (("client", "trusted"),))
        config = policy(rule("client", 2, "minute"), domain="tls-uds")
        mtls = {
            "GRPC_SERVER_USE_TLS": "true",
            "GRPC_SERVER_TLS_CERT": str(material["server_cert"]),
            "GRPC_SERVER_TLS_KEY": str(material["server_key"]),
            "GRPC_CLIENT_TLS_CACERT": str(material["ca"]),
            "GRPC_CLIENT_TLS_SAN": "client.integration.test",
        }
        good_client = tls_client_credentials(material, True)
        wrong_san_client = tls_client_credentials(material, True, "wrong_client")
        private_proxy_socket = None
        public_sockets = []

        with paired_servers(args, "grpc-tls-mtls-uds", config, mtls,
                            client_tls=good_client, use_grpc_uds=True) as pair:
            cpp, go = pair.services
            public_sockets = [cpp.grpc_uds, go.grpc_uds]
            assert all(path and stat.S_ISSOCK(os.lstat(path).st_mode) for path in public_sockets)
            private_proxy_socket = assert_private_tls_proxy_upstream(cpp)

            first = pair.call(request, "mTLS UDS first rate-limit reply")
            assert first.overall_code == rls.RateLimitResponse.OK
            assert first.statuses[0].limit_remaining == 1
            pair.health("ratelimit", "mTLS UDS gRPC health status")
            second = pair.call(request, "mTLS UDS second rate-limit reply")
            assert second.overall_code == rls.RateLimitResponse.OK
            assert second.statuses[0].limit_remaining == 0
            pair.snapshot("mTLS UDS Redis state")

        assert private_proxy_socket is not None
        assert not private_proxy_socket.exists(), "private UDS upstream survived shutdown: %s" % private_proxy_socket
        assert not private_proxy_socket.parent.exists(), (
            "private UDS directory survived shutdown: %s" % private_proxy_socket.parent)
        assert all(not path.exists() for path in public_sockets), (
            "public TLS UDS socket survived shutdown: %r" % public_sockets)

        # Reuse exactly one public pathname sequentially so the transport's
        # endpoint-bearing error detail remains a byte-for-byte parity check.
        with paired_servers(args, "grpc-wrong-client-san-uds", config, mtls,
                            client_tls=good_client, start=False, use_grpc_uds=True) as pair:
            cpp, go = pair.services
            go.grpc_uds = cpp.grpc_uds
            for redis in pair.redis:
                redis.start()
            cpp.start()
            cpp_rejected = cpp.call_with_tls(request, wrong_san_client)
            cpp.stop_service()
            assert not cpp.grpc_uds.exists(), "C++ public TLS UDS socket survived stop: %s" % cpp.grpc_uds
            go.start()
            go_rejected = go.call_with_tls(request, wrong_san_client)
            assert isinstance(cpp_rejected, tuple) and isinstance(go_rejected, tuple)
            compare((cpp_rejected, go_rejected), "mTLS UDS wrong-client-SAN rejection")
            pair.snapshot("mTLS UDS wrong-client-SAN leaves Redis untouched")
    finally:
        shutil.rmtree(certificate_directory, ignore_errors=True)


class XdsSotwServer:
    """Small real ADS/SotW server that records requests and sends live updates."""
    TYPE_URL = "type.googleapis.com/ratelimit.config.ratelimit.v3.RateLimitConfig"

    def __init__(self, resources):
        self.port = free_port()
        self.resources = list(resources)
        self.version = 1
        self.condition = threading.Condition()
        self.requests, self.stream_count = [], 0
        self.server = None

    def start(self):
        self.server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=4))
        handler = grpc.stream_stream_rpc_method_handler(
            self.stream, request_deserializer=discovery.DiscoveryRequest.FromString,
            response_serializer=discovery.DiscoveryResponse.SerializeToString)
        self.server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "envoy.service.discovery.v3.AggregatedDiscoveryService",
            {"StreamAggregatedResources": handler}),))
        assert self.server.add_insecure_port("127.0.0.1:%d" % self.port) != 0
        self.server.start()

    def stop(self):
        if self.server:
            self.server.stop(0).wait(timeout=5)
            self.server = None

    def response(self, version, resources):
        from google.protobuf.any_pb2 import Any
        packed = []
        for resource in resources:
            value = Any()
            value.Pack(resource)
            packed.append(value)
        return discovery.DiscoveryResponse(version_info=str(version), resources=packed,
                                           type_url=self.TYPE_URL, nonce="nonce-%d" % version)

    def record(self, request, stream_id):
        copy = discovery.DiscoveryRequest()
        copy.CopyFrom(request)
        with self.condition:
            self.requests.append((stream_id, copy))
            self.condition.notify_all()

    def stream(self, iterator, context):
        incoming, complete = queue.Queue(), threading.Event()
        with self.condition:
            self.stream_count += 1
            stream_id = self.stream_count

        def receive():
            try:
                for request in iterator:
                    incoming.put(request)
            except grpc.RpcError:
                # Server shutdown cancels active streams; this is an expected
                # transport event after the services have been validated.
                pass
            finally:
                complete.set()
                incoming.put(None)

        threading.Thread(target=receive, daemon=True).start()
        try:
            first = incoming.get(timeout=10)
        except queue.Empty:
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "xDS client did not send DiscoveryRequest")
        if first is None:
            return
        self.record(first, stream_id)
        sent = 0
        while context.is_active() and not complete.is_set():
            with self.condition:
                version, resources = self.version, list(self.resources)
            if version > sent:
                yield self.response(version, resources)
                sent = version
            try:
                request = incoming.get(timeout=0.05)
            except queue.Empty:
                continue
            if request is None:
                return
            self.record(request, stream_id)

    def update(self, resources):
        with self.condition:
            self.resources = list(resources)
            self.version += 1
            self.condition.notify_all()

    def initial_requests_from(self, node_ids):
        with self.condition:
            observed = {request.node.id for _, request in self.requests
                        if request.type_url == self.TYPE_URL and not request.version_info}
        return set(node_ids).issubset(observed)

    def acked_by(self, version, node_ids):
        with self.condition:
            observed = {request.node.id for _, request in self.requests
                        if request.type_url == self.TYPE_URL and request.version_info == str(version)
                        and request.response_nonce == "nonce-%d" % version
                        and not request.HasField("error_detail")}
        return set(node_ids).issubset(observed)


def xds_rate_limit_config(name, requests_per_unit):
    descriptor = xds_rls.RateLimitDescriptor(key="xds")
    descriptor.rate_limit.unit = xds_rls.MINUTE
    descriptor.rate_limit.requests_per_unit = requests_per_unit
    return xds_rls.RateLimitConfig(name=name, domain="xds", descriptors=[descriptor])


def test_xds_sotw(args):
    """Exercise initial ADS/SotW load, ACK, and a live xDS policy replacement."""
    server = XdsSotwServer([xds_rate_limit_config("xds-initial", 2)])
    nodes = {"cpp-xds-integration", "go-xds-integration"}
    server.start()
    try:
        def xds_environment(label):
            return {
                "CONFIG_TYPE": "GRPC_XDS_SOTW",
                "CONFIG_GRPC_XDS_SERVER_URL": "127.0.0.1:%d" % server.port,
                "CONFIG_GRPC_XDS_NODE_ID": "%s-xds-integration" % label,
                "CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL": "100ms",
                "FORCE_START_WITHOUT_INITIAL_CONFIG": "false",
            }

        with paired_servers(args, "xds-sotw", None, xds_environment,
                            create_config=False) as pair:
            wait_for(lambda: server.initial_requests_from(nodes), "initial xDS SotW DiscoveryRequests")
            wait_for(lambda: server.acked_by(1, nodes), "initial xDS SotW ACKs")
            request = make_request("xds", (("xds", "value"),))
            initial = pair.call(request, "xDS initial policy reply")
            assert initial.overall_code == rls.RateLimitResponse.OK
            assert initial.statuses[0].current_limit.requests_per_unit == 2
            assert initial.statuses[0].limit_remaining == 1

            server.update([xds_rate_limit_config("xds-reloaded", 1)])
            wait_for(lambda: server.acked_by(2, nodes), "reloaded xDS SotW ACKs")
            reloaded = pair.call(request, "xDS reloaded policy reply")
            assert reloaded.overall_code == rls.RateLimitResponse.OVER_LIMIT
            assert reloaded.statuses[0].current_limit.requests_per_unit == 1
            assert reloaded.statuses[0].limit_remaining == 0
            pair.snapshot("xDS reloaded Redis state")
    finally:
        server.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", required=True)
    parser.add_argument("--go", required=True)
    parser.add_argument("--generated", default="build/python")
    parser.add_argument("--filter", default="")
    parser.add_argument("--keep-logs", action="store_true")
    args = parser.parse_args()
    args.cpp, args.go = str(Path(args.cpp).resolve()), str(Path(args.go).resolve())
    sys.path.insert(0, str(Path(args.generated).resolve()))
    global rls
    from envoy.service.ratelimit.v3 import rls_pb2 as rls
    global rate_limit
    from envoy.extensions.common.ratelimit.v3 import ratelimit_pb2 as rate_limit
    global health
    # grpcio owns the top-level ``grpc`` package, so the generated descriptor
    # module cannot be imported through its on-disk grpc/ namespace.  Load the
    # dependency-free generated health message module directly instead.
    health_path = Path(args.generated).resolve() / "grpc/health/v1/health_pb2.py"
    health_spec = importlib.util.spec_from_file_location("ratelimit_test_health_pb2", health_path)
    health = importlib.util.module_from_spec(health_spec)
    sys.modules[health_spec.name] = health
    health_spec.loader.exec_module(health)
    global discovery
    from envoy.service.discovery.v3 import discovery_pb2 as discovery
    global xds_rls
    from api.ratelimit.config.ratelimit.v3 import rls_conf_pb2 as xds_rls
    tests = [
        ("sequential_multi_descriptor_ttl", test_sequential_multi_descriptor_ttl),
        ("expired_redis_key", test_expired_key),
        ("service_restart_preserves_redis_quota", test_service_restart_preserves_redis_quota),
        ("redis_restart_and_recovery", test_redis_restart_and_recovery),
        ("missing_config_and_redis_unavailable_startup", test_missing_and_unavailable_startup),
        ("invalid_initial_config", test_invalid_initial_config),
        ("grpc_health", test_grpc_health),
        ("health_requires_loaded_config_transitions", test_health_requires_loaded_config_transitions),
        ("quota_same_model_different_backends", test_quota_same_model_different_backends),
        ("zero_and_uint32_max_limits", test_zero_and_uint32_max_limits),
        ("statsd_detailed_metric", test_statsd_detailed_metric),
        ("statsd_go_source_metric_matrix", test_statsd_go_source_metric_matrix),
        ("calendar_month_current_window_keys", test_calendar_month_current_window_keys),
        ("grpc_tls_and_mtls", test_grpc_tls_and_mtls),
        ("grpc_tls_mtls_uds", test_grpc_tls_mtls_uds),
        ("grpc_client_san_rejection_parity", test_grpc_client_san_rejection_parity),
        ("xds_sotw", test_xds_sotw),
    ]
    ran = 0
    for name, test in tests:
        if args.filter in name:
            test(args)
            ran += 1
            print("PASS", name, flush=True)
    if not ran:
        parser.error("no tests matched --filter")
    print("integration validation: %d black-box C++/Go scenarios passed" % ran)


if __name__ == "__main__":
    main()
