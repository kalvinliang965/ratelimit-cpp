#!/usr/bin/env python3
"""Exercise real gRPC servers with private Redis instances.

Without --go, checks independently specified invariants against C++. With --go,
also compares *every field* of each response (including field presence, headers,
durations, metadata and ordering), gRPC errors, and final Redis keys/counters.
No response fields are stripped or normalized to hide implementation differences.
"""

import argparse
import concurrent.futures
import contextlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import grpc
from google.protobuf import json_format


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def redis_command(port, *args):
    """Tiny RESP client, avoiding a Python redis dependency."""
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        data = [str(a).encode() for a in args]
        sock.sendall(b"*%d\r\n" % len(data) + b"".join(
            b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in data))
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
                result = stream.read(length)
                stream.read(2)
                return result.decode()
            if kind == b"*":
                return [read() for _ in range(int(value))]
            return value.decode()

        return read()


class Server:
    def __init__(self, binary, directory, config, settings):
        self.binary = binary
        self.directory = directory
        self.config_dir = directory / "runtime" / "ratelimit" / "config"
        self.config_dir.mkdir(parents=True)
        self.write_config(config)
        self.processes = []
        self.logs = []
        self.channel = None
        self.redis_ports = []
        self.port = free_port()
        self.settings = settings

    def write_config(self, config):
        # JSON is a YAML subset and preserves scalar types without extra packages.
        configs = config if isinstance(config, list) else [config]
        for index, item in enumerate(configs):
            temporary = self.config_dir / (".pending%d" % index)
            temporary.write_text(json.dumps(item))
            temporary.replace(self.config_dir / ("policy%d.yaml" % index))

    def launch(self, command, name, env=None):
        output = open(self.directory / (name + ".log"), "wb")
        self.logs.append(output)
        process = subprocess.Popen(command, env=env, stdout=output, stderr=output)
        self.processes.append(process)
        return process

    def start_redis(self):
        port = free_port()
        self.launch(["redis-server", "--bind", "127.0.0.1", "--port", str(port),
                     "--save", "", "--appendonly", "no", "--loglevel", "warning"],
                    "redis%d" % len(self.redis_ports))
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                if redis_command(port, "PING") == "PONG":
                    self.redis_ports.append(port)
                    return port
            except OSError:
                time.sleep(0.025)
        raise AssertionError("Redis did not become ready")

    def start(self):
        redis_port = self.start_redis()
        # Prevent developer-shell application settings from affecting fixtures.
        prefixes = ("REDIS_", "RUNTIME_", "LIMIT_", "GRPC_", "CONFIG_", "MEMCACHE_",
                    "STATSD_", "TRACING_", "PROMETHEUS_")
        env = {k: v for k, v in os.environ.items() if not k.startswith(prefixes)}
        env.update({
            "RUNTIME_ROOT": str(self.directory / "runtime"),
            "RUNTIME_SUBDIRECTORY": "ratelimit", "RUNTIME_APPDIRECTORY": "config",
            "RUNTIME_WATCH_ROOT": "false", "RUNTIME_IGNOREDOTFILES": "true",
            "GRPC_HOST": "127.0.0.1", "GRPC_PORT": str(self.port),
            "HOST": "127.0.0.1", "PORT": str(free_port()),
            "DEBUG_HOST": "127.0.0.1", "DEBUG_PORT": str(free_port()),
            "REDIS_SOCKET_TYPE": "tcp", "REDIS_URL": "127.0.0.1:%d" % redis_port,
            "REDIS_TYPE": "SINGLE", "REDIS_TIMEOUT": "2s",
            "REDIS_STARTUP_MAX_ELAPSED_TIME": "5s", "REDIS_PERSECOND": "false",
            "BACKEND_TYPE": "redis", "EXPIRATION_JITTER_MAX_SECONDS": "0",
            "LOCAL_CACHE_SIZE_IN_BYTES": "0", "CACHE_KEY_PREFIX": "",
            "NEAR_LIMIT_RATIO": "0.8", "STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT": "false",
            "USE_CALENDAR_MONTH_RATE_LIMIT": "false", "USE_STATSD": "false",
            "USE_DOG_STATSD": "false", "USE_PROMETHEUS": "false",
            "DISABLE_STATS": "true", "TRACING_ENABLED": "false", "LOG_LEVEL": "error",
            "SHADOW_MODE": "false", "QUOTA_MODE": "false", "RESPONSE_DYNAMIC_METADATA": "false",
            "MERGE_DOMAIN_CONFIG": "false", "CONFIG_TYPE": "FILE",
            "FORCE_START_WITHOUT_INITIAL_CONFIG": "false",
        })
        env.update(self.settings)
        if env.get("REDIS_PERSECOND") == "true":
            env.update(REDIS_PERSECOND_SOCKET_TYPE="tcp",
                       REDIS_PERSECOND_URL="127.0.0.1:%d" % self.start_redis())
        process = self.launch([self.binary], "server", env)
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % self.port)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("Server exited with code %s" % process.returncode)
            try:
                grpc.channel_ready_future(self.channel).result(timeout=0.2)
                self.rpc = self.channel.unary_unary(
                    "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
                    request_serializer=rls.RateLimitRequest.SerializeToString,
                    response_deserializer=rls.RateLimitResponse.FromString)
                # Deliberately bypass protobuf parsing in the client so the
                # server, rather than Python's message constructor, receives
                # malformed gRPC payloads.
                self.raw_rpc = self.channel.unary_unary(
                    "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
                    request_serializer=lambda body: body,
                    response_deserializer=lambda body: body)
                return self
            except grpc.FutureTimeoutError:
                pass
        raise AssertionError("gRPC server did not become ready")

    def call(self, request):
        try:
            return self.rpc(request, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())

    def call_raw(self, body):
        try:
            return self.raw_rpc(body, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())

    def snapshot(self):
        return [dict((key, redis_command(port, "GET", key))
                     for key in sorted(redis_command(port, "KEYS", "*")))
                for port in self.redis_ports]

    def close(self):
        if self.channel:
            self.channel.close()
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for output in self.logs:
            output.close()


def rule(key, count=3, unit="hour", value=None, **extra):
    result = {"key": key, "rate_limit": {"unit": unit, "requests_per_unit": count}}
    if value is not None:
        result["value"] = value
    result.update(extra)
    return result


def policy(*rules, domain="parity"):
    return {"domain": domain, "descriptors": list(rules)}


def descriptor(*entries, hits=None, negative=False, limit=None):
    result = {"entries": [{"key": key, "value": value} for key, value in entries]}
    if hits is not None:
        result["hits_addend"] = {"value": hits}
    if negative:
        result["is_negative_hits"] = True
    if limit is not None:
        result["limit"] = {"requests_per_unit": limit[0], "unit": limit[1]}
    return result


def request(*descriptors, domain="parity", hits=0):
    return rls.RateLimitRequest(domain=domain, descriptors=descriptors, hits_addend=hits)


def one(key, value="x", **kw):
    return request(descriptor((key, value), **kw))


def expected(code=None, remaining=None, status_codes=None):
    def check(response):
        if isinstance(response, tuple):
            raise AssertionError("Unexpected gRPC error: %r" % (response,))
        if code is not None:
            assert response.overall_code == code, ("overall_code", response)
        if remaining is not None:
            assert [s.limit_remaining for s in response.statuses] == remaining, response
        if status_codes is not None:
            assert [s.code for s in response.statuses] == status_codes, response
    return check


def expected_headers(kind):
    """Check header contents independently of an optional Go reference server."""
    def check(response):
        expected(rls.RateLimitResponse.OK, [8])(response)
        response_headers = list(response.response_headers_to_add)
        request_headers = list(response.request_headers_to_add)
        headers = request_headers if kind == "request" else response_headers
        unused = response_headers if kind == "request" else request_headers
        assert not unused, ("unexpected headers in the other direction", unused)
        if kind == "per_unit":
            wanted = {"RateLimit-Limit-Hours": "9", "RateLimit-Remaining-Hours": "8"}
        else:
            prefix = "x-request-" if kind == "request" else "x-response-"
            wanted = {prefix + "limit": "9", prefix + "remaining": "8", prefix + "reset": None}
        # Check the length before constructing a dict, so duplicate keys cannot
        # hide an incorrectly appended or overwritten header.
        assert len(headers) == len(wanted), (kind, "header count", headers, wanted)
        actual = {header.key: header.value for header in headers}
        assert set(actual) == set(wanted), (kind, "header names", actual, wanted)
        for key, value in wanted.items():
            if value is None:
                assert actual[key].isdigit() and 1 <= int(actual[key]) <= 3600, (key, actual[key])
            else:
                assert actual[key] == value, (key, actual[key], value)
    return check


def scenarios():
    ok, over = rls.RateLimitResponse.OK, rls.RateLimitResponse.OVER_LIMIT
    # These assertions run under make test, without requiring --go.
    for kind, enabled in [("response", "LIMIT_RESPONSE_HEADERS_ENABLED"),
                          ("request", "LIMIT_REQUEST_HEADERS_ENABLED"),
                          ("per_unit", "LIMIT_PER_UNIT_HEADERS_ENABLED")]:
        settings = {"LIMIT_RESPONSE_HEADERS_ENABLED": "false",
                    "LIMIT_REQUEST_HEADERS_ENABLED": "false",
                    "LIMIT_PER_UNIT_HEADERS_ENABLED": "false",
                    "LIMIT_LIMIT_HEADER": "x-response-limit",
                    "LIMIT_REMAINING_HEADER": "x-response-remaining",
                    "LIMIT_RESET_HEADER": "x-response-reset",
                    "LIMIT_REQUEST_LIMIT_HEADER": "x-request-limit",
                    "LIMIT_REQUEST_REMAINING_HEADER": "x-request-remaining",
                    "LIMIT_REQUEST_RESET_HEADER": "x-request-reset"}
        settings[enabled] = "true"
        yield ("header_entries_" + kind, policy(rule("headers", 9, unit="hour")),
               settings, [(one("headers"), expected_headers(kind))])

    basic = policy(rule("basic"), rule("blocked", 0),
                   rule("unlimited", rate_limit={"unlimited": True}),
                   rule("shadow", 1, shadow_mode=True))
    yield ("basic_errors_unlimited_shadow", basic, {}, [
        (request(domain=""), ("UNKNOWN", "rate limit domain must not be empty")),
        (request(), ("UNKNOWN", "rate limit descriptor list must not be empty")),
        (one("missing"), expected(ok, [0])),
        (request(descriptor(), domain="unknown"), expected(ok, [0])),
        (request(descriptor(("basic", "x")), domain="unknown"), expected(ok, [0])),
        *[(one("basic"), expected(ok if i < 3 else over, [max(2-i, 0)])) for i in range(5)],
        (one("blocked"), expected(over, [0])),
        (one("unlimited"), expected(ok, [4294967295])),
        (one("shadow"), expected(ok, [0])), (one("shadow"), expected(ok, [0])),
    ])
    # Exercise both ends of the configured uint32 range through Redis and the
    # gRPC response fields.  In particular, the second request crosses the
    # maximum configured threshold rather than overflowing a local counter.
    yield "zero_and_maximum_limits", policy(
        rule("zero", 0), rule("maximum", 4294967295)), {}, [
            (one("zero"), expected(over, [0])),
            (one("maximum"), expected(ok, [4294967294])),
            (one("maximum", hits=4294967295), expected(over, [0])),
        ]
    matching = policy(
        rule("user", 3), rule("user", 1, value="specific"),
        rule("path", 2, value="/api/*"), rule("middle", 2, value="a*b*c"),
        rule("suffix", 2, value="*tail"), rule("shared", 2, value="team-*", share_threshold=True),
        {"key": "app", "value": "web", "descriptors": [rule("user", 2), rule("user", 1, value="vip")]},
        rule("depth", 1),
    )
    steps = [(one("user", "specific"), expected(ok, [0])),
             (one("user", "specific"), expected(over, [0])),
             (one("user", "other"), expected(ok, [2]))]
    for key, values in [("path", ["/api/a", "/api/b", "/api/a", "/api/a", "/no"]),
                        ("middle", ["abc", "axbyc", "axc", "abc", "abc"]),
                        ("suffix", ["tail", "longtail", "tail", "tail", "tailx"]),
                        ("shared", ["team-a", "team-b", "team-c"])]:
        steps.extend((one(key, value), None) for value in values)
    steps.extend((request(descriptor(*entries)), None) for entries in [
        (("app", "web"),), (("app", "web"), ("user", "alice")),
        (("app", "web"), ("user", "vip")), (("app", "web"), ("user", "vip")),
        (("app", "web"), ("user", "vip"), ("extra", "x")),
        (("depth", "x"), ("extra", "x")), (("depth", "x"),),
    ])
    yield "nested_exact_wildcard_shared", matching, {}, steps
    hits_policy = policy(rule("hits", 8), rule("other", 10))
    yield "hits_overrides_refunds", hits_policy, {}, [
        (request(descriptor(("hits", "x")), hits=3), expected(ok, [5])),
        (request(descriptor(("hits", "x"), hits=2), hits=7), expected(ok, [3])),
        (one("hits", hits=0), expected(ok, [3])),
        (one("hits", hits=4), expected(over, [0])),
        (one("hits", hits=1, negative=True), expected(ok, [0])),
        (one("hits", hits=3, negative=True), expected(ok, [3])),
        (one("hits", hits=99, negative=True), expected(ok, [8])),
        (one("hits"), expected(ok, [7])),
        (one("hits", "override", limit=(2, 3)), None),
        (one("hits", "override", limit=(2, 3)), None),
        (one("hits", "override", limit=(2, 3)), None),
        (one("missing", limit=(2, 3)), None),
        (one("other", hits=4294967296), expected(over, [0])),
        (one("other", hits=4294967296, negative=True), expected(ok, [10])),
        (one("missing", hits=2, negative=True), expected(ok, [0])),
    ]
    replaced = rule("old", rate_limit={"unit": "hour", "requests_per_unit": 1, "name": "old-limit"})
    replacer = rule("new", rate_limit={"unit": "hour", "requests_per_unit": 2,
                                     "name": "new-limit", "replaces": [{"name": "old-limit"}]})
    yield "named_replacement", policy(replaced, replacer), {}, [
        (request(descriptor(("old", "x")), descriptor(("new", "x"))), expected(ok, [0, 1])),
        (request(descriptor(("old", "x")), descriptor(("new", "x"))), expected(ok, [0, 0])),
        (request(descriptor(("old", "x")), descriptor(("new", "x"))), expected(over, [0, 0])),
        (one("old"), expected(ok, [0])), (one("old"), expected(over, [0])),
    ]
    all_units = ["second", "minute", "hour", "day", "week", "month", "year"]
    units_policy = policy(*(rule(unit, 9, unit=unit) for unit in all_units))
    unit_request = request(*(descriptor((unit, "x")) for unit in all_units))
    for calendar in [False, True]:
        yield ("all_units_headers_calendar_%s" % calendar, units_policy,
               {"USE_CALENDAR_MONTH_RATE_LIMIT": str(calendar).lower(),
                "LIMIT_RESPONSE_HEADERS_ENABLED": "true", "LIMIT_REQUEST_HEADERS_ENABLED": "true",
                "LIMIT_PER_UNIT_HEADERS_ENABLED": "true", "LIMIT_LIMIT_HEADER": "x-limit",
                "LIMIT_REMAINING_HEADER": "x-remaining", "LIMIT_RESET_HEADER": "x-reset",
                "LIMIT_REQUEST_LIMIT_HEADER": "x-request-limit",
                "LIMIT_REQUEST_REMAINING_HEADER": "x-request-remaining",
                "LIMIT_REQUEST_RESET_HEADER": "x-request-reset"},
               [(unit_request, expected(ok, [8]*7)), (unit_request, expected(ok, [7]*7))],
               {"atomic_second_window": True})
    model_policy = policy({"key": "backend_name", "descriptors": [
        {"key": "model_name_override", "value": "a", "descriptors": [
            rule("tenant", 1, quota_mode=True, metadata={"routing": {"a": True}}),
            rule("default", 4, quota_mode=True, metadata={"routing": {"defaultA": True}})]},
        {"key": "model_name_override", "value": "b", "descriptors": [
            rule("default", 3, quota_mode=True, metadata={"routing": {"b": True}})]},
    ]})
    model_request = request(
        descriptor(("backend_name", "ns/backend"), ("model_name_override", "a"), ("tenant", "u")),
        descriptor(("backend_name", "ns/backend"), ("model_name_override", "a"), ("default", "all")),
        descriptor(("backend_name", "ns/backend"), ("model_name_override", "b"), ("default", "all")))
    yield "quota_model_failover_metadata", model_policy, {"RESPONSE_DYNAMIC_METADATA": "true"}, [
        (model_request, expected(ok, [0, 3, 2])),
        (model_request, expected(ok, [0, 2, 1], [over, ok, ok])),
        (model_request, expected(ok, [0, 1, 0], [over, ok, ok])),
        (model_request, expected(over, [0, 0, 0], [over, ok, over])),
    ]
    metadata_policy = policy(
        rule("meta", 5, metadata={"route": {"first": 1, "conflict": "first"},
                                   "list": [1, True, "text"], "null": None}),
        rule("other", 5, metadata={"route": {"second": 2, "conflict": "second"}}),
    )
    yield "metadata_overrides_and_merge", metadata_policy, {"RESPONSE_DYNAMIC_METADATA": "true"}, [
        (request(descriptor(("meta", "x"), limit=(4, 3), hits=0),
                 descriptor(("other", "x")), hits=2), None),
        (request(descriptor(("other", "x")), descriptor(("meta", "x"))), None),
        (request(descriptor(("missing", "x"))), None),
    ]
    for env in [{"SHADOW_MODE": "true"}, {"QUOTA_MODE": "true"},
                {"SHADOW_MODE": "true", "QUOTA_MODE": "true"}]:
        req = request(descriptor(("a", "x")), descriptor(("b", "x")))
        yield "global_" + "_".join(env), policy(rule("a", 1), rule("b", 3)), env, [(req, None)]*5
    for cache in [False, True]:
        for stop in [False, True]:
            settings = {"LOCAL_CACHE_SIZE_IN_BYTES": "1048576" if cache else "0",
                        "STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT": str(stop).lower()}
            req = request(descriptor(("a", "x")), descriptor(("b", "x")))
            steps = [(req, None)] * 5 + [
                (one("a", hits=20, negative=True), None), (req, None),
                (one("b", hits=20, negative=True), None), (one("b"), None)]
            yield "local_cache_%s_stop_%s" % (cache, stop), policy(rule("a", 1), rule("b", 8)), settings, steps
    per_second_req = request(descriptor(("second", "x")), descriptor(("minute", "x")))
    yield "dedicated_per_second_redis", policy(rule("second", 2, "second"), rule("minute", 4, "minute")), {
        "REDIS_PERSECOND": "true"}, [(per_second_req, None)]*4
    yield "cache_prefix_and_duplicate_descriptors", policy(rule("a", 4)), {"CACHE_KEY_PREFIX": "integration_"}, [
        (request(descriptor(("a", "x")), descriptor(("a", "x"))), expected(ok, [3, 2])),
        (one("a"), expected(ok, [1])),
        (request(descriptor(("a", "x")), descriptor(("a", "x"))), expected(over, [0, 0])),
    ]
    # Exercise response fields which are easy to accidentally approximate: a
    # descriptor-level shadow response retains OVER_LIMIT's zero remaining
    # capacity while returning OK; headers choose the lowest remaining limit
    # for each unit; and a zero addend is a pure check rather than a default
    # single hit when it is set on the descriptor.
    shadow_headers = policy(
        rule("strict", 1, unit="second"),
        rule("shadow", 1, unit="second", shadow_mode=True),
        rule("small", 2, unit="second"),
        rule("minute", 3, unit="minute"),
    )
    combined = request(descriptor(("strict", "x")), descriptor(("shadow", "x")),
                       descriptor(("small", "x")), descriptor(("minute", "x")))
    yield "shadow_zero_hits_same_unit_headers", shadow_headers, {
        "LIMIT_RESPONSE_HEADERS_ENABLED": "true", "LIMIT_REQUEST_HEADERS_ENABLED": "true",
        "LIMIT_PER_UNIT_HEADERS_ENABLED": "true"}, [
            (combined, None), (combined, None),
            (one("small", hits=0), expected(ok, [0])),
            (one("minute", hits=0), expected(ok, [1])),
        ]
    # A regular descriptor remains decisive even when quota descriptors still
    # have capacity. A quota descriptor with no backend/model entries is a
    # separate enforcement group, which exercises the non-model group-key path.
    mixed_quota = policy(rule("regular", 1), rule("quota_a", 1, quota_mode=True),
                         rule("quota_b", 2, quota_mode=True))
    mixed_request = request(descriptor(("regular", "x")), descriptor(("quota_a", "x")),
                            descriptor(("quota_b", "x")))
    yield "mixed_regular_and_anonymous_quota", mixed_quota, {"RESPONSE_DYNAMIC_METADATA": "true"}, [
        (mixed_request, None), (mixed_request, None), (mixed_request, None),
    ]
    # Overrides use the descriptor's exact fields and create independent cache
    # keys from the configured policy. Test both an existing unmatched key and
    # a configured key with an override, including a zero request-level addend.
    override_policy = policy(rule("configured", 9), rule("default", 9))
    yield "override_presence_and_zero_request_hits", override_policy, {}, [
        (request(descriptor(("configured", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("configured", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("configured", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("missing", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("default", "x")), descriptor(("missing", "x"))), None),
    ]
    yield "merge_domain_files", [policy(rule("a", 2)), policy(rule("b", 3))], {
        "MERGE_DOMAIN_CONFIG": "true"}, [
            (request(descriptor(("a", "x")), descriptor(("b", "x"))), expected(ok, [1, 2]))]


def describe(response):
    return repr(response) if isinstance(response, tuple) else json_format.MessageToJson(response)


def encoded(response):
    # Raw malformed-wire tests intentionally use a bytes deserializer so a
    # valid request with unknown fields can be compared at the exact gRPC
    # payload level too.
    if isinstance(response, bytes):
        return response
    return repr(response).encode() if isinstance(response, tuple) else response.SerializeToString(deterministic=True)


def settle_second():
    # Go's reset fields are whole seconds. Make paired requests well clear of an
    # epoch-second transition so equality is meaningful without normalizing fields.
    fraction = time.time() % 1
    if fraction > 0.65:
        time.sleep(1.03 - fraction)


def begin_second_window():
    """Enter a fresh second once for a sequence with stateful SECOND limits."""
    time.sleep(1.03 - (time.time() % 1))


class Runner:
    def __init__(self, args):
        self.args = args
        self.comparisons = 0
        self.passed = 0

    @contextlib.contextmanager
    def servers(self, name, config, settings):
        directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-%s-" % name))
        binaries = [self.args.cpp] + ([self.args.go] if self.args.go else [])
        servers = []
        success = False
        try:
            for label, binary in zip(["cpp", "go"], binaries):
                server = Server(binary, directory / label, config, settings)
                servers.append(server)
                server.start()
            yield servers
            success = True
        finally:
            for server in reversed(servers):
                server.close()
            if success and not self.args.keep_logs:
                shutil.rmtree(directory)
            else:
                print("  Logs:", directory, flush=True)
                if not success:
                    for log in directory.glob("*/server.log"):
                        print("  %s (last lines):\n%s" % (log, "\n".join(log.read_text(errors="replace").splitlines()[-12:])))

    def compare(self, responses, label):
        self.comparisons += 1
        # Compare deterministic protobuf bytes rather than only Python's
        # message equality. This includes field presence, repeated-field
        # order, durations, header order, map contents, and all response
        # fields; errors are compared as (status code, details) tuples.
        if len(responses) > 1 and encoded(responses[0]) != encoded(responses[1]):
            raise AssertionError("%s\nC++: %s\nGo: %s" %
                                 (label, describe(responses[0]), describe(responses[1])))

    def paired_call(self, servers, req, label, settle=True):
        if settle:
            settle_second()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as executor:
            responses = list(executor.map(lambda server: server.call(req), servers))
        self.compare(responses, label)
        return responses[0]

    def run_scenario(self, case):
        name, config, settings, steps, *markers = case
        atomic_second_window = bool(markers and markers[0].get("atomic_second_window"))
        with self.servers(name, config, settings) as servers:
            if atomic_second_window:
                # Settling before each request can itself cross an epoch
                # second and reset the quota the vector intentionally checks.
                begin_second_window()
            for index, (req, check) in enumerate(steps):
                response = self.paired_call(
                    servers, req, "%s, request %d: %s" % (name, index+1, req),
                    settle=not atomic_second_window)
                if callable(check):
                    check(response)
                elif check is not None:
                    assert response == check, (name, response, check)
            snapshots = [server.snapshot() for server in servers]
            if len(snapshots) > 1:
                assert snapshots[0] == snapshots[1], (name, "Redis state differs", snapshots)
        self.passed += 1
        print("PASS", name, "(%d requests)" % len(steps), flush=True)

    def concurrency(self):
        name = "concurrent_atomic_counters"
        with self.servers(name, policy(rule("parallel", 17)), {}) as servers:
            settle_second()
            req = one("parallel")

            def bombard(server):
                with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    return list(executor.map(lambda _: server.call(req), range(64)))

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as executor:
                responses = list(executor.map(bombard, servers))
            for group in responses:
                assert sum(not isinstance(r, tuple) and r.overall_code == rls.RateLimitResponse.OK for r in group) == 17
            if len(responses) > 1:
                assert sorted(map(encoded, responses[0])) == sorted(map(encoded, responses[1])), "Concurrent response multisets differ"
                assert servers[0].snapshot() == servers[1].snapshot(), "Concurrent Redis states differ"
            self.comparisons += 64
        self.passed += 1
        print("PASS", name, "(64 requests, 12 workers)", flush=True)

    def reload(self):
        name = "valid_and_invalid_config_reload"
        with self.servers(name, policy(rule("reload", 7)), {}) as servers:
            req = one("reload", hits=0)
            expected(rls.RateLimitResponse.OK, [7])(self.paired_call(servers, req, name + " before"))
            for server in servers:
                server.write_config(policy(rule("reload", 11)))
            for server in servers:
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    result = server.call(req)
                    if not isinstance(result, tuple) and result.statuses[0].current_limit.requests_per_unit == 11:
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError("Configuration reload was not observed")
            expected(rls.RateLimitResponse.OK, [11])(self.paired_call(servers, req, name + " after"))
            for server in servers:
                server.write_config(policy(rule("reload", 11, unit="invalid")))
            time.sleep(0.6)
            expected(rls.RateLimitResponse.OK, [11])(self.paired_call(servers, req, name + " invalid retained"))
        self.passed += 1
        print("PASS", name, flush=True)

    def malformed_grpc(self):
        """Exercise actual gRPC decoding errors, not an internal service call."""
        name = "malformed_grpc_payloads"
        with self.servers(name, policy(rule("a", 1)), {}) as servers:
            # Truncated length-delimited field, malformed varint, and a valid
            # request with an unknown field all go through the wire decoder.
            # Unknown fields must be ignored by both implementations.
            payloads = [b"\x0a", b"\x80", b"\x0a\x06parity\x12\x00\x98\x06\x01"]
            for index, body in enumerate(payloads):
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as executor:
                    responses = list(executor.map(lambda server: server.call_raw(body), servers))
                self.compare(responses, "%s payload %d" % (name, index + 1))
                if index < 2 and not isinstance(responses[0], tuple):
                    raise AssertionError("malformed protobuf was accepted: %r" % (responses[0],))
                if index == 2 and isinstance(responses[0], tuple):
                    raise AssertionError("valid request with unknown field failed: %r" % (responses[0],))
        self.passed += 1
        print("PASS", name, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", required=True, help="Path to the standalone C++ binary")
    parser.add_argument("--go", help="Optional original Go binary for differential tests")
    parser.add_argument("--generated", default="build/python", help="Generated Python protobuf directory")
    parser.add_argument("--filter", default="", help="Run cases whose name contains this string")
    parser.add_argument("--keep-logs", action="store_true")
    args = parser.parse_args()
    args.cpp = str(Path(args.cpp).resolve())
    if args.go:
        args.go = str(Path(args.go).resolve())
    sys.path.insert(0, str(Path(args.generated).resolve()))
    global rls
    from envoy.service.ratelimit.v3 import rls_pb2 as rls
    runner = Runner(args)
    for case in scenarios():
        if args.filter in case[0]:
            runner.run_scenario(case)
    if args.filter in "concurrent_atomic_counters":
        runner.concurrency()
    if args.filter in "valid_and_invalid_config_reload":
        runner.reload()
    if args.filter in "malformed_grpc_payloads":
        runner.malformed_grpc()
    if not runner.passed:
        parser.error("No scenarios matched --filter")
    mode = "C++/Go differential" if args.go else "C++ smoke"
    print("%s: %d scenarios, %d request comparisons passed" % (mode, runner.passed, runner.comparisons))


if __name__ == "__main__":
    main()
