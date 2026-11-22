#!/usr/bin/env python3
"""Exercise real gRPC servers with private Redis instances.

Without --go, checks hardcoded invariants against C++. With --go,
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
import subprocess
import tempfile
import time

import grpc
from google.protobuf import json_format

from common.support import encoded, free_port, redis_command
import parity_scenarios
# load.py also uses these through `import parity as base`.
from parity_scenarios import (descriptor, expected, expected_headers, one, policy,  # noqa: F401
                              request, rule, scenarios)


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


def describe(response):
    return repr(response) if isinstance(response, tuple) else json_format.MessageToJson(response)


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


def load_protos(generated):
    """Import the generated protobufs for this module and parity_scenarios."""
    global rls
    rls = parity_scenarios.load_protos(generated)
    return rls


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
    load_protos(args.generated)
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
