#!/usr/bin/env python3
"""Real gRPC/Redis concurrency and restart validation.

The checks intentionally use the executable server and separate client
connections.  When a Go binary is supplied each C++ and Go instance receives
the same policy and an equivalent, isolated Redis state.  Responses are
compared as deterministic protobuf bytes; concurrent calls are compared as a
multiset because Redis decides the winning request order.
"""

import argparse
import concurrent.futures
import contextlib
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import grpc

TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS))
import parity as base  # noqa: E402


class ManagedServer(base.Server):
    """The parity fixture with controlled application and Redis restarts."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_process = None
        self.app_env = None
        self.app_starts = 0
        self.redis_processes = {}

    def launch(self, command, name, env=None):
        process = super().launch(command, name, env)
        if command and command[0] == "redis-server":
            port = int(command[command.index("--port") + 1])
            self.redis_processes[port] = process
        elif command and Path(command[0]).resolve() == Path(self.binary).resolve():
            self.app_process = process
            self.app_env = env.copy()
        return process

    def _connect_app(self, process):
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % self.port)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("Server exited with code %s" % process.returncode)
            try:
                grpc.channel_ready_future(self.channel).result(timeout=0.2)
                self.rpc = self.channel.unary_unary(
                    "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
                    request_serializer=base.rls.RateLimitRequest.SerializeToString,
                    response_deserializer=base.rls.RateLimitResponse.FromString)
                return
            except grpc.FutureTimeoutError:
                pass
        raise AssertionError("gRPC server did not become ready after restart")

    def start(self):
        super().start()
        self._connect_app(self.app_process)
        return self

    def _stop_process(self, process, label):
        if not process or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.returncode is None:
            raise AssertionError("%s did not stop" % label)

    def restart_app(self):
        if self.channel:
            self.channel.close()
            self.channel = None
        self._stop_process(self.app_process, "application")
        self.app_starts += 1
        process = self.launch([self.binary], "server-restart-%d" % self.app_starts, self.app_env)
        self._connect_app(process)

    def stop_redis(self, port):
        self._stop_process(self.redis_processes[port], "redis")

    def restart_redis(self, port):
        self.stop_redis(port)
        process = self.launch(
            ["redis-server", "--bind", "127.0.0.1", "--port", str(port),
             "--save", "", "--appendonly", "no", "--loglevel", "warning"],
            "redis-restart-%d" % port)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                if base.redis_command(port, "PING") == "PONG":
                    self.redis_processes[port] = process
                    return
            except OSError:
                time.sleep(0.025)
        raise AssertionError("Redis did not become ready after restart")

    def call_on_new_channel(self, request):
        """Use a distinct client transport to model independent callers."""
        channel = grpc.insecure_channel("127.0.0.1:%d" % self.port)
        rpc = channel.unary_unary(
            "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
            request_serializer=base.rls.RateLimitRequest.SerializeToString,
            response_deserializer=base.rls.RateLimitResponse.FromString)
        try:
            return rpc(request, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())
        finally:
            channel.close()

    def snapshot_with_ttls(self):
        return [dict((key, (base.redis_command(port, "GET", key),
                            base.redis_command(port, "TTL", key)))
                     for key in sorted(base.redis_command(port, "KEYS", "*")))
                for port in self.redis_ports]

    def sanitizer_errors(self):
        errors = []
        for log in self.directory.glob("server*.log"):
            text = log.read_text(errors="replace")
            for marker in ("ERROR: AddressSanitizer", "ERROR: ThreadSanitizer", "runtime error:"):
                if marker in text:
                    errors.append((log, marker, text))
        return errors


class Runner:
    def __init__(self, args):
        self.args = args
        self.comparisons = 0
        self.passed = 0

    @contextlib.contextmanager
    def servers(self, name, config, settings=None):
        directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-load-%s-" % name))
        binaries = [self.args.cpp] + ([self.args.go] if self.args.go else [])
        servers = []
        successful = False
        try:
            for label, binary in zip(["cpp", "go"], binaries):
                server = ManagedServer(binary, directory / label, config, settings or {})
                servers.append(server.start())
            yield servers
            for server in servers:
                errors = server.sanitizer_errors()
                if errors:
                    log, marker, text = errors[0]
                    raise AssertionError("%s in %s:\n%s" % (marker, log, text))
            successful = True
        finally:
            for server in reversed(servers):
                server.close()
            if successful and not self.args.keep_logs:
                shutil.rmtree(directory)
            else:
                print("  Logs:", directory, flush=True)

    def exact(self, outcomes, label):
        self.comparisons += 1
        if len(outcomes) > 1 and base.encoded(outcomes[0]) != base.encoded(outcomes[1]):
            raise AssertionError("%s\nC++: %s\nGo: %s" %
                                 (label, base.describe(outcomes[0]), base.describe(outcomes[1])))
        return outcomes[0]

    def paired_call(self, servers, request, label, settle=True):
        if settle:
            base.settle_second()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as pool:
            outcomes = list(pool.map(lambda server: server.call(request), servers))
        return self.exact(outcomes, label)

    def paired_action(self, servers, action):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as pool:
            list(pool.map(action, servers))

    def burst(self, server, request, clients=16, calls_per_client=4):
        """Make concurrent calls through independent gRPC client channels."""
        barrier = threading.Barrier(clients)

        def client(_):
            channel = grpc.insecure_channel("127.0.0.1:%d" % server.port)
            rpc = channel.unary_unary(
                "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
                request_serializer=base.rls.RateLimitRequest.SerializeToString,
                response_deserializer=base.rls.RateLimitResponse.FromString)
            try:
                grpc.channel_ready_future(channel).result(timeout=5)
                barrier.wait(timeout=5)
                answers = []
                for _ in range(calls_per_client):
                    try:
                        answers.append(rpc(request, timeout=5))
                    except grpc.RpcError as error:
                        answers.append((error.code().name, error.details()))
                return answers
            finally:
                channel.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
            return [answer for group in pool.map(client, range(clients)) for answer in group]

    def paired_burst(self, servers, request, label):
        base.settle_second()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as pool:
            groups = list(pool.map(
                lambda server: self.burst(server, request, self.args.clients, self.args.calls_per_client),
                servers))
        for group in groups:
            if any(isinstance(answer, tuple) for answer in group):
                raise AssertionError("%s returned an RPC error: %r" % (label, group))
        if len(groups) > 1:
            left, right = sorted(map(base.encoded, groups[0])), sorted(map(base.encoded, groups[1]))
            if left != right:
                raise AssertionError("%s: concurrent protobuf response multisets differ" % label)
        self.comparisons += len(groups[0])
        return groups[0]

    def health(self):
        name = "health_check_watch_list"
        with self.servers(name, base.policy(base.rule("health", 4))) as servers:
            # Generated Python message modules deliberately do not include a
            # grpc stub; construct the standard health RPCs over the wire.
            def check(server, service):
                channel = grpc.insecure_channel("127.0.0.1:%d" % server.port)
                rpc = channel.unary_unary(
                    "/grpc.health.v1.Health/Check",
                    request_serializer=health.HealthCheckRequest.SerializeToString,
                    response_deserializer=health.HealthCheckResponse.FromString)
                try:
                    return rpc(health.HealthCheckRequest(service=service), timeout=5)
                except grpc.RpcError as error:
                    return (error.code().name, error.details())
                finally:
                    channel.close()

            for service in ("", "ratelimit", "missing"):
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as pool:
                    self.exact(list(pool.map(lambda server: check(server, service), servers)),
                               "%s Check(%r)" % (name, service))
            # Check a true server-streaming method, including the initial
            # state.  This runs against a live service, not a direct class call.
            def watch_initial(server):
                channel = grpc.insecure_channel("127.0.0.1:%d" % server.port)
                rpc = channel.unary_stream(
                    "/grpc.health.v1.Health/Watch",
                    request_serializer=health.HealthCheckRequest.SerializeToString,
                    response_deserializer=health.HealthCheckResponse.FromString)
                call = rpc(health.HealthCheckRequest(service="ratelimit"), timeout=5)
                try:
                    return next(call)
                finally:
                    call.cancel()
                    channel.close()
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as pool:
                self.exact(list(pool.map(watch_initial, servers)), "%s Watch initial" % name)
        self.passed += 1
        print("PASS", name, flush=True)

    def atomic_multi_client_load(self):
        name = "atomic_multiclient_multidescriptor_load"
        requests_per_round = self.args.clients * self.args.calls_per_client
        total_requests = self.args.rounds * requests_per_round
        quota = total_requests // 2 + 2
        config = base.policy(base.rule("shared", quota, "hour"),
                             base.rule("companion", total_requests + 100, "hour"))
        request = base.request(base.descriptor(("shared", "same")),
                               base.descriptor(("companion", "same")))
        with self.servers(name, config) as servers:
            answers = []
            for round_number in range(self.args.rounds):
                answers.extend(self.paired_burst(servers, request, "%s round %d" % (name, round_number + 1)))
            ok = sum(answer.overall_code == base.rls.RateLimitResponse.OK for answer in answers)
            if ok != quota:
                raise AssertionError("expected %d allowed requests, received %d" % (quota, ok))
            if any(answer.statuses[1].code != base.rls.RateLimitResponse.OK for answer in answers):
                raise AssertionError("companion descriptor unexpectedly exceeded its limit")
            snapshots = [server.snapshot_with_ttls() for server in servers]
            for snapshot in snapshots:
                values = sorted(value for database in snapshot for value, _ in database.values())
                if values != [str(total_requests), str(total_requests)]:
                    raise AssertionError("lost or duplicate Redis increments: %r" % snapshot)
            if len(snapshots) > 1 and snapshots[0] != snapshots[1]:
                raise AssertionError("Redis values or TTLs differ: %r" % snapshots)
        self.passed += 1
        print("PASS", name, "(%d requests / %d independent clients / two descriptors)" %
              (total_requests, self.args.clients), flush=True)

    def expiry_and_service_restart(self):
        name = "expiry_and_service_restart_preserves_redis_quota"
        with self.servers(name, base.policy(base.rule("persist", 10, "hour"))) as servers:
            used = base.one("persist")
            for index in range(4):
                self.paired_call(servers, used, "%s pre-restart request %d" % (name, index + 1))
            self.paired_action(servers, lambda server: server.restart_app())
            probe = base.one("persist", hits=0)
            outcome = self.paired_call(servers, probe, "%s post-restart probe" % name)
            if outcome.statuses[0].limit_remaining != 6:
                raise AssertionError("quota did not survive service restart: %s" % base.describe(outcome))
            outcome = self.paired_call(servers, used, "%s post-restart increment" % name)
            if outcome.statuses[0].limit_remaining != 5:
                raise AssertionError("post-restart increment was incorrect: %s" % base.describe(outcome))
            snapshots = [server.snapshot() for server in servers]
            if len(snapshots) > 1 and snapshots[0] != snapshots[1]:
                raise AssertionError("Redis state differs after service restart: %r" % snapshots)
        self.passed += 1
        print("PASS", name, flush=True)

        name = "expired_redis_key_reopens_quota"
        with self.servers(name, base.policy(base.rule("expiry", 2, "second"))) as servers:
            self.paired_call(servers, base.one("expiry"), "%s initial" % name)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and any(server.snapshot() != [{}] for server in servers):
                time.sleep(0.05)
            if any(server.snapshot() != [{}] for server in servers):
                raise AssertionError("second-window Redis key did not expire")
            outcome = self.paired_call(servers, base.one("expiry"), "%s after expiry" % name)
            if outcome.statuses[0].limit_remaining != 1:
                raise AssertionError("expired quota did not reset: %s" % base.describe(outcome))
        self.passed += 1
        print("PASS", name, flush=True)

    def redis_restart(self):
        name = "redis_restart_error_and_recovery"
        settings = {"REDIS_TIMEOUT": "500ms"}
        with self.servers(name, base.policy(base.rule("recover", 5, "hour")), settings) as servers:
            # Use an explicitly serialized request-level addend, matching the
            # normal Envoy client path while the Redis pool reconnects.
            request = base.request(base.descriptor(("recover", "x")), hits=1)
            self.paired_call(servers, request, "%s initial" % name)
            self.paired_action(servers, lambda server: server.stop_redis(server.redis_ports[0]))
            # The exact tuple comparison includes status and error text. Both
            # services have an established connection here, so a closed Redis
            # socket should expose the same EOF error without endpoint-specific
            # address text.
            failure = self.paired_call(servers, request, "%s unavailable" % name)
            if not isinstance(failure, tuple) or failure[0] != "UNKNOWN":
                raise AssertionError("Redis outage did not return UNKNOWN: %r" % (failure,))
            # Start each real Redis instance and give its production client
            # pool a short, deterministic recovery interval.  A bare Redis
            # PING only proves the listener is up; it does not guarantee that
            # Go's connection pool has discarded the just-closed socket.
            # Responses after this interval are still compared byte-for-byte.
            for server in servers:
                server.restart_redis(server.redis_ports[0])
            time.sleep(1.2)
            outcome = self.paired_call(servers, request, "%s recovered" % name)
            if isinstance(outcome, tuple) or outcome.overall_code != base.rls.RateLimitResponse.OK:
                raise AssertionError("service did not recover after Redis restart: %r" % (outcome,))
        self.passed += 1
        print("PASS", name, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", required=True, help="C++ service binary")
    parser.add_argument("--go", help="Optional original Go reference binary")
    parser.add_argument("--generated", default="build/python", help="Generated Python protobuf directory")
    parser.add_argument("--keep-logs", action="store_true")
    parser.add_argument("--rounds", type=int, default=4,
                        help="Concurrent request rounds in the load scenario (default: 4)")
    parser.add_argument("--clients", type=int, default=16,
                        help="Independent gRPC clients per load round (default: 16)")
    parser.add_argument("--calls-per-client", type=int, default=4,
                        help="RPCs each client makes in a load round (default: 4)")
    args = parser.parse_args()
    args.cpp = str(Path(args.cpp).resolve())
    if args.go:
        args.go = str(Path(args.go).resolve())
    if args.rounds < 1 or args.clients < 1 or args.calls_per_client < 1:
        parser.error("--rounds, --clients, and --calls-per-client must be positive")
    sys.path.insert(0, str(Path(args.generated).resolve()))
    global health
    from envoy.service.ratelimit.v3 import rls_pb2 as rls
    # `grpc` is already the installed grpcio package, so Python cannot import
    # a generated `grpc.health` namespace below it in the usual way.  Load
    # this dependency-free generated module by path; its descriptors and
    # message classes are otherwise identical to a normal import.
    health_path = Path(args.generated).resolve() / "grpc/health/v1/health_pb2.py"
    spec = importlib.util.spec_from_file_location("ratelimit_health_pb2", health_path)
    health = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(health)
    base.rls = rls
    runner = Runner(args)
    runner.health()
    runner.atomic_multi_client_load()
    runner.expiry_and_service_restart()
    runner.redis_restart()
    mode = "C++/Go differential" if args.go else "C++"
    print("%s concurrency/restart validation: %d scenarios, %d exact response comparisons passed" %
          (mode, runner.passed, runner.comparisons))


if __name__ == "__main__":
    main()
