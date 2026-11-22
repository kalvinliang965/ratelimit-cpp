"""Runs parity scenarios against one or two servers and compares the results."""

import concurrent.futures
import contextlib
from pathlib import Path
import shutil
import tempfile
import time

from google.protobuf import json_format

from common.support import encoded
from . import scenarios
from .comparison import comparable
from .grpc_server import Server
from .scenarios import expected, one, policy, rule


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
        # comparable() differs from encoded() only for two Go formatting
        # choices; see parity_suite/comparison.py.
        if len(responses) > 1 and comparable(responses[0]) != comparable(responses[1]):
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
                assert sum(not isinstance(r, tuple) and r.overall_code == scenarios.rls.RateLimitResponse.OK for r in group) == 17
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
            expected(scenarios.rls.RateLimitResponse.OK, [7])(self.paired_call(servers, req, name + " before"))
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
            expected(scenarios.rls.RateLimitResponse.OK, [11])(self.paired_call(servers, req, name + " after"))
            for server in servers:
                server.write_config(policy(rule("reload", 11, unit="invalid")))
            time.sleep(0.6)
            expected(scenarios.rls.RateLimitResponse.OK, [11])(self.paired_call(servers, req, name + " invalid retained"))
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
