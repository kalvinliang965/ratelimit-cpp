#!/usr/bin/env python3
"""Exercise real gRPC servers with private Redis instances.

Without --go, checks hardcoded invariants against C++. With --go,
also compares *every field* of each response (including field presence, headers,
durations, metadata and ordering), gRPC errors, and final Redis keys/counters.
No response fields are stripped or normalized to hide implementation differences.
"""

import argparse
from pathlib import Path

from parity_suite import scenarios as _scenarios
from parity_suite.runner import Runner
# load.py uses these through `import parity as base`.
from common.support import encoded, free_port, redis_command  # noqa: F401
from parity_suite.grpc_server import Server  # noqa: F401
from parity_suite.runner import begin_second_window, describe, settle_second  # noqa: F401
from parity_suite.scenarios import (descriptor, expected, expected_headers, load_protos, one,  # noqa: F401
                                    policy, request, rule, scenarios)


def __getattr__(name):
    # parity.rls is the one generated module owned by parity_suite.scenarios.
    if name == "rls":
        return _scenarios.rls
    raise AttributeError("module %r has no attribute %r" % (__name__, name))


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
