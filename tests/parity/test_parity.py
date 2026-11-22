"""Parity scenarios from parity_suite.  Each scenario starts its own servers and
Redis (scenarios differ in policy and environment).  With --go every response,
gRPC error and final Redis state is compared byte-for-byte with Go; without it,
the scenario's own checks run against C++ alone (like `make test-smoke`).

    uv run --locked pytest tests/test_parity.py --go=build/ratelimit-go -k metadata
"""

import pytest

from parity_suite.runner import Runner


@pytest.fixture(scope="session")
def parity_runner(options, report_comparisons):
    label = "parity (C++/Go)" if options.go else "parity (C++ only)"
    return report_comparisons(label, Runner(options))


def test_scenario(parity_runner, parity_case):
    parity_runner.run_scenario(parity_case)


def test_concurrent_atomic_counters(parity_runner):
    parity_runner.concurrency()


def test_valid_and_invalid_config_reload(parity_runner):
    parity_runner.reload()


def test_malformed_grpc_payloads(parity_runner):
    parity_runner.malformed_grpc()
