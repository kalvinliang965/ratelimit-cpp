"""Concurrency, restart and Redis-recovery scenarios from load.py (C++ only, or
C++/Go with --go).  Each test starts its own servers and Redis."""

import pytest

import load


@pytest.fixture(scope="module")
def load_runner(options, report_comparisons):
    load.load_generated(options.generated)
    label = "load (C++/Go)" if options.go else "load (C++ only)"
    return report_comparisons(label, load.Runner(options))


def test_health_check_watch_list(load_runner):
    load_runner.health()


def test_atomic_multi_client_load(load_runner):
    load_runner.atomic_multi_client_load()


def test_expiry_and_service_restart(load_runner):
    # Also covers expired_redis_key_reopens_quota (one load.py method).
    load_runner.expiry_and_service_restart()


def test_redis_restart_error_and_recovery(load_runner):
    load_runner.redis_restart()
