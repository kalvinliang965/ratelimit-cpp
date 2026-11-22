"""C++ Redis pool tests for idle connections Redis closed (redis_pool_test.py).
Linux only: they read /proc and need a TLS-capable redis-server and openssl."""

import sys

import pytest

import redis_pool_test

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc (Linux only)")


@pytest.mark.parametrize("test, tls", [pytest.param(test, tls, id=name)
                                       for name, test, tls in redis_pool_test.TESTS_TO_RUN])
def test_redis_pool(options, request, test, tls):
    with redis_pool_test.Fixture(options, request.node.callspec.id, tls) as fixture:
        test(fixture)
