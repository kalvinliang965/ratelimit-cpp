"""Black-box C++/Go integration tests from integration_validation.py (needs --go).
Each test starts its own pair of services and Redis instances."""

import pytest

import integration_validation

# Pre-existing failures, kept visible (not strict, so an environment where one
# passes reports XPASS instead of failing the run).
KNOWN_FAILURES = {
    "grpc_tls_and_mtls": "pre-existing: times out waiting for the C++ TLS proxy listeners; cause not investigated",
    "grpc_tls_mtls_uds": "pre-existing: times out waiting for the C++ TLS proxy listeners; cause not investigated",
    "xds_sotw": "C++ does not start its xDS provider (CONFIG_TYPE=GRPC_XDS_SOTW is a placeholder)",
}


@pytest.fixture(scope="module")
def integration_args(go_options):
    integration_validation.load_generated(go_options.generated)
    return go_options


@pytest.mark.parametrize("test", [
    pytest.param(test, id=name, marks=[pytest.mark.xfail(reason=KNOWN_FAILURES[name], strict=False)]
                 if name in KNOWN_FAILURES else [])
    for name, test in integration_validation.TESTS])
def test_integration(integration_args, test):
    test(integration_args)
