"""pytest configuration for the ratelimit-cpp test modules (tests/test_*.py).

Binaries and the generated protobuf directory come from command-line options
with the Makefile's paths as defaults.  Every test starts the real processes
it needs; only these options and the generated modules are shared.

    uv run --locked pytest                                   # C++ only
    uv run --locked pytest --go=build/ratelimit-go           # plus C++/Go parity
"""

import argparse
import os
from pathlib import Path

import pytest

from parity_suite import scenarios

PROTOS_ERROR = pytest.StashKey[str]()
RUNNERS = pytest.StashKey[dict]()


def pytest_addoption(parser):
    group = parser.getgroup("ratelimit-cpp")
    group.addoption("--cpp", default="build/ratelimit-cpp", help="C++ service binary (default: %(default)s)")
    group.addoption("--go", default=None,
                    help="Go reference binary; without it, C++/Go comparisons and Go-only tests are skipped")
    group.addoption("--generated", default="build/python",
                    help="generated Python protobuf directory from `make python-protos` (default: %(default)s)")
    group.addoption("--keep-logs", action="store_true", help="keep server logs of passing tests too")
    group.addoption("--dpo-client", default="build/ratelimit-client-socket",
                    help="DPO socket client binary from `make dpo-client` (default: %(default)s)")
    group.addoption("--dpo-root", default=None,
                    help="smartnic-generic-rpc checkout; ratelimit.dpo and the generated header are read as text")
    group.addoption("--docker-backends", action="store_true",
                    help="run the Redis Cluster/Sentinel tests, which start Docker containers")


def pytest_configure(config):
    # Load the generated modules once, before collection.  If they are missing,
    # record why instead of failing collection; dependent tests then skip.
    generated = Path(config.getoption("generated")).resolve()
    error = None
    if not (generated / "envoy").is_dir():
        error = "generated protobuf modules not found in %s (run `make python-protos`)" % generated
    else:
        try:
            scenarios.load_protos(generated)
        except ImportError as exception:
            error = "cannot import generated protobuf modules from %s: %s" % (generated, exception)
    config.stash[PROTOS_ERROR] = error
    config.stash[RUNNERS] = {}


def pytest_generate_tests(metafunc):
    # One test per parity scenario, with the scenario name as its id.
    if "parity_case" not in metafunc.fixturenames:
        return
    error = metafunc.config.stash[PROTOS_ERROR]
    if error:
        params = [pytest.param(None, id="unavailable", marks=pytest.mark.skip(reason=error))]
    else:
        params = [pytest.param(case, id=case[0]) for case in scenarios.scenarios()]
    metafunc.parametrize("parity_case", params)


def pytest_terminal_summary(terminalreporter, config):
    for label, runner in config.stash.get(RUNNERS, {}).items():
        terminalreporter.write_line("%s: %d exact response comparisons" % (label, runner.comparisons))


@pytest.fixture(scope="session")
def protos(pytestconfig):
    error = pytestconfig.stash[PROTOS_ERROR]
    if error:
        pytest.skip(error)
    return scenarios.rls


@pytest.fixture(scope="session")
def generated_if_available(pytestconfig):
    """The generated protobuf directory, or None when it could not be loaded."""
    if pytestconfig.stash[PROTOS_ERROR]:
        return None
    return str(Path(pytestconfig.getoption("generated")).resolve())


@pytest.fixture(scope="session")
def options(pytestconfig, protos):
    """The argparse-style options the existing suites take."""
    cpp = Path(pytestconfig.getoption("cpp")).resolve()
    if not os.access(cpp, os.X_OK):
        pytest.skip("C++ service binary not found: %s (run `make`)" % cpp)
    go = pytestconfig.getoption("go")
    if go:
        go = Path(go).resolve()
        if not os.access(go, os.X_OK):
            raise pytest.UsageError("--go is not an executable: %s" % go)
    return argparse.Namespace(
        cpp=str(cpp), go=str(go) if go else None,
        generated=str(Path(pytestconfig.getoption("generated")).resolve()),
        keep_logs=pytestconfig.getoption("keep_logs"),
        client_image=os.environ.get("BACKEND_MODES_CLIENT_IMAGE", "ratelimit-cpp-validation-client:latest"),
        rounds=4, clients=16, calls_per_client=4)  # load.py defaults


@pytest.fixture(scope="session")
def go_options(options):
    if not options.go:
        pytest.skip("needs --go (the Go reference binary)")
    return options


@pytest.fixture(scope="session")
def report_comparisons(pytestconfig):
    """Register a runner whose comparison count is printed after the run."""
    def register(label, runner):
        pytestconfig.stash[RUNNERS][label] = runner
        return runner
    return register
