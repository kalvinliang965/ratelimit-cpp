"""Redis Cluster and Sentinel C++/Go parity (backend_modes_docker.py).  Opt-in:
needs --go, --docker-backends, a Docker daemon, and `make backend-client-image`."""

import shutil

import pytest

import backend_modes_docker


@pytest.fixture(scope="module")
def backend_args(pytestconfig, go_options):
    if not pytestconfig.getoption("docker_backends"):
        pytest.skip("starts Docker containers; run with --docker-backends")
    if not shutil.which("docker"):
        pytest.skip("docker CLI not found")
    backend_modes_docker.load_generated(go_options.generated)
    return go_options


@pytest.mark.parametrize("mode", ["cluster", "sentinel"])
def test_backend_mode(backend_args, mode):
    backend_modes_docker.run_mode(backend_args, mode)
