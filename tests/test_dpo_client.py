"""DPO UDP socket client tests (dpo_client_test.py): exact packet bytes against an
independent model, a mock UDP peer, and optional checks against DPO's files."""

import os
from pathlib import Path

import pytest

import dpo_client_test


@pytest.fixture(scope="module")
def dpo_client(pytestconfig):
    client = Path(pytestconfig.getoption("dpo_client")).resolve()
    if not os.access(client, os.X_OK):
        pytest.skip("DPO socket client not found: %s (run `make dpo-client`)" % client)
    return str(client)


@pytest.fixture(scope="module")
def dpo_root(pytestconfig):
    root = pytestconfig.getoption("dpo_root")
    return Path(root).resolve() if root else None


def test_layout_table_is_contiguous():
    dpo_client_test.check_layout_is_contiguous()


def test_layout_matches_dpo_files(dpo_root):
    if dpo_root is None:
        pytest.skip("needs --dpo-root (files are only read as text)")
    dpo_client_test.check_against_dpo(dpo_root)


def test_request_packets(dpo_client, dpo_root):
    dpo_client_test.test_request_packets(dpo_client, dpo_root)


def test_responses(dpo_client, generated_if_available):
    # With the generated modules, --json output is also loaded as RateLimitResponse.
    dpo_client_test.test_responses(dpo_client, generated_if_available)


def test_cli(dpo_client):
    dpo_client_test.test_cli(dpo_client)
