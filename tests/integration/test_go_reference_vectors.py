"""Unmodified Go test fixtures run against real C++ and Go services
(go_reference_vectors.py; needs --go and the third_party/ratelimit submodule)."""

import json

import pytest

import go_reference_vectors

CORPUS = json.loads(go_reference_vectors.DEFAULT_VECTORS.read_text(encoding="utf-8"))
assert CORPUS.get("format") == "ratelimit-go-black-box-v1", "unsupported vector format"


@pytest.fixture(scope="module")
def harness(go_options):
    return go_reference_vectors.load_harness(go_options.generated)


@pytest.mark.parametrize("vector", CORPUS["vectors"], ids=[vector["id"] for vector in CORPUS["vectors"]])
def test_go_reference_vector(go_options, harness, vector):
    module, rls, rate_limit, rate_limit_unit = harness
    assert go_reference_vectors.run_vector(module, rls, rate_limit, rate_limit_unit, go_options, vector) > 0
