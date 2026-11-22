"""Unit tests for parity_suite/comparison.py (no servers)."""

import pytest

from parity_comparison_test import (  # noqa: F401  (collected by pytest)
    test_embedded_override_text,
    test_grpc_errors,
    test_other_fields_stay_strict,
    test_whitespace_elsewhere_is_strict,
)


@pytest.fixture(autouse=True)
def _generated_protos(protos):
    """The checks build RateLimitResponse messages from the generated modules."""
