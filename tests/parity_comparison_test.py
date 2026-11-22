#!/usr/bin/env python3
"""Focused tests for parity_suite/comparison.py (no servers needed).

Equivalent Go/C++ formatting must compare equal; every other difference must
still fail.
"""

import argparse
import base64
import sys

from google.protobuf import json_format

from parity_suite import scenarios
from parity_suite.comparison import comparable

passed = 0


def response(**fields):
    base = {"overall_code": "OK", "statuses": [{"code": "OK", "limit_remaining": 4}]}
    base.update(fields)
    return json_format.ParseDict(base, scenarios.rls.RateLimitResponse())


def metadata(limit="requests_per_unit:4 unit:HOUR", entries=("meta=x",), domain="parity", **extra):
    descriptor = {"entries": list(entries), "hitsAddend": 0.0}
    if limit is not None:
        descriptor["limit"] = limit
    return {"domain": domain, "descriptors": [descriptor, {"entries": ["other=x"]}], **extra}


def same(name, left, right):
    global passed
    assert comparable(left) == comparable(right), "%s: expected equal" % name
    passed += 1
    print("PASS equal:", name)


def different(name, left, right):
    global passed
    assert comparable(left) != comparable(right), "%s: expected different" % name
    passed += 1
    print("PASS different:", name)


def test_embedded_override_text():
    cpp = response(dynamic_metadata=metadata("requests_per_unit:4 unit:HOUR"))
    same("observed Go double space", cpp, response(dynamic_metadata=metadata("requests_per_unit:4  unit:HOUR")))
    same("text-format spacing variants", cpp, response(dynamic_metadata=metadata("requests_per_unit: 4\nunit: HOUR")))
    different("different requests_per_unit", cpp, response(dynamic_metadata=metadata("requests_per_unit:5 unit:HOUR")))
    different("different unit", cpp, response(dynamic_metadata=metadata("requests_per_unit:4 unit:MINUTE")))
    different("missing unit", cpp, response(dynamic_metadata=metadata("requests_per_unit:4")))
    different("limit absent vs present", cpp, response(dynamic_metadata=metadata(None)))
    # A string that is not valid RateLimitOverride text stays byte-exact.
    different("unparseable text keeps whitespace", response(dynamic_metadata=metadata("not a  message")),
              response(dynamic_metadata=metadata("not a message")))
    different("unknown field is not parsed", response(dynamic_metadata=metadata("requests_per_unit:4 bogus:1")),
              response(dynamic_metadata=metadata("requests_per_unit:4  bogus:1")))
    different("parsed limit vs unparseable raw string",
              response(dynamic_metadata=metadata("requests_per_unit:4 unit:HOUR")),
              response(dynamic_metadata=metadata("requests_per_unit:4 unit:HOUR}")))
    different("valid override vs empty override", response(dynamic_metadata=metadata("requests_per_unit:4")),
              response(dynamic_metadata=metadata("")))
    different("parsed limit in a different descriptor",
              response(dynamic_metadata={"descriptors": [{"limit": "requests_per_unit:4"}, {}]}),
              response(dynamic_metadata={"descriptors": [{}, {"limit": "requests_per_unit:4"}]}))


def test_whitespace_elsewhere_is_strict():
    cpp = response(dynamic_metadata=metadata())
    different("entry value whitespace", cpp, response(dynamic_metadata=metadata(entries=("meta= x",))))
    different("domain trailing space", cpp, response(dynamic_metadata=metadata(domain="parity ")))
    different("limit-named key outside descriptors", response(dynamic_metadata=metadata(route={"limit": "a b"})),
              response(dynamic_metadata=metadata(route={"limit": "a  b"})))
    different("header value whitespace",
              response(response_headers_to_add=[{"key": "x-ratelimit-limit", "value": "5, 5;w=60"}]),
              response(response_headers_to_add=[{"key": "x-ratelimit-limit", "value": "5,  5;w=60"}]))
    different("raw_body whitespace", response(raw_body=base64.b64encode(b"a b").decode()),
              response(raw_body=base64.b64encode(b"a  b").decode()))


def test_other_fields_stay_strict():
    cpp = response(dynamic_metadata=metadata())
    different("limit_remaining", cpp, response(dynamic_metadata=metadata(),
                                               statuses=[{"code": "OK", "limit_remaining": 3}]))
    different("status code", response(), response(statuses=[{"code": "OVER_LIMIT", "limit_remaining": 4}]))
    different("overall code", response(), response(overall_code="OVER_LIMIT"))
    different("metadata absent vs empty Struct", response(), response(dynamic_metadata={}))
    different("descriptor order", cpp, response(dynamic_metadata={
        "domain": "parity", "descriptors": list(reversed(metadata()["descriptors"]))}))
    different("hitsAddend value", cpp, response(dynamic_metadata=metadata(hitsAddend=2.0)))
    different("quota presence", response(), response(quota={"requests": 1, "id": "q"}))
    different("duration_until_reset", response(statuses=[{"code": "OK", "duration_until_reset": "59s"}]),
              response(statuses=[{"code": "OK", "duration_until_reset": "60s"}]))
    same("identical responses", cpp, response(dynamic_metadata=metadata()))
    original = response(dynamic_metadata=metadata("requests_per_unit:4  unit:HOUR"))
    snapshot = original.SerializeToString(deterministic=True)
    comparable(original)
    assert original.SerializeToString(deterministic=True) == snapshot, "comparable() mutated its input"
    print("PASS input not mutated")


def test_grpc_errors():
    message = "grpc: error unmarshalling request: proto%scannot parse invalid wire-format data"
    space, nbsp = message % ": ", message % ": "
    same("proto: vs proto:<NBSP> prefix", ("INTERNAL", space), ("INTERNAL", nbsp))
    different("different status code", ("INTERNAL", space), ("INVALID_ARGUMENT", nbsp))
    different("different diagnostic", ("INTERNAL", space),
              ("INTERNAL", "grpc: error unmarshalling request: proto: unexpected EOF"))
    different("extra space after prefix", ("INTERNAL", space), ("INTERNAL", message % ":  "))
    different("NBSP elsewhere is not normalized", ("INTERNAL", "bad request"), ("INTERNAL", "bad request"))
    different("error vs response", ("INTERNAL", space), response())
    same("raw bytes identical", b"\x08\x01", b"\x08\x01")
    different("raw bytes differ", b"\x08\x01", b"\x08\x02")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generated", default="build/python", help="Generated Python protobuf directory")
    args = parser.parse_args()
    scenarios.load_protos(args.generated)
    test_embedded_override_text()
    test_whitespace_elsewhere_is_strict()
    test_other_fields_stay_strict()
    test_grpc_errors()
    print("parity comparison: %d checks passed" % passed)


if __name__ == "__main__":
    sys.exit(main())
