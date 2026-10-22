#!/usr/bin/env python3
"""Run selected unmodified Go rate-limit test vectors against both servers.

The vector data deliberately refers to configuration files in ``../ratelimit``
instead of copied policy text.  That keeps the Go tests' checked-in policies as
the source of truth while this driver supplies the implementation-neutral part:
real gRPC requests, an empty real Redis instance per implementation, and exact
wire/state comparison.

Every successful reply is compared as deterministic protobuf bytes.  Every
failure would be compared as its exact ``(gRPC code, details)`` tuple, and the
final Redis snapshot compares keys, values, and TTLs.  Nothing here imports or
calls either implementation's rate-limit internals.
"""

import argparse
import concurrent.futures
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import time


THIS_DIRECTORY = Path(__file__).resolve().parent
WORKSPACE = THIS_DIRECTORY.parents[1]
DEFAULT_VECTORS = THIS_DIRECTORY / "go_reference_vectors.json"


def load_harness(generated):
    """Load the existing process/Redis harness without invoking its CLI."""
    module_path = THIS_DIRECTORY / "integration_validation.py"
    spec = importlib.util.spec_from_file_location("go_reference_harness", module_path)
    harness = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = harness
    spec.loader.exec_module(harness)

    sys.path.insert(0, str(Path(generated).resolve()))
    from envoy.extensions.common.ratelimit.v3 import ratelimit_pb2 as rate_limit
    from envoy.service.ratelimit.v3 import rls_pb2 as rls
    from envoy.type.v3 import ratelimit_unit_pb2 as rate_limit_unit

    # integration_validation.py normally sets these globals in main().  Its
    # reusable process classes intentionally do not depend on its CLI parser.
    harness.rls = rls
    harness.rate_limit = rate_limit
    # grpcio owns the top-level ``grpc`` module, so load the generated health
    # messages exactly as integration_validation.py's CLI does.
    health_path = Path(generated).resolve() / "grpc/health/v1/health_pb2.py"
    health_spec = importlib.util.spec_from_file_location("go_reference_health_pb2", health_path)
    health = importlib.util.module_from_spec(health_spec)
    sys.modules[health_spec.name] = health
    health_spec.loader.exec_module(health)
    harness.health = health
    return harness, rls, rate_limit, rate_limit_unit


def unit_value(enum, name):
    try:
        return getattr(enum, name)
    except AttributeError as error:
        raise AssertionError("unknown rate-limit unit %r" % name) from error


def build_request(spec, rls, rate_limit, rate_limit_unit):
    request = rls.RateLimitRequest(
        domain=spec.get("domain", ""), hits_addend=int(spec.get("hits_addend", 1)))
    for descriptor_spec in spec.get("descriptors", []):
        descriptor = request.descriptors.add()
        for entry in descriptor_spec.get("entries", []):
            if isinstance(entry, dict):
                key, value = entry["key"], entry["value"]
            else:
                key, value = entry
            descriptor.entries.add(key=key, value=value)
        if "hits_addend" in descriptor_spec:
            descriptor.hits_addend.value = int(descriptor_spec["hits_addend"])
        if descriptor_spec.get("is_negative_hits", False):
            descriptor.is_negative_hits = True
        if "limit" in descriptor_spec:
            override = descriptor_spec["limit"]
            descriptor.limit.requests_per_unit = int(override["requests_per_unit"])
            descriptor.limit.unit = unit_value(rate_limit_unit, override["unit"])
    return request


def response_code(rls, name):
    try:
        return getattr(rls.RateLimitResponse, name)
    except AttributeError as error:
        raise AssertionError("unknown response code %r" % name) from error


def response_unit(rls, name):
    try:
        return getattr(rls.RateLimitResponse.RateLimit, name)
    except AttributeError as error:
        raise AssertionError("unknown response unit %r" % name) from error


def assert_expected(response, expected, rls, label):
    """Check source-test invariants in addition to the C++/Go byte compare."""
    if expected is None:
        return
    if isinstance(response, tuple):
        raise AssertionError("%s unexpectedly returned gRPC error %r" % (label, response))
    if "overall_code" in expected:
        actual = response.overall_code
        wanted = response_code(rls, expected["overall_code"])
        assert actual == wanted, "%s overall code: %r != %r" % (label, actual, wanted)
    statuses = expected.get("statuses")
    if statuses is None:
        return
    assert len(response.statuses) == len(statuses), (
        "%s status count: %d != %d" % (label, len(response.statuses), len(statuses)))
    for index, (actual, wanted) in enumerate(zip(response.statuses, statuses)):
        prefix = "%s status %d" % (label, index)
        if "code" in wanted:
            assert actual.code == response_code(rls, wanted["code"]), prefix
        if "limit_remaining" in wanted:
            assert actual.limit_remaining == wanted["limit_remaining"], prefix
        if "current_limit" in wanted:
            limit = wanted["current_limit"]
            if limit is None:
                assert not actual.HasField("current_limit"), prefix + " unexpectedly has current_limit"
            else:
                assert actual.HasField("current_limit"), prefix + " lacks current_limit"
                assert actual.current_limit.requests_per_unit == limit["requests_per_unit"], prefix
                assert actual.current_limit.unit == response_unit(rls, limit["unit"]), prefix
        if "duration_until_reset" in wanted:
            assert actual.HasField("duration_until_reset") == wanted["duration_until_reset"], prefix


def stage_source_configs(pair, files):
    """Copy the unmodified Go fixture files into each isolated runtime root."""
    sources = []
    for relative in files:
        source = (WORKSPACE / relative).resolve()
        if WORKSPACE not in source.parents or not source.is_file():
            raise AssertionError("vector config is not a workspace file: %s" % relative)
        sources.append(source)
    for service in pair.services:
        service.config_dir.mkdir(parents=True, exist_ok=True)
        for number, source in enumerate(sources):
            # Prefix preserves all fixture basenames even if a future vector
            # stages two files with the same filename.
            shutil.copyfile(source, service.config_dir / ("%02d-" % number + source.name))


def begin_second_window():
    """Start an explicitly atomic SECOND-rate sequence just after a boundary."""
    time.sleep(1.03 - (time.time() % 1))


def paired_call(harness, pair, request, label, settle):
    if settle:
        harness.settle_second()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda service: service.call(request), pair.services))
    harness.compare(responses, label)
    return responses[0]


def run_vector(harness, rls, rate_limit, rate_limit_unit, args, vector):
    name = vector["id"]
    environment = vector.get("environment", {})
    # create_config=False is intentional: these are the original Go YAML
    # fixtures, staged byte-for-byte rather than re-encoded through Python.
    with harness.paired_servers(args, name, None, environment, create_config=False, start=False) as pair:
        stage_source_configs(pair, vector["config_files"])
        pair.start()
        atomic = vector.get("atomic_second_window", False)
        if atomic:
            begin_second_window()
        calls = 0
        for step_number, step in enumerate(vector["steps"], 1):
            request = build_request(step["request"], rls, rate_limit, rate_limit_unit)
            repeats = int(step.get("repeat", 1))
            assert repeats > 0, "%s step %d has non-positive repeat" % (name, step_number)
            for repeat in range(repeats):
                label = "%s step %d/%d call %d/%d" % (name, step_number, len(vector["steps"]), repeat + 1, repeats)
                response = paired_call(harness, pair, request, label, settle=not atomic)
                # An expectation can apply to every repeat, or selected first
                # / final calls.  The source Go test's full progression still
                # goes over the wire and is byte-compared on every call.
                when = step.get("expect_when", "every")
                if when == "every" or (when == "first" and repeat == 0) or (
                        when == "last" and repeat == repeats - 1):
                    assert_expected(response, step.get("expect"), rls, label)
                calls += 1
        snapshot = pair.snapshot(name + " final Redis state")
        if "expected_redis_keys" in vector:
            assert len(snapshot) == vector["expected_redis_keys"], (
                "%s Redis key count: %d != %d" %
                (name, len(snapshot), vector["expected_redis_keys"]))
    print("PASS %s (%d real gRPC calls; %s)" % (name, calls, vector["source"]), flush=True)
    return calls


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", required=True, help="C++ standalone server binary")
    parser.add_argument("--go", required=True, help="original Go service binary")
    parser.add_argument("--generated", default="build/python", help="generated Python protobuf modules")
    parser.add_argument("--vectors", default=str(DEFAULT_VECTORS), help="JSON vector corpus")
    parser.add_argument("--filter", default="", help="only run vector IDs containing this text")
    parser.add_argument("--keep-logs", action="store_true")
    args = parser.parse_args()
    args.cpp, args.go = str(Path(args.cpp).resolve()), str(Path(args.go).resolve())

    with open(args.vectors, encoding="utf-8") as source:
        corpus = json.load(source)
    if corpus.get("format") != "ratelimit-go-black-box-v1":
        parser.error("unsupported vector format in %s" % args.vectors)
    harness, rls, rate_limit, rate_limit_unit = load_harness(args.generated)
    vectors = [vector for vector in corpus["vectors"] if args.filter in vector["id"]]
    if not vectors:
        parser.error("no Go reference vectors matched --filter")
    total = sum(run_vector(harness, rls, rate_limit, rate_limit_unit, args, vector)
                for vector in vectors)
    print("Go-reference black-box vectors: %d scenarios, %d byte-compared gRPC calls passed" %
          (len(vectors), total))


if __name__ == "__main__":
    main()
