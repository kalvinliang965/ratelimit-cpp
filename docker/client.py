#!/usr/bin/env python3
"""A black-box gRPC client for the Docker validation stack.

It uses the checked-in API descriptor set and generic gRPC calls, rather than
the C++ implementation's internal classes.  In comparison mode it submits the
same protobuf request to Go and C++ concurrently and compares the complete
deterministic protobuf serialization, gRPC error result, response metadata,
and Redis key/value/TTL snapshots without normalizing any fields.
"""

import argparse
import base64
import concurrent.futures
import json
import sys
import time

import grpc
import redis
from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory


METHOD = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit"


def load_messages():
    # Importing the well-known protobuf types registers their descriptors in
    # the process-wide pool before the Envoy descriptor set is added.
    from google.protobuf import any_pb2, duration_pb2, empty_pb2, struct_pb2, timestamp_pb2, wrappers_pb2  # noqa: F401

    files = descriptor_pb2.FileDescriptorSet()
    with open("/client/reference.pb", "rb") as descriptor_file:
        files.ParseFromString(descriptor_file.read())
    pool = descriptor_pool.Default()
    pending = list(files.file)
    while pending:
        remaining = []
        progress = False
        for file_descriptor in pending:
            try:
                pool.FindFileByName(file_descriptor.name)
                progress = True
                continue
            except KeyError:
                pass
            try:
                pool.AddSerializedFile(file_descriptor.SerializeToString())
                progress = True
            except TypeError:
                remaining.append(file_descriptor)
        if not progress:
            names = ", ".join(file_descriptor.name for file_descriptor in remaining)
            raise RuntimeError("cannot load API descriptors: " + names)
        pending = remaining
    request = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("envoy.service.ratelimit.v3.RateLimitRequest"))
    response = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("envoy.service.ratelimit.v3.RateLimitResponse"))
    return request, response


Request, Response = load_messages()
OverallCode = Response.DESCRIPTOR.fields_by_name["overall_code"].enum_type.values_by_name
OK = OverallCode["OK"].number
OVER_LIMIT = OverallCode["OVER_LIMIT"].number


def request(value):
    output = Request()
    json_format.ParseDict(value, output)
    return output


def display_metadata(metadata):
    if metadata is None:
        return ()
    return tuple((key, value if isinstance(value, str) else base64.b64encode(value).decode())
                 for key, value in metadata)


def call(target, message):
    channel = grpc.insecure_channel(target)
    try:
        grpc.channel_ready_future(channel).result(timeout=20)
        rpc = channel.unary_unary(
            METHOD,
            request_serializer=lambda value: value.SerializeToString(deterministic=True),
            response_deserializer=Response.FromString,
        )
        try:
            result, rpc_call = rpc.with_call(message, timeout=10)
            return {
                "kind": "response",
                "payload": result.SerializeToString(deterministic=True),
                "initial_metadata": display_metadata(rpc_call.initial_metadata()),
                "trailing_metadata": display_metadata(rpc_call.trailing_metadata()),
            }
        except grpc.RpcError as error:
            return {
                "kind": "error",
                "code": error.code().name,
                "details": error.details(),
                "initial_metadata": display_metadata(error.initial_metadata()),
                "trailing_metadata": display_metadata(error.trailing_metadata()),
            }
    finally:
        channel.close()


def malformed_call(target):
    """Make a deliberately truncated wire request through the real RPC path."""
    channel = grpc.insecure_channel(target)
    try:
        grpc.channel_ready_future(channel).result(timeout=20)
        rpc = channel.unary_unary(
            METHOD,
            request_serializer=lambda _: b"\xff",
            response_deserializer=lambda value: value,
        )
        try:
            value, rpc_call = rpc.with_call(None, timeout=10)
            return {
                "kind": "response",
                "payload": value,
                "initial_metadata": display_metadata(rpc_call.initial_metadata()),
                "trailing_metadata": display_metadata(rpc_call.trailing_metadata()),
            }
        except grpc.RpcError as error:
            return {
                "kind": "error",
                "code": error.code().name,
                "details": error.details(),
                "initial_metadata": display_metadata(error.initial_metadata()),
                "trailing_metadata": display_metadata(error.trailing_metadata()),
            }
    finally:
        channel.close()


def response_json(result):
    if result["kind"] == "error":
        return {key: value for key, value in result.items() if key != "payload"}
    message = Response.FromString(result["payload"])
    return {
        "response": json_format.MessageToDict(message, preserving_proto_field_name=True),
        "wire_base64": base64.b64encode(result["payload"]).decode(),
        "initial_metadata": result["initial_metadata"],
        "trailing_metadata": result["trailing_metadata"],
    }


def redis_snapshot(endpoint):
    client = redis.Redis.from_url("redis://" + endpoint + "/0", decode_responses=False,
                                  socket_connect_timeout=5, socket_timeout=5)
    try:
        output = {}
        for key in sorted(client.scan_iter(match="*")):
            output[key.decode()] = {
                "value": (client.get(key) or b"").decode(),
                "ttl": client.ttl(key),
            }
        return output
    finally:
        client.close()


def flush(endpoint):
    client = redis.Redis.from_url("redis://" + endpoint + "/0", decode_responses=False,
                                  socket_connect_timeout=5, socket_timeout=5)
    try:
        client.flushall()
    finally:
        client.close()


def descriptors(*items):
    return {"domain": "docker", "descriptors": [
        {"entries": [{"key": key, "value": value} for key, value in group]}
        for group in items
    ]}


def test_vectors():
    # Ordered vectors intentionally reuse descriptors to verify Redis-backed
    # sequential accounting and a multi-descriptor request in one RPC.
    return [
        ("unknown", descriptors((("missing", "x"),))),
        ("basic-1", descriptors((("user", "alice"),))),
        ("basic-2", descriptors((("user", "alice"),))),
        ("basic-3", descriptors((("user", "alice"),))),
        ("basic-over", descriptors((("user", "alice"),))),
        ("nested-1", descriptors((("service", "api"), ("user", "bob")))),
        ("nested-2", descriptors((("service", "api"), ("user", "bob")))),
        ("nested-over", descriptors((("service", "api"), ("user", "bob")))),
        ("two-descriptors", descriptors(
            (("user", "carol"),), (("service", "api"), ("user", "dora")))),
        ("blocked", descriptors((("blocked", "x"),))),
        ("unlimited", descriptors((("unlimited", "x"),))),
        ("shadow-1", descriptors((("shadow", "x"),))),
        ("shadow-over", descriptors((("shadow", "x"),))),
        ("persistence-seed", descriptors((("persistent", "x"),))),
        ("hits-addend", {
            "domain": "docker",
            "hits_addend": 2,
            "descriptors": [{"entries": [{"key": "user", "value": "erin"}]}],
        }),
        ("hits-addend-over", {
            "domain": "docker",
            "hits_addend": 2,
            "descriptors": [{"entries": [{"key": "user", "value": "erin"}]}],
        }),
        ("empty-domain", {"descriptors": [{"entries": [{"key": "user", "value": "x"}]}]}),
        ("empty-descriptors", {"domain": "docker"}),
    ]


def assert_single_semantics(name, result):
    if name == "empty-domain":
        assert result["kind"] == "error" and result["code"] == "UNKNOWN", response_json(result)
        assert result["details"] == "rate limit domain must not be empty", response_json(result)
        return
    if name == "empty-descriptors":
        assert result["kind"] == "error" and result["code"] == "UNKNOWN", response_json(result)
        assert result["details"] == "rate limit descriptor list must not be empty", response_json(result)
        return
    assert result["kind"] == "response", response_json(result)
    response = Response.FromString(result["payload"])
    assert response.statuses, response_json(result)
    if name == "blocked":
        assert response.overall_code == OVER_LIMIT and response.statuses[0].limit_remaining == 0, response_json(result)
    if name == "basic-over" or name == "nested-over" or name == "hits-addend-over":
        assert response.overall_code == OVER_LIMIT, response_json(result)
    if name == "unlimited":
        assert response.overall_code == OK and response.statuses[0].limit_remaining == 0xFFFFFFFF, response_json(result)
    if name.startswith("shadow"):
        assert response.overall_code == OK, response_json(result)
    if name == "two-descriptors":
        assert len(response.statuses) == 2, response_json(result)


def reserve_minute_window():
    """Start the sequential fixed-window assertions just after a boundary.

    The stateful vectors deliberately expect the fourth request against a
    three-per-minute quota to be over limit.  If the first request arrives in
    the final second of a minute, that sequence legitimately spans two Redis
    keys and does not exercise that assertion.  Waiting for the next boundary
    makes the fixture deterministic without changing either request or its
    expected rate-limit semantics.
    """
    now = time.time()
    next_boundary = (int(now // 60) + 1) * 60
    time.sleep(next_boundary - now + 0.05)


def run_single(target, redis_endpoint):
    flush(redis_endpoint)
    vectors = test_vectors()
    # The unknown-descriptor request has no quota state.  Reserve a fresh
    # minute immediately before the ordered stateful sequence begins.
    name, vector = vectors[0]
    result = call(target, request(vector))
    assert_single_semantics(name, result)
    print("PASS", name, json.dumps(response_json(result), sort_keys=True), flush=True)
    reserve_minute_window()
    for name, vector in vectors[1:]:
        result = call(target, request(vector))
        assert_single_semantics(name, result)
        print("PASS", name, json.dumps(response_json(result), sort_keys=True), flush=True)
    malformed = malformed_call(target)
    assert malformed["kind"] == "error", response_json(malformed)
    print("PASS malformed", json.dumps(response_json(malformed), sort_keys=True), flush=True)
    snapshot = redis_snapshot(redis_endpoint)
    assert snapshot, "the Redis-backed requests did not create any keys"
    print("PASS Redis snapshot", json.dumps(snapshot, sort_keys=True), flush=True)


def run_persistence(target, redis_endpoint):
    """Verify a restart did not erase an already over-limit Redis counter."""
    result = call(target, request(descriptors((("persistent", "x"),))))
    assert result["kind"] == "response", response_json(result)
    response = Response.FromString(result["payload"])
    assert response.overall_code == OVER_LIMIT, response_json(result)
    snapshot = redis_snapshot(redis_endpoint)
    assert snapshot, "the restart unexpectedly removed Redis quota state"
    print("PASS persisted Redis quota after restart", json.dumps(response_json(result), sort_keys=True), flush=True)
    print("PASS persisted Redis snapshot", json.dumps(snapshot, sort_keys=True), flush=True)


def compare_pair(name, left, right):
    if left != right:
        raise AssertionError(
            "%s differs\nC++: %s\nGo: %s" %
            (name, json.dumps(response_json(left), sort_keys=True), json.dumps(response_json(right), sort_keys=True)))


def pair_call(cpp_target, go_target, vector):
    # A simultaneous dispatch keeps time-window reset durations in the same
    # epoch second while preserving completely independent Redis state.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        cpp_future = executor.submit(call, cpp_target, request(vector))
        go_future = executor.submit(call, go_target, request(vector))
        return cpp_future.result(), go_future.result()


def run_compare(cpp_target, go_target, cpp_redis, go_redis):
    flush(cpp_redis)
    flush(go_redis)
    vectors = test_vectors()
    # Keep the stateless unknown descriptor separate so the stateful fixed
    # window sequence starts immediately after a fresh minute boundary on both
    # services.  This does not alter the vector, its ordering, or the strict
    # result comparison.
    name, vector = vectors[0]
    # Avoid an epoch-second boundary for the one-second reset duration in
    # malformed/no-limit paths without modifying either result.
    if time.time() % 1 > 0.8:
        time.sleep(1.05 - (time.time() % 1))
    cpp_result, go_result = pair_call(cpp_target, go_target, vector)
    compare_pair(name, cpp_result, go_result)
    print("PASS parity", name, flush=True)
    reserve_minute_window()
    for name, vector in vectors[1:]:
        # Avoid an epoch-second boundary for the one-second reset duration in
        # malformed/no-limit paths without modifying either result.
        if time.time() % 1 > 0.8:
            time.sleep(1.05 - (time.time() % 1))
        cpp_result, go_result = pair_call(cpp_target, go_target, vector)
        compare_pair(name, cpp_result, go_result)
        print("PASS parity", name, flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        cpp_result = executor.submit(malformed_call, cpp_target).result()
        go_result = executor.submit(malformed_call, go_target).result()
    compare_pair("malformed", cpp_result, go_result)
    print("PASS parity malformed", flush=True)
    # Redis TTLs are compared as observed integers; no tolerance or rounding is
    # applied.  The calls and snapshots are concurrent to eliminate scheduler
    # skew as a source of false differences.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        cpp_snapshot = executor.submit(redis_snapshot, cpp_redis).result()
        go_snapshot = executor.submit(redis_snapshot, go_redis).result()
    if cpp_snapshot != go_snapshot:
        raise AssertionError("Redis key/value/TTL state differs\nC++: %s\nGo: %s" %
                             (json.dumps(cpp_snapshot, sort_keys=True),
                              json.dumps(go_snapshot, sort_keys=True)))
    print("PASS parity Redis key/value/TTL snapshot", json.dumps(cpp_snapshot, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("single", "persistence", "compare"), required=True)
    parser.add_argument("--target")
    parser.add_argument("--redis")
    parser.add_argument("--cpp-target")
    parser.add_argument("--go-target")
    parser.add_argument("--cpp-redis")
    parser.add_argument("--go-redis")
    args = parser.parse_args()
    if args.mode in ("single", "persistence"):
        if not args.target or not args.redis:
            parser.error(args.mode + " mode requires --target and --redis")
        if args.mode == "single":
            run_single(args.target, args.redis)
        else:
            run_persistence(args.target, args.redis)
    else:
        if not all((args.cpp_target, args.go_target, args.cpp_redis, args.go_redis)):
            parser.error("compare mode requires both targets and both Redis endpoints")
        run_compare(args.cpp_target, args.go_target, args.cpp_redis, args.go_redis)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("VALIDATION FAILED:", error, file=sys.stderr)
        raise
