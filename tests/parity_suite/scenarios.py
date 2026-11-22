"""Rate-limit parity scenarios and the builders and checks they use.

Call load_protos() before building requests or iterating scenarios().
"""

from pathlib import Path
import sys

# Generated rls_pb2 module; set by load_protos() from the --generated path.
rls = None


def load_protos(generated):
    global rls
    sys.path.insert(0, str(Path(generated).resolve()))
    from envoy.service.ratelimit.v3 import rls_pb2
    rls = rls_pb2
    return rls


def rule(key, count=3, unit="hour", value=None, **extra):
    result = {"key": key, "rate_limit": {"unit": unit, "requests_per_unit": count}}
    if value is not None:
        result["value"] = value
    result.update(extra)
    return result


def policy(*rules, domain="parity"):
    return {"domain": domain, "descriptors": list(rules)}


def descriptor(*entries, hits=None, negative=False, limit=None):
    result = {"entries": [{"key": key, "value": value} for key, value in entries]}
    if hits is not None:
        result["hits_addend"] = {"value": hits}
    if negative:
        result["is_negative_hits"] = True
    if limit is not None:
        result["limit"] = {"requests_per_unit": limit[0], "unit": limit[1]}
    return result


def request(*descriptors, domain="parity", hits=0):
    return rls.RateLimitRequest(domain=domain, descriptors=descriptors, hits_addend=hits)


def one(key, value="x", **kw):
    return request(descriptor((key, value), **kw))


def expected(code=None, remaining=None, status_codes=None):
    def check(response):
        if isinstance(response, tuple):
            raise AssertionError("Unexpected gRPC error: %r" % (response,))
        if code is not None:
            assert response.overall_code == code, ("overall_code", response)
        if remaining is not None:
            assert [s.limit_remaining for s in response.statuses] == remaining, response
        if status_codes is not None:
            assert [s.code for s in response.statuses] == status_codes, response
    return check


def expected_headers(kind):
    """Check header contents independently of an optional Go reference server."""
    def check(response):
        expected(rls.RateLimitResponse.OK, [8])(response)
        response_headers = list(response.response_headers_to_add)
        request_headers = list(response.request_headers_to_add)
        headers = request_headers if kind == "request" else response_headers
        unused = response_headers if kind == "request" else request_headers
        assert not unused, ("unexpected headers in the other direction", unused)
        if kind == "per_unit":
            wanted = {"RateLimit-Limit-Hours": "9", "RateLimit-Remaining-Hours": "8"}
        else:
            prefix = "x-request-" if kind == "request" else "x-response-"
            wanted = {prefix + "limit": "9", prefix + "remaining": "8", prefix + "reset": None}
        # Check the length before constructing a dict, so duplicate keys cannot
        # hide an incorrectly appended or overwritten header.
        assert len(headers) == len(wanted), (kind, "header count", headers, wanted)
        actual = {header.key: header.value for header in headers}
        assert set(actual) == set(wanted), (kind, "header names", actual, wanted)
        for key, value in wanted.items():
            if value is None:
                assert actual[key].isdigit() and 1 <= int(actual[key]) <= 3600, (key, actual[key])
            else:
                assert actual[key] == value, (key, actual[key], value)
    return check


def scenarios():
    ok, over = rls.RateLimitResponse.OK, rls.RateLimitResponse.OVER_LIMIT
    # These assertions run under make test, without requiring --go.
    for kind, enabled in [("response", "LIMIT_RESPONSE_HEADERS_ENABLED"),
                          ("request", "LIMIT_REQUEST_HEADERS_ENABLED"),
                          ("per_unit", "LIMIT_PER_UNIT_HEADERS_ENABLED")]:
        settings = {"LIMIT_RESPONSE_HEADERS_ENABLED": "false",
                    "LIMIT_REQUEST_HEADERS_ENABLED": "false",
                    "LIMIT_PER_UNIT_HEADERS_ENABLED": "false",
                    "LIMIT_LIMIT_HEADER": "x-response-limit",
                    "LIMIT_REMAINING_HEADER": "x-response-remaining",
                    "LIMIT_RESET_HEADER": "x-response-reset",
                    "LIMIT_REQUEST_LIMIT_HEADER": "x-request-limit",
                    "LIMIT_REQUEST_REMAINING_HEADER": "x-request-remaining",
                    "LIMIT_REQUEST_RESET_HEADER": "x-request-reset"}
        settings[enabled] = "true"
        yield ("header_entries_" + kind, policy(rule("headers", 9, unit="hour")),
               settings, [(one("headers"), expected_headers(kind))])

    basic = policy(rule("basic"), rule("blocked", 0),
                   rule("unlimited", rate_limit={"unlimited": True}),
                   rule("shadow", 1, shadow_mode=True))
    yield ("basic_errors_unlimited_shadow", basic, {}, [
        (request(domain=""), ("UNKNOWN", "rate limit domain must not be empty")),
        (request(), ("UNKNOWN", "rate limit descriptor list must not be empty")),
        (one("missing"), expected(ok, [0])),
        (request(descriptor(), domain="unknown"), expected(ok, [0])),
        (request(descriptor(("basic", "x")), domain="unknown"), expected(ok, [0])),
        *[(one("basic"), expected(ok if i < 3 else over, [max(2-i, 0)])) for i in range(5)],
        (one("blocked"), expected(over, [0])),
        (one("unlimited"), expected(ok, [4294967295])),
        (one("shadow"), expected(ok, [0])), (one("shadow"), expected(ok, [0])),
    ])
    # Exercise both ends of the configured uint32 range through Redis and the
    # gRPC response fields.  In particular, the second request crosses the
    # maximum configured threshold rather than overflowing a local counter.
    yield "zero_and_maximum_limits", policy(
        rule("zero", 0), rule("maximum", 4294967295)), {}, [
            (one("zero"), expected(over, [0])),
            (one("maximum"), expected(ok, [4294967294])),
            (one("maximum", hits=4294967295), expected(over, [0])),
        ]
    matching = policy(
        rule("user", 3), rule("user", 1, value="specific"),
        rule("path", 2, value="/api/*"), rule("middle", 2, value="a*b*c"),
        rule("suffix", 2, value="*tail"), rule("shared", 2, value="team-*", share_threshold=True),
        {"key": "app", "value": "web", "descriptors": [rule("user", 2), rule("user", 1, value="vip")]},
        rule("depth", 1),
    )
    steps = [(one("user", "specific"), expected(ok, [0])),
             (one("user", "specific"), expected(over, [0])),
             (one("user", "other"), expected(ok, [2]))]
    for key, values in [("path", ["/api/a", "/api/b", "/api/a", "/api/a", "/no"]),
                        ("middle", ["abc", "axbyc", "axc", "abc", "abc"]),
                        ("suffix", ["tail", "longtail", "tail", "tail", "tailx"]),
                        ("shared", ["team-a", "team-b", "team-c"])]:
        steps.extend((one(key, value), None) for value in values)
    steps.extend((request(descriptor(*entries)), None) for entries in [
        (("app", "web"),), (("app", "web"), ("user", "alice")),
        (("app", "web"), ("user", "vip")), (("app", "web"), ("user", "vip")),
        (("app", "web"), ("user", "vip"), ("extra", "x")),
        (("depth", "x"), ("extra", "x")), (("depth", "x"),),
    ])
    yield "nested_exact_wildcard_shared", matching, {}, steps
    hits_policy = policy(rule("hits", 8), rule("other", 10))
    yield "hits_overrides_refunds", hits_policy, {}, [
        (request(descriptor(("hits", "x")), hits=3), expected(ok, [5])),
        (request(descriptor(("hits", "x"), hits=2), hits=7), expected(ok, [3])),
        (one("hits", hits=0), expected(ok, [3])),
        (one("hits", hits=4), expected(over, [0])),
        (one("hits", hits=1, negative=True), expected(ok, [0])),
        (one("hits", hits=3, negative=True), expected(ok, [3])),
        (one("hits", hits=99, negative=True), expected(ok, [8])),
        (one("hits"), expected(ok, [7])),
        (one("hits", "override", limit=(2, 3)), None),
        (one("hits", "override", limit=(2, 3)), None),
        (one("hits", "override", limit=(2, 3)), None),
        (one("missing", limit=(2, 3)), None),
        (one("other", hits=4294967296), expected(over, [0])),
        (one("other", hits=4294967296, negative=True), expected(ok, [10])),
        (one("missing", hits=2, negative=True), expected(ok, [0])),
    ]
    replaced = rule("old", rate_limit={"unit": "hour", "requests_per_unit": 1, "name": "old-limit"})
    replacer = rule("new", rate_limit={"unit": "hour", "requests_per_unit": 2,
                                     "name": "new-limit", "replaces": [{"name": "old-limit"}]})
    yield "named_replacement", policy(replaced, replacer), {}, [
        (request(descriptor(("old", "x")), descriptor(("new", "x"))), expected(ok, [0, 1])),
        (request(descriptor(("old", "x")), descriptor(("new", "x"))), expected(ok, [0, 0])),
        (request(descriptor(("old", "x")), descriptor(("new", "x"))), expected(over, [0, 0])),
        (one("old"), expected(ok, [0])), (one("old"), expected(over, [0])),
    ]
    all_units = ["second", "minute", "hour", "day", "week", "month", "year"]
    units_policy = policy(*(rule(unit, 9, unit=unit) for unit in all_units))
    unit_request = request(*(descriptor((unit, "x")) for unit in all_units))
    for calendar in [False, True]:
        yield ("all_units_headers_calendar_%s" % calendar, units_policy,
               {"USE_CALENDAR_MONTH_RATE_LIMIT": str(calendar).lower(),
                "LIMIT_RESPONSE_HEADERS_ENABLED": "true", "LIMIT_REQUEST_HEADERS_ENABLED": "true",
                "LIMIT_PER_UNIT_HEADERS_ENABLED": "true", "LIMIT_LIMIT_HEADER": "x-limit",
                "LIMIT_REMAINING_HEADER": "x-remaining", "LIMIT_RESET_HEADER": "x-reset",
                "LIMIT_REQUEST_LIMIT_HEADER": "x-request-limit",
                "LIMIT_REQUEST_REMAINING_HEADER": "x-request-remaining",
                "LIMIT_REQUEST_RESET_HEADER": "x-request-reset"},
               [(unit_request, expected(ok, [8]*7)), (unit_request, expected(ok, [7]*7))],
               {"atomic_second_window": True})
    model_policy = policy({"key": "backend_name", "descriptors": [
        {"key": "model_name_override", "value": "a", "descriptors": [
            rule("tenant", 1, quota_mode=True, metadata={"routing": {"a": True}}),
            rule("default", 4, quota_mode=True, metadata={"routing": {"defaultA": True}})]},
        {"key": "model_name_override", "value": "b", "descriptors": [
            rule("default", 3, quota_mode=True, metadata={"routing": {"b": True}})]},
    ]})
    model_request = request(
        descriptor(("backend_name", "ns/backend"), ("model_name_override", "a"), ("tenant", "u")),
        descriptor(("backend_name", "ns/backend"), ("model_name_override", "a"), ("default", "all")),
        descriptor(("backend_name", "ns/backend"), ("model_name_override", "b"), ("default", "all")))
    yield "quota_model_failover_metadata", model_policy, {"RESPONSE_DYNAMIC_METADATA": "true"}, [
        (model_request, expected(ok, [0, 3, 2])),
        (model_request, expected(ok, [0, 2, 1], [over, ok, ok])),
        (model_request, expected(ok, [0, 1, 0], [over, ok, ok])),
        (model_request, expected(over, [0, 0, 0], [over, ok, over])),
    ]
    metadata_policy = policy(
        rule("meta", 5, metadata={"route": {"first": 1, "conflict": "first"},
                                   "list": [1, True, "text"], "null": None}),
        rule("other", 5, metadata={"route": {"second": 2, "conflict": "second"}}),
    )
    yield "metadata_overrides_and_merge", metadata_policy, {"RESPONSE_DYNAMIC_METADATA": "true"}, [
        (request(descriptor(("meta", "x"), limit=(4, 3), hits=0),
                 descriptor(("other", "x")), hits=2), None),
        (request(descriptor(("other", "x")), descriptor(("meta", "x"))), None),
        (request(descriptor(("missing", "x"))), None),
    ]
    for env in [{"SHADOW_MODE": "true"}, {"QUOTA_MODE": "true"},
                {"SHADOW_MODE": "true", "QUOTA_MODE": "true"}]:
        req = request(descriptor(("a", "x")), descriptor(("b", "x")))
        yield "global_" + "_".join(env), policy(rule("a", 1), rule("b", 3)), env, [(req, None)]*5
    for cache in [False, True]:
        for stop in [False, True]:
            settings = {"LOCAL_CACHE_SIZE_IN_BYTES": "1048576" if cache else "0",
                        "STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT": str(stop).lower()}
            req = request(descriptor(("a", "x")), descriptor(("b", "x")))
            steps = [(req, None)] * 5 + [
                (one("a", hits=20, negative=True), None), (req, None),
                (one("b", hits=20, negative=True), None), (one("b"), None)]
            yield "local_cache_%s_stop_%s" % (cache, stop), policy(rule("a", 1), rule("b", 8)), settings, steps
    per_second_req = request(descriptor(("second", "x")), descriptor(("minute", "x")))
    yield "dedicated_per_second_redis", policy(rule("second", 2, "second"), rule("minute", 4, "minute")), {
        "REDIS_PERSECOND": "true"}, [(per_second_req, None)]*4
    yield "cache_prefix_and_duplicate_descriptors", policy(rule("a", 4)), {"CACHE_KEY_PREFIX": "integration_"}, [
        (request(descriptor(("a", "x")), descriptor(("a", "x"))), expected(ok, [3, 2])),
        (one("a"), expected(ok, [1])),
        (request(descriptor(("a", "x")), descriptor(("a", "x"))), expected(over, [0, 0])),
    ]
    # Exercise response fields which are easy to accidentally approximate: a
    # descriptor-level shadow response retains OVER_LIMIT's zero remaining
    # capacity while returning OK; headers choose the lowest remaining limit
    # for each unit; and a zero addend is a pure check rather than a default
    # single hit when it is set on the descriptor.
    shadow_headers = policy(
        rule("strict", 1, unit="second"),
        rule("shadow", 1, unit="second", shadow_mode=True),
        rule("small", 2, unit="second"),
        rule("minute", 3, unit="minute"),
    )
    combined = request(descriptor(("strict", "x")), descriptor(("shadow", "x")),
                       descriptor(("small", "x")), descriptor(("minute", "x")))
    yield "shadow_zero_hits_same_unit_headers", shadow_headers, {
        "LIMIT_RESPONSE_HEADERS_ENABLED": "true", "LIMIT_REQUEST_HEADERS_ENABLED": "true",
        "LIMIT_PER_UNIT_HEADERS_ENABLED": "true"}, [
            (combined, None), (combined, None),
            (one("small", hits=0), expected(ok, [0])),
            (one("minute", hits=0), expected(ok, [1])),
        ]
    # A regular descriptor remains decisive even when quota descriptors still
    # have capacity. A quota descriptor with no backend/model entries is a
    # separate enforcement group, which exercises the non-model group-key path.
    mixed_quota = policy(rule("regular", 1), rule("quota_a", 1, quota_mode=True),
                         rule("quota_b", 2, quota_mode=True))
    mixed_request = request(descriptor(("regular", "x")), descriptor(("quota_a", "x")),
                            descriptor(("quota_b", "x")))
    yield "mixed_regular_and_anonymous_quota", mixed_quota, {"RESPONSE_DYNAMIC_METADATA": "true"}, [
        (mixed_request, None), (mixed_request, None), (mixed_request, None),
    ]
    # Overrides use the descriptor's exact fields and create independent cache
    # keys from the configured policy. Test both an existing unmatched key and
    # a configured key with an override, including a zero request-level addend.
    override_policy = policy(rule("configured", 9), rule("default", 9))
    yield "override_presence_and_zero_request_hits", override_policy, {}, [
        (request(descriptor(("configured", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("configured", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("configured", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("missing", "x"), limit=(2, 1)), hits=0), None),
        (request(descriptor(("default", "x")), descriptor(("missing", "x"))), None),
    ]
    yield "merge_domain_files", [policy(rule("a", 2)), policy(rule("b", 3))], {
        "MERGE_DOMAIN_CONFIG": "true"}, [
            (request(descriptor(("a", "x")), descriptor(("b", "x"))), expected(ok, [1, 2]))]
