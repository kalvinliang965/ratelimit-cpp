# Go test-suite parity audit

The adjacent `../ratelimit` checkout is the behavioral reference.  Its Go
tests remain unmodified.  This document separates behavior that can be run
through the standalone service from tests that deliberately exercise Go
objects, mocks, an injected clock, or an in-process stats store.

`tests/go_reference_vectors.py` is the executable part of this audit.  Its
data lives in `tests/go_reference_vectors.json`, and every vector names the
originating Go test function and file.  The driver copies the listed original
Go YAML file(s) byte-for-byte into separate temporary runtime directories.  It
then starts the C++ and Go binaries, each with an empty real Redis server,
sends the same real gRPC request sequence to both, and checks:

- every successful response as deterministic serialized protobuf bytes;
- every failure as its exact `(gRPC code, details)` tuple;
- the final Redis keys, values, and TTLs without key or duration
  normalization.

Run it after the normal build prerequisites are available:

```sh
make -C ratelimit-cpp test-go-reference-vectors
```

The vectors intentionally do not invoke Go code or C++ internal functions.
They use the Go fixture files as source data and the public gRPC API as the
driver boundary.

## Directly reusable Go cases

| Vector ID | Exact Go source | Original configuration staged | Request sequence exercised |
| --- | --- | --- | --- |
| `go_integration_negative_hits` | `test/integration/integration_test.go:TestNegativeHitsIntegration/testNegativeHits` (92–136) | `test/integration/runtime/current/ratelimit/config/basic.yaml` | `basic/key1=foo`: +5, descriptor-level −3, then +1. Checks the Go assertions 45, 48, 47 remaining inside one fresh second. |
| `go_integration_basic_base_sequence` | `test/integration/integration_test.go:TestBasicConfig/testBasicBaseConfig` (138–154, 657–888), standard Redis/no-local-cache subtest | `runtime/.../config/basic.yaml` and `another.yaml` | Unknown descriptor; `basic/key1`; 25 sequential minute requests to `another/key2`; then 15 requests containing `another/key2` and `another/key3` in descriptor order. This retains the original exhaustion and multiple-descriptor sequence. |
| `go_config_basic_yaml` | `test/config/config_test.go:TestBasicConfig`, `TestConfigLimitOverride` (30–335) | `test/config/basic_config.yaml` | Domain miss, nested default/exact limits, value-specific override, whitelist/no-limit, hour limit, unlimited rule, detailed metric rule, and month rule. The Go config fixture itself, including comments and field spelling, is what both services parse. |
| `go_config_wildcard_yaml` | `test/config/config_test.go:TestWildcardConfig` (576–710) | `test/config/wildcard.yaml` | No-value lookup, trailing wildcard, nested wildcard, no match, middle wildcard including empty match, rejected middle wildcard, multiple wildcard, and rejected multi-wildcard. |
| `go_config_share_threshold_yaml` | `test/config/config_test.go:TestShareThreshold` (1669–1980) and `test/limiter/base_limiter_test.go:TestGenerateCacheKeysWithShareThreshold` | `test/config/share_threshold.yaml` | Different values through shared top-level and nested wildcard thresholds; value-to-metric and detailed-metric descendants; middle and multi-wildcard patterns. The first two `files/*` requests assert one shared remaining sequence (99 then 98). |
| `go_config_merge_domain_yaml` | `test/config/config_test.go:TestDomainMerge` (227–255) | `test/config/merge_domain_key1.yaml`, `merge_domain_key2.yaml` with `MERGE_DOMAIN_CONFIG=true` | One two-descriptor request checks merged-domain selection, descriptor order, limits 10 and 20, and remaining counts 9 and 19. |

The driver accepts `--filter <vector-id-substring>` to run one source case
while investigating a difference.  Its repeat notation is only a compact form
for repeated public gRPC calls; responses are still paired and byte-compared
one call at a time.

## Source-mapped StatsD and calendar cases

`tests/go_statsd_reference_vectors.json` and
`tests/integration_validation.py:test_statsd_go_source_metric_matrix` extend
the same approach to the public TCP StatsD wire protocol.  Every case starts
real C++ and Go binaries with separate real Redis and StatsD listeners; every
gRPC reply is byte-compared, Redis keys/values/TTLs are compared exactly, and
the complete rate-limit StatsD counter map is compared exactly.

| Fixture case | Exact Go source | How it is reused |
| --- | --- | --- |
| `share_threshold_yaml` | `test/config/config_test.go:TestShareThreshold` (1669–1980) | The original `test/config/share_threshold.yaml` is copied byte-for-byte. The complete source request sequence covers top-level, nested, middle, and multi-wildcard sharing; explicit-over-wildcard precedence; and `share_threshold` precedence over both `value_to_metric` and `detailed_metric`. It asserts the source metric names and shared counter totals on the real StatsD wire. |
| `wildcard_*`, `middle_trailing_*` | `TestWildcardStatsBehavior` (1983–2210) | These Go subtests build configs in Go structs, so they cannot be passed directly to an executable. Their exact policy fields, descriptors, runtime values, and expected `Stats.Key` values are extracted as neutral JSON fixtures and sent unchanged to both binaries. |
| `value_to_metric_detailed_metric_precedence` | `TestValueToMetric_DoesNotOverrideDetailedMetric` (1206–1263) | The inline Go config is extracted into the shared neutral fixture; the real StatsD key proves detailed-metric precedence over two value-to-metric ancestors. |
| `value_to_metric_wildcard_runtime_values` | `TestValueToMetric_WithWildcard` (1354–1426) | The inline Go config and both original requests are extracted into the shared neutral fixture; separate actual runtime values must yield the two source metric keys. |

`tests/integration_validation.py:test_calendar_month_current_window_keys`
stages the unmodified `test/config/basic_config.yaml` (`key8`, lines 73–77)
and runs both `USE_CALENDAR_MONTH_RATE_LIMIT=true` and the legacy `false`
mode. It asserts each real response byte-for-byte, the exact live Redis key,
counter, and TTL, and checks the public reset calculation. Calendar mode must
use the current UTC month-start timestamp and expiry to the next UTC month;
legacy mode must use the current fixed 30-day bucket and a full 30-day Redis
TTL while reporting the remainder of that bucket as its reset duration.

## Existing black-box coverage mapped to the rest of Go's behavior suite

| Go source tests | Observable contract | Current real-binary coverage |
| --- | --- | --- |
| `test/integration/integration_test.go:TestBasicConfig` variants | per-second Redis, local cache, cache prefix, stop-increment-on-over-limit | `tests/parity.py`: `dedicated_per_second_redis`, `local_cache_*`, and `cache_prefix_and_duplicate_descriptors`; each compares C++ and Go response bytes and Redis snapshots. |
| `TestXdsProviderBasicConfig`, `TestXdsProviderBasicConfigReload` and `test/integration/xds_sotw_integration_test.go` | ADS SotW initial load, ACK, and policy replacement | `tests/integration_validation.py:test_xds_sotw` starts a real SotW server and validates ACKs, gRPC responses, and Redis state. |
| `TestBasicReloadConfig` / `testConfigReload` | file reload accepts a new policy and retains the last valid policy after invalid input | `tests/parity.py:Runner.reload`; response bytes are retained exactly. |
| `TestBasicTLSConfig`, `Test_mTLS`, `TestReloadGRPCServerCerts` | server TLS, mTLS, client certificate/SAN behavior | `tests/integration_validation.py:test_grpc_tls_and_mtls` and `test_grpc_client_san_rejection_parity`; both use real certificates and binaries. |
| `TestBasicAuthConfig`, Redis TLS, Cluster, Sentinel | Redis auth/TLS/topology behavior | `tests/backend_modes_docker.py` and Docker validation run C++/Go real gRPC clients against Redis Cluster and Sentinel. TLS/mTLS service validation is separate because the upstream Go integration fixtures use stunnel. |
| `TestBasicConfig_ExtraTags`, `TestSanitizeDescriptorMetricDots`, `TestShareThreshold`, `TestWildcardStatsBehavior`, and the value-to-metric/detailed-metric config tests | StatsD names/counters, wildcard metric hierarchy, share-threshold key reuse, value-to-metric/detailed-metric precedence, and descriptor dot sanitation | `test_statsd_detailed_metric` plus `test_statsd_go_source_metric_matrix` use real TCP StatsD captures and exact C++/Go counter maps. The latter stages Go's checked-in share-threshold YAML and neutral fixtures extracted from Go-only inline config cases. |
| `test/service/ratelimit_test.go:TestQuotaMetadataSameModelDifferentBackends` (1855–1904) | quota aggregation must keep distinct backend/model pairs separate, preserve descriptor order, and include only the passing backend in dynamic metadata | `tests/integration_validation.py:test_quota_same_model_different_backends` sends the original two-backend/same-model shape to real C++ and Go processes. It byte-compares the full response and exact Redis snapshot; the over-limit backend is 101/100 and the independent backend is 1/100. |
| `test/service/ratelimit_test.go` global/rule shadow modes, quota modes, metadata and header behavior | overall result aggregation, descriptor statuses, dynamic metadata, response/request headers | `tests/parity.py`: `basic_errors_unlimited_shadow`, `global_*`, `quota_model_failover_metadata`, `metadata_overrides_and_merge`, `shadow_zero_hits_same_unit_headers`, and `mixed_regular_and_anonymous_quota`. These compare complete response bytes, so ordering, field presence, headers, metadata, and durations are included. |
| service empty-domain/empty-descriptor/unlimited and Redis failure paths | exact gRPC error/result behavior and unbounded remaining value | `tests/parity.py:basic_errors_unlimited_shadow`, `malformed_grpc_payloads`; `tests/integration_validation.py:test_redis_restart_and_recovery`, `test_missing_and_unavailable_startup`, `test_zero_and_uint32_max_limits`. |
| `test/limiter/base_limiter_test.go` cache-prefix/share-threshold/local-cache response paths | key sharing, counter state, over-limit fast path | `tests/parity.py`: wildcard/shared, cache-prefix, local-cache, negative-hit, and concurrent-counter scenarios; the source-mapped StatsD case also checks the real share-threshold Redis key stems. Final Redis state is checked exactly for all paired scenarios. |
| `test/config/config_test.go` override, wildcard, metadata, detailed/value-to-metric | policy selection and externally emitted response/stats behavior | direct fixtures above plus `tests/parity.py` matching/override/metadata scenarios and the source-mapped real StatsD matrix. |

## Go-internal-only or partially external cases

These tests remain source-of-truth design evidence, but cannot be rerun
literally against the executable without changing their intent.

| Go tests | Why they are not literal service tests | External status |
| --- | --- | --- |
| `test/service/ratelimit_test.go:TestService` and most service tests | They install mocked config/cache objects and inject exact cache return structures, counters, and reload events. | Their gRPC-observable aggregates are covered by the parity scenarios above. In-process cache call ordering and counter increments are Go implementation details. |
| `TestRequestHeadersDisabledOnHotReload` (393–425) | It unsets an environment variable inside the already-running Go process before emitting a mock reload event. | No ordinary external process can mutate its inherited environment. File reload/header behavior is covered; this exact transition remains an in-process regression case. |
| `TestServiceTracer` (971–996) | It asserts an OTel span from a Go test exporter. | No C++ tracing implementation or collector parity test exists. |
| `TestServiceHealthStatusAtLeastOneConfigLoaded` (1026–1078) | The Go unit test drives provider events directly. | `tests/integration_validation.py:test_health_requires_loaded_config_transitions` recreates it through real FILE-provider events: empty directory gives named health `NOT_SERVING`, atomic valid-policy add gives `SERVING`, invalid replacement retains the last valid policy and `SERVING`, then removal gives `NOT_SERVING`. Each stable health protobuf, RLS reply, and Redis state is paired between real C++ and Go binaries. |
| config rejection tests (347–498) | They assert Go panics and exact parser/error text for malformed YAML and invalid fields. | C++ `tests/config_test.cc` covers semantic validation; startup tests cover invalid/missing config. Exact Go panic wording is not a gRPC contract. |
| `test/limiter/base_limiter_test.go:TestGenerateCacheKeysMonth` (86–118) and `test/utils/utilities_test.go:TestCalculateResetMonthEndOfMonth`, `TestCalculateResetMonthLeapYear`, `TestExpirationSecondsMonth` (47–115) | They inject fixed Jan/Feb timestamps through a mock `TimeSource`. | The live current-window counterpart now checks exact public Redis keys/TTLs and reset values in both calendar and legacy modes. The literal Jan/Feb and leap-year timestamps cannot be driven through public gRPC/configuration: `src/service_cmd/runner/runner.go` constructs `utils.NewTimeSourceImpl()` (126, 134, 205), whose `UnixNow()` is hard-wired to `time.Now().Unix()` in `src/utils/time.go` (19–25); no public clock setting or RPC exists. Changing host time or preloading a fake clock would stop being the same service/Redis process contract, so those fixed-date assertions remain internal-only source evidence. |
| mock Redis/driver, memcache, stats-manager, server JSON-handler, SRV, and settings tests | They assert Go library calls, mocks, or Go-only constructor defaults. | Real Redis/health/StatsD/HTTP routes have separate black-box coverage where the behavior is public; the internal API shape is intentionally not ported as a test target. |

## Remaining fixed-clock limitation

The current-window test does not claim to replay Go's 2024-01-31,
2024-02-28, or boundary-crossing timestamps. Those inputs are available only
through the Go tests' injected `TimeSource`, as described above. The
standalone public binaries expose no compatible clock control, so replaying
them would require altering the process environment outside the gRPC/Redis
contract rather than reusing an existing Go test input honestly.
