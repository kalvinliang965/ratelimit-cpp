#include "config.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>

using namespace rlc;

namespace {
void check(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

Descriptor descriptor(std::initializer_list<std::pair<std::string, std::string>> entries) {
  Descriptor result;
  for (const auto& entry : entries) {
    auto* added = result.add_entries();
    added->set_key(entry.first);
    added->set_value(entry.second);
  }
  return result;
}

void invalid(const std::string& yaml, const std::string& expected) {
  try {
    Config::load({{"test", yaml}});
  } catch (const std::exception& error) {
    check(std::string(error.what()).find(expected) != std::string::npos,
          "wrong configuration error: " + std::string(error.what()));
    return;
  }
  throw std::runtime_error("expected configuration rejection: " + expected);
}

void matching() {
  const auto config = Config::load({{"test", R"YAML(
domain: test
descriptors:
  - key: tenant
    descriptors:
      - key: method
        value: read
        rate_limit: {unit: second, requests_per_unit: 5}
      - key: method
        rate_limit: {unit: minute, requests_per_unit: 20}
  - key: path
    value: /api/exact
    rate_limit: {unit: hour, requests_per_unit: 30}
  - key: path
    value: /api/*
    rate_limit: {unit: day, requests_per_unit: 40}
  - key: path
    value: /api/*end
    rate_limit: {unit: week, requests_per_unit: 50}
  - key: path
    rate_limit: {unit: month, requests_per_unit: 60}
  - key: multi
    value: a*b*c
    rate_limit: {unit: year, requests_per_unit: 70}
  - key: open
    shadow_mode: true
    quota_mode: true
    rate_limit: {unlimited: true, name: open, replaces: [{name: old}]}
)YAML"}});
  check(!config->empty(), "loaded domain is empty");
  check(!config->find("missing", descriptor({{"path", "/api/exact"}})), "unknown domain matched");
  check(!config->find("test", descriptor({})), "empty descriptor matched");
  check(!config->find("test", descriptor({{"tenant", "a"}})), "parent without limit matched");
  check(!config->find("test", descriptor({{"tenant", "a"}, {"missing", "b"}})), "unknown child matched");
  check(!config->find("test", descriptor({{"path", "/api/exact"}, {"extra", "b"}})), "excess request depth matched");
  auto result = config->find("test", descriptor({{"tenant", "a"}, {"method", "read"}}));
  check(result && result->limit.requests_per_unit() == 5 && result->full_key == "test.tenant.method_read", "nested explicit match");
  result = config->find("test", descriptor({{"tenant", "a"}, {"method", "write"}}));
  check(result && result->limit.requests_per_unit() == 20, "nested default match");
  result = config->find("test", descriptor({{"path", "/api/exact"}}));
  check(result && result->limit.requests_per_unit() == 30, "exact must precede wildcard");
  result = config->find("test", descriptor({{"path", "/api/someend"}}));
  check(result && result->limit.requests_per_unit() == 40 && result->full_key == "test.path_/api/*", "wildcards must retain configuration order");
  result = config->find("test", descriptor({{"path", "/other"}}));
  check(result && result->limit.requests_per_unit() == 60, "default key must follow wildcard");
  for (const auto& value : {"abc", "aXXbYYc", "abbbc"}) {
    result = config->find("test", descriptor({{"multi", value}}));
    check(result && result->limit.requests_per_unit() == 70, "middle wildcard failed");
  }
  for (const auto& value : {"ac", "bac", "ab", "abcx", "xabc"})
    check(!config->find("test", descriptor({{"multi", value}})), "middle wildcard falsely matched");
  result = config->find("test", descriptor({{"open", "anything"}}));
  check(result && result->unlimited && result->shadow && result->quota && result->replaces == std::vector<std::string>{"old"}, "unlimited flags and replaces");
  auto request = descriptor({{"unconfigured", "value"}});
  request.mutable_limit()->set_requests_per_unit(17);
  request.mutable_limit()->set_unit(envoy::type::v3::RateLimitUnit::MINUTE);
  result = config->find("test", request);
  check(result && result->limit.requests_per_unit() == 17 && !result->shadow && result->full_key == "test.unconfigured_value", "limit override");
  check(!config->find("missing", request), "override must not create domain");
}

void metricsAndMetadata() {
  const auto config = Config::load({{"test", R"YAML(
domain: test
descriptors:
  - key: tenant
    value: a*b
    share_threshold: true
    value_to_metric: true
    descriptors:
      - key: action
        detailed_metric: true
        rate_limit: {unit: minute, requests_per_unit: 12}
        metadata: {lost: true}
  - key: category
    value_to_metric: true
    descriptors:
      - key: item
        rate_limit: {unit: second, requests_per_unit: 7}
        metadata: {lost: true}
  - key: metadata
    rate_limit: {unit: second, requests_per_unit: 5}
    metadata:
      text: "yes"
      truth: yes
      integer: 012
      float: 2.5
      array: [null, 4, text, {3: [no, "4"]}]
  - key: detail
    detailed_metric: true
    rate_limit: {unit: second, requests_per_unit: 6}
    metadata: {lost: true}
  - key: parent
    detailed_metric: true
    rate_limit: {unit: second, requests_per_unit: 8}
    metadata: {kept: true}
    descriptors:
      - key: child
        rate_limit: {unit: second, requests_per_unit: 9}
)YAML"}});
  auto result = config->find("test", descriptor({{"tenant", "aXYb"}, {"action", "read"}}));
  check(result && result->share_patterns == std::vector<std::string>({"a*b", ""}), "nested share_threshold patterns");
  check(result->full_key == "test.tenant_a*b.action_read", "shared detailed metric key");
  check(!result->metadata, "detailed leaf must preserve Go metadata recreation quirk");
  result = config->find("test", descriptor({{"category", "actual"}, {"item", "unused"}}));
  check(result && result->full_key == "test.category_actual.item", "value_to_metric parent");
  check(!result->metadata, "enhanced key must preserve Go metadata recreation quirk");
  result = config->find("test", descriptor({{"metadata", "v"}}));
  check(result && result->metadata, "plain metadata missing");
  const auto& values = result->metadata->fields();
  check(values.at("text").string_value() == "yes" && values.at("truth").bool_value(), "YAML quoted and boolean metadata types");
  check(values.at("integer").number_value() == 10 && values.at("float").number_value() == 2.5, "YAML numeric metadata types");
  check(values.at("array").list_value().values(3).struct_value().fields().at("3").list_value().values(1).string_value() == "4", "nested metadata values");
  result = config->find("test", descriptor({{"detail", ""}}));
  check(result && result->full_key == "test.detail_", "detailed empty value includes underscore");
  result = config->find("test", descriptor({{"parent", ""}}));
  check(result && result->metadata, "non-leaf detailed metadata must survive");

  setenv("SANITIZE_DESCRIPTOR_METRIC_DOTS", "true", 1);
  auto sanitized = Config::load({{"test", R"YAML(
domain: dotted.domain
descriptors:
  - key: dotted.key
    value: dotted.value
    rate_limit: {unit: second, requests_per_unit: 4}
    metadata: {lost: true}
)YAML"}});
  unsetenv("SANITIZE_DESCRIPTOR_METRIC_DOTS");
  result = sanitized->find("dotted.domain", descriptor({{"dotted.key", "dotted.value"}}));
  check(result && result->full_key == "dotted.domain.dotted_key_dotted_value", "sanitize fragments without changing matching/domain");
  check(!result->metadata, "sanitized metric key recreation must drop metadata");
}

void validation() {
  check(Config::load({})->empty(), "empty config must have no domains");
  invalid("domain: ''", "empty domain");
  invalid("domain: a\ndescriptors: [{value: a}]", "empty key");
  invalid("domain: a\nunknown: true", "unknown key 'unknown'");
  invalid("domain: a\n123: true", "key is not of type string");
  invalid("domain: a\ndescriptors: [no]", "list of type other than map");
  invalid("domain: a\ndescriptors: [{key: k, rate_limit: {unit: invalid}}]", "invalid rate limit unit");
  invalid("domain: a\ndescriptors: [{key: k, rate_limit: {unit: second, unlimited: true}}]", "should not specify rate limit unit when unlimited");
  invalid("domain: a\ndescriptors: [{key: k, rate_limit: {unit: second, requests_per_unit: -1}}]", "requests_per_unit");
  invalid("domain: a\ndescriptors: [{key: k, rate_limit: {unit: second, requests_per_unit: 1.5}}]", "error checking config");
  invalid("domain: a\ndescriptors: [{key: k, rate_limit: {unit: second, replaces: [{name: ''}]}}]", "empty replaces entry");
  invalid("domain: a\ndescriptors: [{key: k, rate_limit: {unit: second, name: n, replaces: [{name: n}]}}]", "same descriptor");
  invalid("domain: a\ndescriptors: [{key: k, share_threshold: true}]", "share_threshold can only be used with wildcard");
  invalid("domain: a\ndescriptors: [{key: k}, {key: k}]", "duplicate descriptor composite key");
  const std::vector<std::pair<std::string, std::string>> documents = {
      {"one", "domain: a\ndescriptors: [{key: k, rate_limit: {unit: second, requests_per_unit: 1}}]"},
      {"two", "domain: a\ndescriptors: [{key: m, rate_limit: {unit: second, requests_per_unit: 2}}]"}};
  auto merged = Config::load(documents, true);
  check(merged->find("a", descriptor({{"k", "x"}}))->limit.requests_per_unit() == 1 &&
        merged->find("a", descriptor({{"m", "x"}}))->limit.requests_per_unit() == 2, "domain merge");
  bool rejected = false;
  try { Config::load(documents); } catch (const std::exception&) { rejected = true; }
  check(rejected, "duplicate domain without merge accepted");
}
}  // namespace

int main() {
  try {
    unsetenv("SANITIZE_DESCRIPTOR_METRIC_DOTS");
    matching();
    metricsAndMetadata();
    validation();
    std::cout << "config tests passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "config tests failed: " << error.what() << '\n';
    return 1;
  }
}
