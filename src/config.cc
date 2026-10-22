#include "config.h"

#include <yaml-cpp/yaml.h>

#include <cctype>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <sstream>

namespace rlc {
namespace {

[[noreturn]] void configError(const std::string& file, const std::string& message) {
  throw std::runtime_error(file + ": " + message);
}

// yaml.v2 accepts duplicate YAML fields and uses the last occurrence.
YAML::Node field(const YAML::Node& map, const std::string& key) {
  YAML::Node result(YAML::NodeType::Undefined);
  if (!map.IsMap()) return result;
  for (const auto& pair : map) {
    if (pair.first.IsScalar() && pair.first.Scalar() == key) result.reset(pair.second);
  }
  return result;
}

bool absent(const YAML::Node& node) { return !node.IsDefined() || node.IsNull(); }

enum class ScalarKind { String, Bool, Integer, UnsignedInteger, Float };

struct Scalar {
  ScalarKind kind = ScalarKind::String;
  bool boolean = false;
  int64_t integer = 0;
  uint64_t unsigned_integer = 0;
  double number = 0;
};

// YAML 1.1 scalar resolution is significant for metadata (notably yes/no and
// octal numbers). Quoted scalars remain strings, as they do in yaml.v2.
Scalar resolveScalar(const YAML::Node& node) {
  Scalar value;
  if (!node.IsScalar()) return value;
  const auto tag = node.Tag();
  if (tag == "!" || tag == "tag:yaml.org,2002:str" || tag == "tag:yaml.org,2002:binary") return value;
  const auto raw = node.Scalar();
  static const std::set<std::string> yes = {"y", "Y", "yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON"};
  static const std::set<std::string> no = {"n", "N", "no", "No", "NO", "false", "False", "FALSE", "off", "Off", "OFF"};
  if (yes.count(raw) || no.count(raw)) {
    value.kind = ScalarKind::Bool;
    value.boolean = yes.count(raw) != 0;
    return value;
  }
  if (raw.empty()) return value;
  std::string number = raw;
  number.erase(std::remove(number.begin(), number.end(), '_'), number.end());
  const bool numeric_start = std::isdigit(static_cast<unsigned char>(number[0])) || number[0] == '+' || number[0] == '-' || number[0] == '.';
  if (!numeric_start) return value;
  size_t offset = (number[0] == '+' || number[0] == '-') ? 1 : 0;
  int base = 0;
  std::string integral = number;
  if (number.size() > offset + 2 && number[offset] == '0' && (number[offset + 1] == 'b' || number[offset + 1] == 'B')) {
    integral.erase(offset, 2);
    base = 2;
  } else if (number.size() > offset + 2 && number[offset] == '0' && (number[offset + 1] == 'o' || number[offset + 1] == 'O')) {
    integral.erase(offset, 2);
    base = 8;
  }
  try {
    size_t end = 0;
    value.integer = std::stoll(integral, &end, base);
    if (end == integral.size()) {
      value.kind = tag == "tag:yaml.org,2002:float" ? ScalarKind::Float : ScalarKind::Integer;
      value.number = static_cast<double>(value.integer);
      return value;
    }
  } catch (const std::exception&) {}
  if (number[0] != '-') {
    try {
      size_t end = 0;
      value.unsigned_integer = std::stoull(integral, &end, base);
      if (end == integral.size()) {
        value.kind = ScalarKind::UnsignedInteger;
        value.number = static_cast<double>(value.unsigned_integer);
        return value;
      }
    } catch (const std::exception&) {}
  }
  std::string lower = number;
  std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
  if (lower == ".inf" || lower == "+.inf" || lower == "-.inf" || lower == ".nan") {
    value.kind = ScalarKind::Float;
    value.number = lower == ".nan" ? std::numeric_limits<double>::quiet_NaN() : std::numeric_limits<double>::infinity();
    if (lower == "-.inf") value.number = -value.number;
    return value;
  }
  // strtod additionally accepts hexadecimal floats, which yaml.v2 does not.
  if (number.find_first_not_of("0123456789+-.eE") == std::string::npos) {
    try {
      size_t end = 0;
      value.number = std::stod(number, &end);
      if (end == number.size()) value.kind = ScalarKind::Float;
    } catch (const std::exception&) {}
  }
  return value;
}

void validateKeys(const YAML::Node& map, const std::string& file) {
  static const std::set<std::string> valid = {
      "domain", "key", "value", "descriptors", "rate_limit", "unit", "requests_per_unit",
      "unlimited", "shadow_mode", "quota_mode", "name", "replaces", "detailed_metric",
      "value_to_metric", "share_threshold", "metadata"};
  if (absent(map)) return;
  if (!map.IsMap()) configError(file, "error loading config file: expected map");
  for (const auto& pair : map) {
    if (!pair.first.IsScalar() || resolveScalar(pair.first).kind != ScalarKind::String)
      configError(file, "config error, key is not of type string: " + YAML::Dump(pair.first));
    const auto key = pair.first.Scalar();
    const auto& value = pair.second;
    if (!valid.count(key)) configError(file, "config error, unknown key '" + key + "'");
    if (key == "metadata") continue;
    if (value.IsSequence()) {
      for (const auto& element : value) {
        if (!element.IsMap()) configError(file, "config error, yaml file contains list of type other than map: " + YAML::Dump(element));
        validateKeys(element, file);
      }
    } else if (value.IsMap()) {
      validateKeys(value, file);
    } else if (value.IsScalar()) {
      const auto kind = resolveScalar(value).kind;
      if (kind == ScalarKind::Float || kind == ScalarKind::UnsignedInteger)
        configError(file, "error checking config");
    }
  }
}

std::string readString(const YAML::Node& map, const char* key, const std::string& file) {
  auto node = field(map, key);
  if (absent(node)) return {};
  if (!node.IsScalar()) configError(file, std::string("error loading config file: expected string for ") + key);
  return node.Scalar();
}

bool readBool(const YAML::Node& map, const char* key, const std::string& file) {
  const auto node = field(map, key);
  if (absent(node)) return false;
  const auto value = resolveScalar(node);
  if (!node.IsScalar() || value.kind != ScalarKind::Bool)
    configError(file, std::string("error loading config file: expected boolean for ") + key);
  return value.boolean;
}

uint32_t readRequests(const YAML::Node& map, const std::string& file) {
  const auto node = field(map, "requests_per_unit");
  if (absent(node)) return 0;
  const auto value = resolveScalar(node);
  if (!node.IsScalar() || value.kind != ScalarKind::Integer || value.integer < 0 ||
      static_cast<uint64_t>(value.integer) > std::numeric_limits<uint32_t>::max())
    configError(file, "error loading config file: requests_per_unit must be a uint32");
  return static_cast<uint32_t>(value.integer);
}

google::protobuf::Value metadataValue(const YAML::Node& node);

std::string nestedMetadataKey(const YAML::Node& node) {
  if (node.IsNull()) return "<nil>";
  if (!node.IsScalar()) throw std::runtime_error("unsupported metadata map key");
  const auto scalar = resolveScalar(node);
  switch (scalar.kind) {
    case ScalarKind::Bool: return scalar.boolean ? "true" : "false";
    case ScalarKind::Integer: return std::to_string(scalar.integer);
    case ScalarKind::UnsignedInteger: return std::to_string(scalar.unsigned_integer);
    case ScalarKind::Float: { std::ostringstream out; out << scalar.number; return out.str(); }
    default: return node.Scalar();
  }
}

google::protobuf::Struct metadataMap(const YAML::Node& node, bool top_level) {
  google::protobuf::Struct result;
  if (!node.IsMap()) throw std::runtime_error("metadata must be a map");
  for (const auto& pair : node) {
    const std::string key = top_level && pair.first.IsScalar() ? pair.first.Scalar() : nestedMetadataKey(pair.first);
    (*result.mutable_fields())[key] = metadataValue(pair.second);
  }
  return result;
}

google::protobuf::Value metadataValue(const YAML::Node& node) {
  google::protobuf::Value value;
  if (node.IsNull()) {
    value.set_null_value(google::protobuf::NULL_VALUE);
  } else if (node.IsMap()) {
    *value.mutable_struct_value() = metadataMap(node, false);
  } else if (node.IsSequence()) {
    auto* list = value.mutable_list_value();
    for (const auto& element : node) *list->add_values() = metadataValue(element);
  } else {
    const auto scalar = resolveScalar(node);
    switch (scalar.kind) {
      case ScalarKind::Bool: value.set_bool_value(scalar.boolean); break;
      case ScalarKind::Integer: value.set_number_value(static_cast<double>(scalar.integer)); break;
      case ScalarKind::UnsignedInteger: value.set_number_value(static_cast<double>(scalar.unsigned_integer)); break;
      case ScalarKind::Float: value.set_number_value(scalar.number); break;
      default: value.set_string_value(node.Scalar()); break;
    }
  }
  return value;
}

struct Wildcard {
  std::string key;
  std::vector<std::string> parts;
  explicit Wildcard(std::string pattern) : key(std::move(pattern)) {
    size_t start = 0;
    while (true) {
      auto end = key.find('*', start);
      parts.emplace_back(key.substr(start, end == std::string::npos ? end : end - start));
      if (end == std::string::npos) break;
      start = end + 1;
    }
  }
  bool matches(const std::string& value) const {
    if (parts.size() == 1) return parts.front() == value;
    size_t fixed = 0;
    for (const auto& part : parts) fixed += part.size();
    if (fixed > value.size() || value.compare(0, parts.front().size(), parts.front()) != 0 ||
        value.compare(value.size() - parts.back().size(), parts.back().size(), parts.back()) != 0) return false;
    size_t position = parts.front().size();
    const auto end = value.size() - parts.back().size();
    for (size_t i = 1; i + 1 < parts.size(); ++i) {
      position = value.find(parts[i], position);
      if (position == std::string::npos || position + parts[i].size() > end) return false;
      position += parts[i].size();
    }
    return true;
  }
};

struct Node {
  std::map<std::string, std::unique_ptr<Node>> descriptors;
  std::optional<Limit> limit;
  std::vector<Wildcard> wildcards;
  bool value_to_metric = false;
  bool share_threshold = false;
  std::string wildcard_pattern;
  std::optional<google::protobuf::Struct> metadata;

  void load(const YAML::Node& list, const std::string& parent, const std::string& file) {
    if (absent(list)) return;
    if (!list.IsSequence()) configError(file, "error loading config file: descriptors must be a list");
    for (const auto& descriptor : list) {
      const auto key = readString(descriptor, "key", file);
      if (key.empty()) configError(file, "descriptor has empty key");
      const auto value = readString(descriptor, "value", file);
      const auto final_key = key + (value.empty() ? "" : "_" + value);
      const auto full_key = parent + final_key;
      if (descriptors.count(final_key)) configError(file, "duplicate descriptor composite key '" + full_key + "'");
      auto child = std::make_unique<Node>();
      auto rate = field(descriptor, "rate_limit");
      if (!absent(rate)) {
        if (!rate.IsMap()) configError(file, "error loading config file: rate_limit must be a map");
        Limit limit;
        limit.full_key = full_key;
        limit.unlimited = readBool(rate, "unlimited", file);
        limit.shadow = readBool(descriptor, "shadow_mode", file);
        limit.quota = readBool(descriptor, "quota_mode", file);
        limit.detailed = readBool(descriptor, "detailed_metric", file);
        auto unit_string = readString(rate, "unit", file);
        auto unit_upper = unit_string;
        std::transform(unit_upper.begin(), unit_upper.end(), unit_upper.begin(), [](unsigned char c) { return std::toupper(c); });
        Unit unit = pb::RateLimitResponse_RateLimit_Unit_UNKNOWN;
        const bool valid_unit = pb::RateLimitResponse_RateLimit_Unit_Parse(unit_upper, &unit) && unit != pb::RateLimitResponse_RateLimit_Unit_UNKNOWN;
        if (limit.unlimited && valid_unit) configError(file, "should not specify rate limit unit when unlimited");
        if (!limit.unlimited && !valid_unit) configError(file, "invalid rate limit unit '" + unit_string + "'");
        limit.limit.set_unit(unit);
        limit.limit.set_requests_per_unit(readRequests(rate, file));
        limit.limit.set_name(readString(rate, "name", file));
        auto replaces = field(rate, "replaces");
        if (!absent(replaces)) {
          if (!replaces.IsSequence()) configError(file, "error loading config file: replaces must be a list");
          for (const auto& replace : replaces) {
            const auto name = readString(replace, "name", file);
            if (name.empty()) configError(file, "should not have an empty replaces entry");
            if (name == limit.limit.name()) configError(file, "replaces should not contain name of same descriptor");
            limit.replaces.push_back(name);
          }
        }
        child->limit = std::move(limit);
      } else {
        // Typed Go YAML decoding validates these fields even without a limit.
        readBool(descriptor, "shadow_mode", file);
        readBool(descriptor, "quota_mode", file);
        readBool(descriptor, "detailed_metric", file);
      }
      child->value_to_metric = readBool(descriptor, "value_to_metric", file);
      child->share_threshold = readBool(descriptor, "share_threshold", file);
      if (child->share_threshold) {
        if (final_key.find('*') == std::string::npos)
          configError(file, "share_threshold can only be used with wildcard values (containing '*'), but found key '" + final_key + "'");
        child->wildcard_pattern = final_key;
      }
      if (final_key.find('*') != std::string::npos) wildcards.emplace_back(final_key);
      const auto metadata = field(descriptor, "metadata");
      if (!absent(metadata)) {
        try {
          auto converted = metadataMap(metadata, true);
          if (!converted.fields().empty()) child->metadata = std::move(converted);
        } catch (const std::exception& error) {
          configError(file, std::string("error parsing metadata: ") + error.what());
        }
      }
      child->load(field(descriptor, "descriptors"), full_key + ".", file);
      descriptors.emplace(final_key, std::move(child));
    }
  }

  void dump(std::ostringstream& out) const {
    if (limit) out << limit->full_key << ": unit=" << pb::RateLimitResponse_RateLimit_Unit_Name(limit->limit.unit())
                   << " requests_per_unit=" << limit->limit.requests_per_unit()
                   << ", shadow_mode: " << (limit->shadow ? "true" : "false")
                   << ", quota_mode: " << (limit->quota ? "true" : "false") << '\n';
    for (const auto& descriptor : descriptors) descriptor.second->dump(out);
  }
};

std::string trimPrefix(const std::string& value, const std::string& prefix) {
  return value.compare(0, prefix.size(), prefix) == 0 ? value.substr(prefix.size()) : value;
}

}  // namespace

struct Config::Impl {
  std::map<std::string, Node> domains;
  bool sanitize = envBool("SANITIZE_DESCRIPTOR_METRIC_DOTS", false);
  std::string metric(std::string fragment) const {
    if (sanitize) std::replace(fragment.begin(), fragment.end(), '.', '_');
    return fragment;
  }
};

Config::Config() : impl_(std::make_unique<Impl>()) {}
Config::~Config() = default;

std::shared_ptr<Config> Config::load(const std::vector<std::pair<std::string, std::string>>& documents, bool merge) {
  auto config = std::make_shared<Config>();
  for (const auto& document : documents) {
    YAML::Node root;
    try {
      root = YAML::Load(document.second);
    } catch (const YAML::Exception& error) {
      configError(document.first, std::string("error loading config file: ") + error.what());
    }
    validateKeys(root, document.first);
    const auto domain = readString(root, "domain", document.first);
    if (domain.empty()) configError(document.first, "config file cannot have empty domain");
    auto found = config->impl_->domains.find(domain);
    if (found != config->impl_->domains.end() && !merge)
      configError(document.first, "duplicate domain '" + domain + "' in config file");
    config->impl_->domains[domain].load(field(root, "descriptors"), domain + ".", document.first);
  }
  return config;
}

std::optional<Limit> Config::find(const std::string& domain, const Descriptor& descriptor) const {
  const auto domain_entry = impl_->domains.find(domain);
  if (domain_entry == impl_->domains.end()) return std::nullopt;
  if (descriptor.has_limit()) {
    Limit result;
    result.full_key = domain + ".";
    std::string entries_key;
    for (const auto& entry : descriptor.entries()) {
      if (!entries_key.empty()) entries_key += ".";
      entries_key += impl_->metric(entry.key());
      if (!entry.value().empty()) entries_key += "_" + impl_->metric(entry.value());
    }
    result.full_key += entries_key;
    result.limit.set_requests_per_unit(descriptor.limit().requests_per_unit());
    result.limit.set_unit(static_cast<Unit>(descriptor.limit().unit()));
    return result;
  }
  const Node* current = &domain_entry->second;
  std::optional<Limit> result;
  std::string detailed_key = domain;
  std::string enhanced_key = domain;
  std::vector<std::string> share_patterns;
  for (int i = 0; i < descriptor.entries_size(); ++i) {
    const auto& entry = descriptor.entries(i);
    const auto final_key = entry.key() + "_" + entry.value();
    detailed_key += "." + impl_->metric(entry.key()) + "_" + impl_->metric(entry.value());
    auto candidate = current->descriptors.find(final_key);
    const Node* next = candidate == current->descriptors.end() ? nullptr : candidate->second.get();
    std::string wildcard_key;
    if (!next) {
      for (const auto& wildcard : current->wildcards) {
        if (wildcard.matches(final_key)) {
          next = current->descriptors.at(wildcard.key).get();
          wildcard_key = wildcard.key;
          break;
        }
      }
    }
    const bool matched_value = next != nullptr;
    if (!next) {
      candidate = current->descriptors.find(entry.key());
      if (candidate != current->descriptors.end()) next = candidate->second.get();
    }
    if (!wildcard_key.empty() && next && next->share_threshold && !next->wildcard_pattern.empty()) {
      if (share_patterns.empty()) share_patterns.resize(descriptor.entries_size());
      share_patterns[i] = trimPrefix(next->wildcard_pattern, entry.key() + "_");
    }
    enhanced_key += "." + impl_->metric(entry.key());
    if (next) {
      std::string metric_value;
      if (!wildcard_key.empty()) {
        if (!share_patterns.empty() && !share_patterns[i].empty()) metric_value = share_patterns[i];
        else if (next->value_to_metric) metric_value = entry.value();
        else metric_value = trimPrefix(wildcard_key, entry.key() + "_");
      } else if (matched_value || next->value_to_metric) {
        metric_value = entry.value();
      }
      if (!metric_value.empty()) enhanced_key += "_" + impl_->metric(metric_value);
    }
    if (next && next->limit && i == descriptor.entries_size() - 1) {
      result = *next->limit;
      result->metadata = next->metadata;
      result->share_patterns = share_patterns;
    }
    if (!next || next->descriptors.empty()) {
      // Preserve the Go implementation's recreation behavior, including its
      // dropping metadata for a detailed metric at a leaf.
      if (result && result->detailed) result->metadata.reset();
      break;
    }
    current = next;
  }
  if (result && result->detailed) {
    if (result->share_patterns.empty()) {
      result->full_key = detailed_key;
    } else {
      result->full_key = domain;
      for (int i = 0; i < descriptor.entries_size(); ++i) {
        const auto& entry = descriptor.entries(i);
        result->full_key += "." + impl_->metric(entry.key());
        const auto& value = result->share_patterns[i].empty() ? entry.value() : result->share_patterns[i];
        if (!value.empty()) result->full_key += "_" + impl_->metric(value);
      }
    }
  }
  if (result && !result->detailed && enhanced_key != result->full_key) {
    result->full_key = enhanced_key;
    result->metadata.reset();
  }
  return result;
}

bool Config::empty() const { return impl_->domains.empty(); }

std::string Config::dump() const {
  std::ostringstream out;
  for (const auto& domain : impl_->domains) domain.second.dump(out);
  return out.str();
}

}  // namespace rlc
