#pragma once
#include "common.h"
#include <memory>
#include <optional>
#include <vector>
#include <utility>
#include <nlohmann/json.hpp>
namespace rlc {
struct Limit {
  RateLimitResponse::RateLimit limit;
  bool unlimited = false, shadow = false, quota = false, detailed = false;
  std::string full_key;
  std::vector<std::string> replaces;
  std::vector<std::string> share_patterns;
  std::optional<nlohmann::json> metadata;
};
class Config {
 public:
  Config();
  ~Config();
  static std::shared_ptr<Config> load(const std::vector<std::pair<std::string, std::string>>& documents, bool merge = false);
  std::optional<Limit> find(const std::string& domain, const GrpcRateLimitDescriptor& descriptor) const;
  bool empty() const;
  std::string dump() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}
