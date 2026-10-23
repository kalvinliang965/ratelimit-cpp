#pragma once
#include "common.h"
#include <memory>
#include <optional>
#include <vector>
#include <utility>
namespace rlc {
// translate this to cpp code too.
struct Limit {
  pb::RateLimitResponse_RateLimit limit;
  bool unlimited = false, shadow = false, quota = false, detailed = false;
  std::string full_key;
  std::vector<std::string> replaces;
  std::vector<std::string> share_patterns;
  std::optional<google::protobuf::Struct> metadata;
};
class Config {
 public:
  Config();
  ~Config();
  static std::shared_ptr<Config> load(const std::vector<std::pair<std::string, std::string>>& documents, bool merge = false);
  std::optional<Limit> find(const std::string& domain, const Descriptor& descriptor) const;
  bool empty() const;
  std::string dump() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}
