#pragma once
#include <cstdlib>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <algorithm>
#include "envoy/service/ratelimit/v3/rls.pb.h"
#include "types.h"
#include <grpcpp/grpcpp.h>

namespace rlc {
namespace pb = envoy::service::ratelimit::v3;
namespace common = envoy::extensions::common::ratelimit::v3;

// Shared application types (defined independently of gRPC in types.h).
using ::RateLimitRequest;
using ::RateLimitDescriptor;
using ::RateLimitUnit;
using ::RateLimitStatus;
using ::RateLimitStatusCode;

// gRPC frontend / protobuf types. Response and config migration is still pending.
using GrpcRateLimitRequest = pb::RateLimitRequest;
using GrpcRateLimitResponse = pb::RateLimitResponse;
using GrpcRateLimitDescriptor = common::RateLimitDescriptor;
using GrpcRateLimitDescriptorStatus = pb::RateLimitResponse::DescriptorStatus;
using GrpcRateLimitUnit = pb::RateLimitResponse_RateLimit_Unit;

// RPC operation status, distinct from a descriptor's OK/OVER_LIMIT decision.
using GrpcRateLimitStatus = grpc::Status;
using GrpcRateLimitStatusCode = grpc::StatusCode;

inline std::string env(const char* key, const std::string& fallback = "") {
  const char* value = std::getenv(key); return value ? value : fallback;
}
inline bool envBool(const char* key, bool fallback = false) {
  auto value = env(key, fallback ? "true" : "false");
  if (value == "1" || value == "t" || value == "T" || value == "TRUE" || value == "true" || value == "True") return true;
  if (value == "0" || value == "f" || value == "F" || value == "FALSE" || value == "false" || value == "False") return false;
  throw std::runtime_error(std::string("invalid boolean setting ") + key);
}
inline int64_t envInt(const char* key, int64_t fallback) {
  auto value = env(key, std::to_string(fallback)); size_t end = 0;
  auto result = std::stoll(value, &end);
  if (end != value.size()) throw std::runtime_error(std::string("invalid integer setting ") + key);
  return result;
}
}
