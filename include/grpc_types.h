#pragma once
#include "common.h"
#include "types.h"
#include "envoy/service/ratelimit/v3/rls.pb.h"
#include "envoy/extensions/common/ratelimit/v3/ratelimit.pb.h"
#include <grpcpp/grpcpp.h>

namespace rlc {
namespace pb = envoy::service::ratelimit::v3;
namespace common = envoy::extensions::common::ratelimit::v3;

using GrpcRateLimitRequest = pb::RateLimitRequest;
using GrpcRateLimitResponse = pb::RateLimitResponse;
using GrpcRateLimitDescriptor = common::RateLimitDescriptor;
using GrpcRateLimitDescriptorStatus = pb::RateLimitResponse::DescriptorStatus;
using GrpcRateLimitUnit = pb::RateLimitResponse_RateLimit_Unit;
using GrpcRateLimitStatus = grpc::Status;
using GrpcRateLimitStatusCode = grpc::StatusCode;
}