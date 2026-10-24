#ifndef GRPC_CONVERSION_H
#define GRPC_CONVERSION_H

#include "common.h"


namespace rlc::grpc_conversion {

RateLimitStatus FromProto(const GrpcRateLimitStatus& status);
GrpcRateLimitStatus ToProto(const RateLimitStatus& status);

RateLimitDescriptor FromProto(const GrpcRateLimitDescriptor& descriptor);
GrpcRateLimitDescriptor ToProto(const RateLimitDescriptor& descriptor);

RateLimitRequest FromProto(const GrpcRateLimitRequest& request);
GrpcRateLimitRequest ToProto(const RateLimitRequest& request);

// Response conversions and reusable nested-message conversions.
RateLimitResponse::Timestamp FromProto(const google::protobuf::Timestamp& source);
google::protobuf::Timestamp ToProto(const RateLimitResponse::Timestamp& source);
RateLimitResponse::Duration FromProto(const google::protobuf::Duration& source);
google::protobuf::Duration ToProto(const RateLimitResponse::Duration& source);
RateLimitResponse::RateLimit FromProto(const GrpcRateLimitResponse::RateLimit& source);
GrpcRateLimitResponse::RateLimit ToProto(const RateLimitResponse::RateLimit& source);
RateLimitResponse::Quota FromProto(const GrpcRateLimitResponse::Quota& source);
GrpcRateLimitResponse::Quota ToProto(const RateLimitResponse::Quota& source);
RateLimitResponse::HeaderValue FromProto(const envoy::config::core::v3::HeaderValue& source);
envoy::config::core::v3::HeaderValue ToProto(const RateLimitResponse::HeaderValue& source);
RateLimitResponse::DescriptorStatus FromProto(const GrpcRateLimitDescriptorStatus& source);
GrpcRateLimitDescriptorStatus ToProto(const RateLimitResponse::DescriptorStatus& source);
RateLimitResponse::Value::Object FromProto(const google::protobuf::Struct& source);
google::protobuf::Struct ToProto(const RateLimitResponse::Value::Object& source);
RateLimitResponse::Value::List FromProto(const google::protobuf::ListValue& source);
google::protobuf::ListValue ToProto(const RateLimitResponse::Value::List& source);
RateLimitResponse::Value FromProto(const google::protobuf::Value& source);
google::protobuf::Value ToProto(const RateLimitResponse::Value& source);
RateLimitResponse FromProto(const GrpcRateLimitResponse& source);
GrpcRateLimitResponse ToProto(const RateLimitResponse& source);

}

#endif // GRPC_CONVERSION_H
