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
// Unset protobuf Value kinds normalize to JSON null; numbers use double.
nlohmann::json FromProto(const google::protobuf::Struct& source);
nlohmann::json FromProto(const google::protobuf::ListValue& source);
nlohmann::json FromProto(const google::protobuf::Value& source);
google::protobuf::Struct ToProtoStruct(const nlohmann::json& source);
google::protobuf::ListValue ToProtoList(const nlohmann::json& source);
google::protobuf::Value ToProtoValue(const nlohmann::json& source);
RateLimitResponse FromProto(const GrpcRateLimitResponse& source);
GrpcRateLimitResponse ToProto(const RateLimitResponse& source);

}

#endif // GRPC_CONVERSION_H
