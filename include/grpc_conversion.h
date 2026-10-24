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

}

#endif // GRPC_CONVERSION_H
