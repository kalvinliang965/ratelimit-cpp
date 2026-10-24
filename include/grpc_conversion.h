#ifndef GRPC_CONVERSION_H
#define GRPC_CONVERSION_H

#include "common.h"


namespace rlc::grpc_conversion {

Status FromProto(const GrpcStatus& status);
GrpcStatus ToProto(const Status& status);

RateLimitDescriptor FromProto(const common::RateLimitDescriptor& descriptor);
common::RateLimitDescriptor ToProto(const RateLimitDescriptor& descriptor);

Request FromProto(const GrpcRequest& request);
GrpcRequest ToProto(const Request& request);

}

#endif // GRPC_CONVERSION_H
