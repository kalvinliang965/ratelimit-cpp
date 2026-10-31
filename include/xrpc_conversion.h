// Declares the conversion between native rate-limit types and XRPC payload bytes.
// The codec handles the payload; the caller supplies fields carried in the DPO header.
#ifndef XRPC_CONVERSION_H
#define	XRPC_CONVERSION_H

#include "types.h"
#include <cstddef>
#include <cstdint>
#include <vector>

// Version 2 of the bounded payload format documented in protocols/ratelimit/README.md.
// No protobuf or generated DPO headers are required by this codec.
namespace xrpc_conversion {


/****
* string we limit to one byte only. 
* blob are two byte
*/
// using string_t = uint_8;

// Application buffer budget: DPO header plus codec payload, not the network MTU.
constexpr size_t kHeaderSize = 35;
constexpr size_t kMaxPacketBytes = 1500;
constexpr size_t kMaxPayloadBytes = kMaxPacketBytes - kHeaderSize;

// recommanded limit
constexpr size_t kRecommandDescriptors = 5;
constexpr size_t kRecommandEntries = 10;
constexpr size_t kRecommandStringBytes = 64;
constexpr size_t kRecommandHeaders = 16; 
constexpr size_t kRecommandMetadataDepth = 32;
constexpr size_t kEmptyResponseSize = 6;

using Bytes = std::vector<uint8_t>;

using Statuses = std::vector<rlc::RateLimitResponse::DescriptorStatus>;

// Throw std::runtime_error on malformed/oversized input, including trailing bytes.
Bytes encode_request(const rlc::RateLimitRequest& request);
rlc::RateLimitRequest decode_request(const uint8_t* data, size_t size);
Bytes encode_statuses(const Statuses& statuses);
Statuses decode_statuses(const uint8_t* data, size_t size);
// Overall code is carried separately in the fixed DPO header.
Bytes encode_response(const rlc::RateLimitResponse& response);
rlc::RateLimitResponse decode_response(const uint8_t* data, size_t size,
                                       rlc::RateLimitResponse::Code overall_code);
} // namespace xrpc_conversion

#endif // XRPC_CONVERSION_H
