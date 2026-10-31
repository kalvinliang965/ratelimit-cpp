// Declares the conversion between native rate-limit types and XRPC payload bytes.
// The codec handles the payload; the caller supplies fields carried in the DPO header.
#ifndef XRPC_CONVERSION_H
#define	XRPC_CONVERSION_H

#include "types.h"
#include <cstddef>
#include <cstdint>
#include <vector>

// Bounded payload format documented in protocols/ratelimit/README.md.
// No protobuf or generated DPO headers are required by this codec.
namespace xrpc_conversion {


/****
* string we limit to one byte only. 
* blob are two byte
*/
// using string_t = uint_8;

constexpr size_t kEmptyResponseSize = 6;

using Bytes = std::vector<uint8_t>;

using Statuses = std::vector<rlc::RateLimitResponse::DescriptorStatus>;

// max_payload is the largest encoded payload the caller can send or receive.
// Decoders also require the actual number of bytes in data; size must not exceed
// max_payload. All functions throw std::runtime_error for invalid input.
Bytes encode_request(const rlc::RateLimitRequest& request, size_t max_payload);
rlc::RateLimitRequest decode_request(const uint8_t* data, size_t size, size_t max_payload);
Bytes encode_statuses(const Statuses& statuses, size_t max_payload);
Statuses decode_statuses(const uint8_t* data, size_t size, size_t max_payload);
// Overall code is carried separately in the fixed DPO header.
Bytes encode_response(const rlc::RateLimitResponse& response, size_t max_payload);
rlc::RateLimitResponse decode_response(const uint8_t* data, size_t size,
                                       rlc::RateLimitResponse::Code overall_code, size_t max_payload);
} // namespace xrpc_conversion

#endif // XRPC_CONVERSION_H
