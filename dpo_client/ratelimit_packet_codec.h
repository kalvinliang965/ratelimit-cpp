#pragma once
#include "xrpc_conversion.h"
#include <cstddef>
#include <cstdint>
namespace ratelimit_packet_codec {
xrpc_conversion::Bytes encode_request_packet(const rlc::RateLimitRequest&, uint64_t transaction_id, uint64_t timestamp, uint8_t target_stage, size_t capacity);
rlc::RateLimitResponse decode_response_packet(const uint8_t* data, size_t size, uint64_t transaction_id, uint64_t timestamp, size_t capacity);
}
