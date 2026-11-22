#include "ratelimit_packet_codec.h"
#include "dpo_socket_packet.h"
#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <array>
namespace wire = xrpc_conversion;
static_assert(wire::kFastPathStringBytes == 16, "Update rate-limit packet layout");
static void put_uint(wire::Bytes& packet, size_t offset, uint64_t value, unsigned bytes) {
    for (unsigned i = 0; i < bytes; ++i) packet.at(offset + i) = static_cast<uint8_t>(value >> (8 * i));
}

namespace ratelimit_packet_codec {
wire::Bytes encode_request_packet(const rlc::RateLimitRequest& request, uint64_t transaction_id, uint64_t timestamp, uint8_t target_stage, size_t capacity) {
    if (capacity < dpo_socket::kHeaderSize) throw std::runtime_error("packet capacity smaller than header");
    const auto encoded = wire::encode_request_with_fast_path(request, capacity - dpo_socket::kHeaderSize);
    wire::Bytes packet(dpo_socket::kHeaderSize, 0);
    // Offsets follow ratelimit.dpo's generated global header layout.
    packet[0] = 0; // MsgType::RateLimitRequest.
    put_uint(packet, 14, transaction_id, 8);
    put_uint(packet, 22, timestamp, 8);
    packet[30] = target_stage;
    if (encoded.fast_path) {
        const auto& fast = *encoded.fast_path;
        packet.at(32) = 1;
        packet.at(33) = fast.domain.length;
        std::copy(fast.domain.bytes.begin(), fast.domain.bytes.end(), packet.begin() + 34);
        packet.at(50) = fast.entry_key.length;
        std::copy(fast.entry_key.bytes.begin(), fast.entry_key.bytes.end(), packet.begin() + 51);
        packet.at(67) = fast.entry_value.length;
        std::copy(fast.entry_value.bytes.begin(), fast.entry_value.bytes.end(), packet.begin() + 68);
        packet.at(84) = fast.override_unit;
        put_uint(packet, 85, fast.override_requests_per_unit, 4);
    }
    packet.insert(packet.end(), encoded.payload.begin(), encoded.payload.end());

    return packet;
}
rlc::RateLimitResponse decode_response_packet(const uint8_t* data, size_t size, uint64_t transaction_id, uint64_t timestamp, size_t capacity) {
    if (capacity < dpo_socket::kHeaderSize) throw std::runtime_error("packet capacity smaller than header");
    std::array<uint8_t, 30> expected{};
    for (unsigned i = 0; i < 8; ++i) { expected[14+i] = static_cast<uint8_t>(transaction_id >> (8*i)); expected[22+i] = static_cast<uint8_t>(timestamp >> (8*i)); }
    const auto* response = data;
        if (!data || size < dpo_socket::kHeaderSize ||
            size > capacity || response[0] != 1) {
            throw std::runtime_error("invalid response size or message type");
        }
        if (std::memcmp(data + 14, expected.data() + 14, 16) != 0) {
            throw std::runtime_error("response transaction ID or timestamp differs");
        }
	const size_t response_size = static_cast<size_t>(size)
                             - dpo_socket::kHeaderSize;

        const auto code = response[31];
        if (code > 2 || response[89] > 1) throw std::runtime_error("invalid response header");
        rlc::RateLimitResponse decoded;
        if (response[89]) {
            auto number = [&](size_t offset, unsigned bytes) {
                uint64_t value = 0;
                for (unsigned i = 0; i < bytes; ++i) value |= uint64_t(response[offset + i]) << (8 * i);
                return value;
            };
            wire::FastPathResponse fast{};
            fast.overall_code = code;
            fast.status_code = response[90];
            fast.current_limit_name.length = response[91];
            std::copy_n(response + 92, fast.current_limit_name.bytes.size(), fast.current_limit_name.bytes.begin());
            fast.current_limit_requests_per_unit = static_cast<uint32_t>(number(108, 4));
            fast.current_limit_unit = response[112];
            fast.limit_remaining = static_cast<uint32_t>(number(113, 4));
            const auto seconds = number(117, 8);
            const auto nanos = number(125, 4);
            if (seconds > uint64_t(std::numeric_limits<int64_t>::max()) || nanos > uint64_t(std::numeric_limits<int32_t>::max()))
                throw std::runtime_error("invalid fast-path duration");
            fast.duration_until_reset_seconds = static_cast<int64_t>(seconds);
            fast.duration_until_reset_nanos = static_cast<int32_t>(nanos);
            decoded = wire::response_from_fast_path(fast);
        } else {
            decoded = wire::decode_response(data + dpo_socket::kHeaderSize, response_size,
                static_cast<rlc::RateLimitResponse::Code>(code), capacity - dpo_socket::kHeaderSize);
        }
    return decoded;
}
}
