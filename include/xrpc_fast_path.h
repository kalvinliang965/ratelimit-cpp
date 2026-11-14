#ifndef XRPC_FAST_PATH_H
#define XRPC_FAST_PATH_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace xrpc_conversion {

// helper struct to build the dpo headers

// TODO: after we remove the cache idenenty might want to bump this to 32bytes
// A string's length is measured in bytes (current 16 bytes); unused bytes are zero-filled.

constexpr std::size_t kFastPathStringBytes = 16;

struct FastPathString {
    uint8_t length = 0;
    std::array<uint8_t, kFastPathStringBytes> bytes{};
};

struct FastPathRequest {
    FastPathString domain;
    FastPathString entry_key;
    FastPathString entry_value;
    uint8_t override_unit = 0;
    uint32_t override_requests_per_unit = 0;
};

struct FastPathResponse {
    uint8_t overall_code = 0;
    uint8_t status_code = 0;
    FastPathString current_limit_name;
    uint32_t current_limit_requests_per_unit = 0;
    uint8_t current_limit_unit = 0;
    uint32_t limit_remaining = 0;
    int64_t duration_until_reset_seconds = 0;
    int32_t duration_until_reset_nanos = 0;
};

struct EncodedFastPathRequest {
    std::vector<uint8_t> payload;
    std::optional<FastPathRequest> fast_path;
};

} // namespace xrpc_conversion

#endif /* XRPC_FAST_PATH_H */
