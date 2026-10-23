#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

// Numeric values match the descriptor override's protobuf RateLimitUnit.
enum class RateLimitUnit : int32_t {
    UNKNOWN = 0,
    SECOND = 1,
    MINUTE = 2,
    HOUR = 3,
    DAY = 4,
    MONTH = 5,
    YEAR = 6,
};

struct RateLimitDescriptor {
    struct Entry {
        std::string key;
        std::string value;
    };
    struct Override {
        uint32_t requests_per_unit = 0;
        RateLimitUnit unit = RateLimitUnit::UNKNOWN;
    };

    std::vector<Entry> entries;
    std::optional<Override> limit;
    // Absence means use the request-level value; explicit zero is distinct.
    std::optional<uint64_t> hits_addend;
    bool is_negative_hits = false;
};

struct RateLimitRequest {
    std::string domain;
    std::vector<RateLimitDescriptor> descriptors;
    uint32_t hits_addend = 0;
};
