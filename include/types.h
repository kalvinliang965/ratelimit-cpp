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
        std::string key_;
        const std::string& key() const { return key_; }
        std::string value_;
        const std::string& value() const { return value_; }
    };
    struct Override {
        uint32_t requests_per_unit_ = 0;
        uint32_t requests_per_unit() const { return requests_per_unit_; }
        RateLimitUnit unit_ = RateLimitUnit::UNKNOWN;
        RateLimitUnit unit() const { return unit_; }
    };

    std::vector<Entry> entries_;
    const std::vector<Entry>& entries() const { return entries_; }
    int entries_size() const { return static_cast<int>(entries_.size()); }
    const Entry& entries(int index) const { return entries_.at(index); }
    std::optional<Override> limit_;
    bool has_limit() const { return limit_.has_value(); }
    const Override& limit() const {
        static const Override default_limit{};
        return limit_ ? *limit_ : default_limit;
    }
    // Absence means use the request-level value; explicit zero is distinct.
    std::optional<uint64_t> hits_addend_;
    bool has_hits_addend() const { return hits_addend_.has_value(); }
    const std::optional<uint64_t>& hits_addend() const { return hits_addend_; }
    bool is_negative_hits_ = false;
    bool is_negative_hits() const { return is_negative_hits_; }
};

struct RateLimitRequest {
    std::string domain_;
    const std::string& domain() const { return domain_; }
    std::vector<RateLimitDescriptor> descriptors_;
    const std::vector<RateLimitDescriptor>& descriptors() const { return descriptors_; }
    int descriptors_size() const { return static_cast<int>(descriptors_.size()); }
    const RateLimitDescriptor& descriptors(int index) const { return descriptors_.at(index); }
    uint32_t hits_addend_ = 0;
    uint32_t hits_addend() const { return hits_addend_; }
};
