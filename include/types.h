#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <nlohmann/json.hpp>
#include <unordered_map>


namespace rlc {

struct RateLimitResponse {
    enum class Code : int32_t {
        UNKNOWN = 0,
        OK = 1,
        OVER_LIMIT = 2,
    };

    struct RateLimit {
        enum class Unit : int32_t {
            UNKNOWN = 0,
            SECOND = 1,
            MINUTE = 2,
            HOUR = 3,
            DAY = 4,
            MONTH = 5,
            YEAR = 6,
            WEEK = 7,
        };
        std::string name_;
        uint32_t requests_per_unit_ = 0;
        Unit unit_ = Unit::UNKNOWN;

        const std::string& name() const { return name_; }

        uint32_t requests_per_unit() const { return requests_per_unit_; }
        Unit unit() const { return unit_; }
	

        void set_name(std::string name) { name_ = std::move(name); }
        void set_requests_per_unit(uint32_t value) { requests_per_unit_ = value; }
        void set_unit(Unit unit) { unit_ = unit; }
    };

    struct Timestamp {
        int64_t seconds_ = 0;
        int32_t nanos_ = 0;
    };

    struct Duration {
        int64_t seconds_ = 0;
        int32_t nanos_ = 0;
	Duration(int64_t second, int32_t nano) 
		:seconds_(second), nanos_(nano) {}
    };

    struct Quota {
        uint32_t requests_ = 0;
        std::string id_;
        std::optional<Timestamp> valid_until_;
    };

    struct DescriptorStatus {
        Code code_ = Code::UNKNOWN;
        std::optional<RateLimit> current_limit_;
        uint32_t limit_remaining_ = 0;
        std::optional<Duration> duration_until_reset_;
        std::optional<Quota> quota_;

	const RateLimit& current_limit() const {
		return current_limit_.value();
	}

	uint32_t limit_remaining() const {
		return limit_remaining_;
	}

	const Code& code() const {
		return code_;
	}

	void set_code(Code code) {
		code_ = code;
	}

	void set_limit_remaining(uint32_t limit_remaining) {
		limit_remaining_ = limit_remaining;
	}
	
	void set_current_limit(RateLimit limit) {
		current_limit_ = limit;
	}

	bool has_current_limit() const {
	    return current_limit_.has_value();
	}

	void set_duration_until_reset(Duration d) {
		duration_until_reset_ = d;
	}

    };

    struct HeaderValue {
        std::string key_;
        std::string value_;
        std::string raw_value_;

      	void set_key(std::string key) {
		key_ = key;
	}
	void set_value(std::string value) {
		value_ = value;
	}
	void set_raw_value(std::string rv){
		raw_value_ = rv;
	}
    };

    Code overall_code_ = Code::UNKNOWN;
    std::vector<DescriptorStatus> statuses_;
    std::vector<HeaderValue> response_headers_to_add_;
    std::vector<HeaderValue> request_headers_to_add_;
    std::string raw_body_;
    // Absent metadata differs from a present empty JSON object.
    std::optional<nlohmann::json> dynamic_metadata_;
    std::optional<Quota> quota_;

    DescriptorStatus& add_statuses() {
	return statuses_.emplace_back();
    }

    HeaderValue& add_response_header() {
	return response_headers_to_add_.emplace_back();
    }

    HeaderValue& add_request_header() {
	return request_headers_to_add_.emplace_back();
    }

    void set_overall_code(Code final_code) {
	overall_code_ = final_code;
    }

   
};

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




inline std::string to_string(RateLimitResponse::RateLimit::Unit unit) {
    switch (unit) {
        case RateLimitResponse::RateLimit::Unit::UNKNOWN: return "UNKNOWN";
        case RateLimitResponse::RateLimit::Unit::SECOND: return "SECOND";
        case RateLimitResponse::RateLimit::Unit::MINUTE: return "MINUTE";
        case RateLimitResponse::RateLimit::Unit::HOUR: return "HOUR";
        case RateLimitResponse::RateLimit::Unit::DAY: return "DAY";
        case RateLimitResponse::RateLimit::Unit::MONTH: return "MONTH";
        case RateLimitResponse::RateLimit::Unit::YEAR: return "YEAR";
        case RateLimitResponse::RateLimit::Unit::WEEK: return "WEEK";
    }
    return "UNKNOWN";
}

inline std::string to_string(RateLimitUnit unit) {
    switch (unit) {
        case RateLimitUnit::UNKNOWN: return "UNKNOWN";
        case RateLimitUnit::SECOND: return "SECOND";
        case RateLimitUnit::MINUTE: return "MINUTE";
        case RateLimitUnit::HOUR: return "HOUR";
        case RateLimitUnit::DAY: return "DAY";
        case RateLimitUnit::MONTH: return "MONTH";
        case RateLimitUnit::YEAR: return "YEAR";
    }
    return "UNKNOWN";
}

enum class RateLimitStatusCode : int32_t {
    OK = 0,
    CANCELLED = 1,
    UNKNOWN = 2,
    INVALID_ARGUMENT = 3,
    DEADLINE_EXCEEDED = 4,
    NOT_FOUND = 5,
    ALREADY_EXISTS = 6,
    PERMISSION_DENIED = 7,
    RESOURCE_EXHAUSTED = 8,
    FAILED_PRECONDITION = 9,
    ABORTED = 10,
    OUT_OF_RANGE = 11,
    UNIMPLEMENTED = 12,
    INTERNAL = 13,
    UNAVAILABLE = 14,
    DATA_LOSS = 15,
    UNAUTHENTICATED = 16
};

struct RateLimitStatus {
	RateLimitStatusCode code_;
	std::string message_;
	RateLimitStatusCode code() const {
		return code_;
	}
	const std::string& message() const {
		return message_;
	}
 	RateLimitStatus(RateLimitStatusCode code, std::string message)
        : code_(code), message_(std::move(message)) {}
};

struct RateLimitDescriptor {
    struct Entry {
        std::string key_;
        const std::string& key() const { return key_; }
        std::string value_;
        const std::string& value() const { return value_; }
	Entry (std::string key, std::string value)
	    : key_(key), value_(value) {};
    };

    struct Override {
        uint32_t requests_per_unit_ = 0;
        uint32_t requests_per_unit() const { return requests_per_unit_; }
        RateLimitUnit unit_ = RateLimitUnit::UNKNOWN;
        RateLimitUnit unit() const { return unit_; }
	Override() = default;
	Override(uint32_t requests_per_unit, RateLimitUnit unit)
        	: requests_per_unit_(requests_per_unit), unit_(unit) {}
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

}  // namespace rlc
