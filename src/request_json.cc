#include "request_json.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace rlc {
namespace {
using Unit = RateLimitUnit;
using Entry = RateLimitDescriptor::Entry;
using Descriptor = RateLimitDescriptor;
using Request = RateLimitRequest;
using json = nlohmann::json;

Unit MakeUnit(std::string unit) {
	if (unit == "UNKNOWN") return Unit::UNKNOWN;
	if (unit == "SECOND") {
		return Unit::SECOND;
	} else if (unit == "MINUTE") {
		return Unit::MINUTE;
	} else if (unit == "HOUR") {
		return Unit::HOUR;
	} else if (unit == "DAY") {
		return Unit::DAY;
	} else if (unit == "MONTH") {
		return Unit::MONTH;
	} else if (unit == "YEAR") {
		 return Unit::YEAR;
	} 
	// this function should be use for testing purposes.
	throw std::runtime_error("Invalid descriptor ratelimit unit");
}

Descriptor MakeDescriptor(
	std::vector<std::pair<std::string, std::string>> entries,
	std::optional<std::pair<uint32_t, std::string>> limit,
	std::optional<uint64_t> hits_addend,
	bool is_negative
) {
	Descriptor d;
	// make entries
	for (auto& [k, v]: entries) {
		d.entries_.push_back(Entry(k, v));
	}
	if (limit.has_value()) {
		uint32_t request_per_unit = limit->first;
		Unit unit = MakeUnit(limit->second);
		d.limit_.emplace(request_per_unit, unit);
	}
	d.hits_addend_ = hits_addend;
	d.is_negative_hits_ = is_negative;
	return d;
}

// Read a JSON integer without silently wrapping negative or oversized values.
uint64_t ReadUnsigned(const json& value, const std::string& name, uint64_t maximum) {
    uint64_t result;
    if (value.is_number_unsigned()) {
        result = value.get<uint64_t>();
    } else if (value.is_number_integer()) {
        const auto number = value.get<int64_t>();
        if (number < 0) throw std::runtime_error(name + " must be non-negative");
        result = static_cast<uint64_t>(number);
    } else {
        throw std::runtime_error(name + " must be an integer");
    }
    if (result > maximum) throw std::runtime_error(name + " is too large");
    return result;
}

Descriptor MakeDescriptorFromJson(const json& document) {
    const auto& json_entries = document.at("entries");
    if (!json_entries.is_array()) {
        throw std::runtime_error("entries must be an array");
    }
    std::vector<std::pair<std::string, std::string>> entries;
    for (const auto& entry : json_entries) {
        std::string key = entry.at("key").get<std::string>();
        std::string value = entry.at("value").get<std::string>();
        entries.emplace_back(key, value);
    }

    std::optional<std::pair<uint32_t, std::string>> limit;
    if (document.contains("limit") && !document.at("limit").is_null()) {
        const auto& json_limit = document.at("limit");
        const auto requests = ReadUnsigned(json_limit.at("requests_per_unit"),
                                           "requests_per_unit", std::numeric_limits<uint32_t>::max());
        std::string unit = json_limit.at("unit").get<std::string>();
        limit = std::make_pair(static_cast<uint32_t>(requests), unit);
    }

    std::optional<uint64_t> hits_addend;
    if (document.contains("hits_addend") && !document.at("hits_addend").is_null()) {
        hits_addend = ReadUnsigned(document.at("hits_addend"), "hits_addend",
                                   std::numeric_limits<uint64_t>::max());
    }
    bool is_negative = document.value("is_negative_hits", false);
    return MakeDescriptor(entries, limit, hits_addend, is_negative);
}


} // namespace

// A request owns several descriptors. Reuse the descriptor converter for each one.
Request MakeRequestFromJson(const json& document) {
    Request request;
    request.domain_ = document.at("domain").get<std::string>();

    const auto& json_descriptors = document.at("descriptors");
    if (!json_descriptors.is_array()) {
        throw std::runtime_error("descriptors must be an array");
    }
    for (const auto& json_descriptor : json_descriptors) {
        request.descriptors_.push_back(MakeDescriptorFromJson(json_descriptor));
    }
    // Request hits_addend is a uint32_t, not an optional value in types.h.
    // Missing or null therefore maps to its default value, zero.
    if (document.contains("hits_addend") && !document.at("hits_addend").is_null()) {
        request.hits_addend_ = static_cast<uint32_t>(
            ReadUnsigned(document.at("hits_addend"), "request hits_addend",
                         std::numeric_limits<uint32_t>::max()));
    }
    return request;
}


} // namespace rlc
