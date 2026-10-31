#include "xrpc_conversion.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>
#include <utility>
#include <string>

#include <nlohmann/json.hpp>

namespace {

// Example budget; a transport should calculate its own limit.
constexpr size_t kMaxPayload = 1500 - 34;
using Request = rlc::RateLimitRequest;
using Entry = rlc::RateLimitDescriptor::Entry;
using Unit = rlc::RateLimitUnit;
using Descriptor = rlc::RateLimitDescriptor;
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

// Write from the descriptor itself, so this tests conversion in both directions.
void WriteDescriptorToJson(json& output, const Descriptor& descriptor) {
    output = json::object();
    output["entries"] = json::array();
    for (const auto& entry : descriptor.entries()) {
        output["entries"].push_back({
            {"key", entry.key()},
            {"value", entry.value()}
        });
    }
    if (descriptor.has_limit()) {
        output["limit"] = {
            {"requests_per_unit", descriptor.limit().requests_per_unit()},
            {"unit", rlc::to_string(descriptor.limit().unit())}
        };
    }
    if (descriptor.has_hits_addend()) {
        output["hits_addend"] = *descriptor.hits_addend();
    }
    output["is_negative_hits"] = descriptor.is_negative_hits();
}

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

void WriteRequestToJson(json& output, const Request& request) {
    output = json::object();
    output["domain"] = request.domain();
    output["hits_addend"] = request.hits_addend();
    output["descriptors"] = json::array();
    for (const auto& descriptor : request.descriptors()) {
        json json_descriptor;
        WriteDescriptorToJson(json_descriptor, descriptor);
        output["descriptors"].push_back(json_descriptor);
    }
}

using Response = rlc::RateLimitResponse;
using ResponseUnit = Response::RateLimit::Unit;

Response::Code MakeCode(const json& value) {
    const auto name = value.get<std::string>();
    if (name == "UNKNOWN") return Response::Code::UNKNOWN;
    if (name == "OK") return Response::Code::OK;
    if (name == "OVER_LIMIT") return Response::Code::OVER_LIMIT;
    throw std::runtime_error("invalid response code: " + name);
}

std::string CodeName(Response::Code code) {
    switch (code) {
        case Response::Code::UNKNOWN: return "UNKNOWN";
        case Response::Code::OK: return "OK";
        case Response::Code::OVER_LIMIT: return "OVER_LIMIT";
    }
    throw std::runtime_error("invalid response code");
}

ResponseUnit MakeResponseUnit(const json& value) {
    const auto name = value.get<std::string>();
    // The response enum includes WEEK, unlike the request override enum.
    for (int i = 0; i <= 7; ++i) {
        const auto unit = static_cast<ResponseUnit>(i);
        if (rlc::to_string(unit) == name) return unit;
    }
    throw std::runtime_error("invalid response unit: " + name);
}

bool HasValue(const json& document, const char* name) {
    return document.contains(name) && !document.at(name).is_null();
}

uint32_t ReadU32(const json& value, const std::string& name) {
    return static_cast<uint32_t>(
        ReadUnsigned(value, name, std::numeric_limits<uint32_t>::max()));
}

int64_t ReadSigned(const json& value, int64_t minimum, int64_t maximum) {
    if (!value.is_number_integer()) throw std::runtime_error("time must be an integer");
    if (value.is_number_unsigned()) {
        const auto number = value.get<uint64_t>();
        if (number > static_cast<uint64_t>(maximum))
            throw std::runtime_error("time integer is too large");
        return static_cast<int64_t>(number);
    }
    const auto number = value.get<int64_t>();
    if (number < minimum || number > maximum)
        throw std::runtime_error("time integer is out of range");
    return number;
}

// Timestamp and duration share the same fields, but remain distinct C++ types.
Response::Timestamp MakeTimeFromJson(const json& document) {
    return {
        ReadSigned(document.at("seconds"), INT64_MIN, INT64_MAX),
        static_cast<int32_t>(ReadSigned(document.at("nanos"), INT32_MIN, INT32_MAX))
    };
}

json WriteTimeToJson(int64_t seconds, int32_t nanos) {
    return {{"seconds", seconds}, {"nanos", nanos}};
}

Response::RateLimit MakeLimitFromJson(const json& document) {
    Response::RateLimit limit;
    limit.set_name(document.at("name").get<std::string>());
    limit.set_requests_per_unit(ReadU32(document.at("requests_per_unit"), "requests_per_unit"));
    limit.set_unit(MakeResponseUnit(document.at("unit")));
    return limit;
}

json WriteLimitToJson(const Response::RateLimit& limit) {
    return {{"name", limit.name()}, {"requests_per_unit", limit.requests_per_unit()},
            {"unit", rlc::to_string(limit.unit())}};
}

Response::Quota MakeQuotaFromJson(const json& document) {
    Response::Quota quota;
    quota.requests_ = ReadU32(document.at("requests"), "quota requests");
    quota.id_ = document.at("id").get<std::string>();
    if (HasValue(document, "valid_until"))
        quota.valid_until_ = MakeTimeFromJson(document.at("valid_until"));
    return quota;
}

json WriteQuotaToJson(const Response::Quota& quota) {
    json result = {{"requests", quota.requests_}, {"id", quota.id_}};
    if (quota.valid_until_)
        result["valid_until"] = WriteTimeToJson(quota.valid_until_->seconds_,
                                                quota.valid_until_->nanos_);
    return result;
}

Response::DescriptorStatus MakeStatusFromJson(const json& document) {
    Response::DescriptorStatus status;
    status.set_code(MakeCode(document.at("code")));
    status.set_limit_remaining(ReadU32(document.at("limit_remaining"), "limit_remaining"));
    if (HasValue(document, "current_limit"))
        status.set_current_limit(MakeLimitFromJson(document.at("current_limit")));
    if (HasValue(document, "duration_until_reset")) {
        const auto time = MakeTimeFromJson(document.at("duration_until_reset"));
        status.duration_until_reset_.emplace(time.seconds_, time.nanos_);
    }
    if (HasValue(document, "quota"))
        status.quota_ = MakeQuotaFromJson(document.at("quota"));
    return status;
}

json WriteStatusToJson(const Response::DescriptorStatus& status) {
    json result = {{"code", CodeName(status.code())},
                   {"limit_remaining", status.limit_remaining()}};
    if (status.current_limit_)
        result["current_limit"] = WriteLimitToJson(*status.current_limit_);
    if (status.duration_until_reset_)
        result["duration_until_reset"] = WriteTimeToJson(status.duration_until_reset_->seconds_,
                                                         status.duration_until_reset_->nanos_);
    if (status.quota_) result["quota"] = WriteQuotaToJson(*status.quota_);
    return result;
}

std::vector<Response::HeaderValue> MakeHeadersFromJson(const json& document) {
    if (!document.is_array()) throw std::runtime_error("headers must be an array");
    std::vector<Response::HeaderValue> headers;
    for (const auto& item : document) {
        headers.push_back({item.at("key").get<std::string>(),
                           item.at("value").get<std::string>(),
                           item.at("raw_value").get<std::string>()});
    }
    return headers;
}

json WriteHeadersToJson(const std::vector<Response::HeaderValue>& headers) {
    json result = json::array();
    for (const auto& header : headers)
        result.push_back({{"key", header.key_}, {"value", header.value_},
                          {"raw_value", header.raw_value_}});
    return result;
}

Response MakeResponseFromJson(const json& document) {
    Response response;
    response.set_overall_code(MakeCode(document.at("overall_code")));
    const auto& statuses = document.at("statuses");
    if (!statuses.is_array()) throw std::runtime_error("statuses must be an array");
    for (const auto& status : statuses)
        response.statuses_.push_back(MakeStatusFromJson(status));
    response.response_headers_to_add_ = MakeHeadersFromJson(document.at("response_headers_to_add"));
    response.request_headers_to_add_ = MakeHeadersFromJson(document.at("request_headers_to_add"));
    response.raw_body_ = document.at("raw_body").get<std::string>();
    if (HasValue(document, "dynamic_metadata")) {
        const auto& metadata = document.at("dynamic_metadata");
        if (!metadata.is_object()) throw std::runtime_error("dynamic_metadata must be an object");
        response.dynamic_metadata_ = metadata;
    }
    if (HasValue(document, "quota"))
        response.quota_ = MakeQuotaFromJson(document.at("quota"));
    return response;
}

void WriteResponseToJson(json& output, const Response& response) {
    output = {{"overall_code", CodeName(response.overall_code_)},
              {"statuses", json::array()},
              {"response_headers_to_add", WriteHeadersToJson(response.response_headers_to_add_)},
              {"request_headers_to_add", WriteHeadersToJson(response.request_headers_to_add_)},
              {"raw_body", response.raw_body_}};
    for (const auto& status : response.statuses_)
        output["statuses"].push_back(WriteStatusToJson(status));
    if (response.dynamic_metadata_) output["dynamic_metadata"] = *response.dynamic_metadata_;
    if (response.quota_) output["quota"] = WriteQuotaToJson(*response.quota_);
}

// Write before comparing so a failed check leaves output that you can inspect.
void WriteAndCompare(const std::string& path, const json& document, const json& output) {
    std::filesystem::create_directories("build/ratelimit_wire_output");
    std::ofstream file(path);
    if (!file) throw std::runtime_error("could not open " + path);
    file << output.dump(2) << '\n';
    file.close();
    if (!file) throw std::runtime_error("could not write " + path);
    if (document != output)
        throw std::runtime_error("JSON differs: inspect " + path);
    std::cout << "JSON matches: " << path << '\n';
}

} // namespace

int main(int argc, char** argv) {
    try {
        const bool response_mode = argc >= 2 && std::string(argv[1]) == "--response";
        const bool request_mode = argc == 1 ||
            (argc >= 2 && std::string(argv[1]) == "--request");
        if ((!response_mode && !request_mode) || argc > 3)
            throw std::runtime_error("usage: ./out [--request|--response [fixture.json]]");

        const std::string kind = response_mode ? "response" : "request";
        const std::string input_path = argc == 3 ? argv[2] : "tests/fixtures/ratelimit_wire/" + kind + ".json";
        std::ifstream input(input_path);
        if (!input) throw std::runtime_error("could not open " + input_path);
        json document;
        input >> document;

        json native_json, decoded_json;
        size_t byte_count = 0;
        if (response_mode) {
            // Phase 1: fixture JSON -> C++ response -> JSON.
            const auto response = MakeResponseFromJson(document);
            WriteResponseToJson(native_json, response);
            WriteAndCompare("build/ratelimit_wire_output/response_native.json", document, native_json);

            // Phase 2: C++ response -> payload bytes -> C++ response -> JSON.
            const auto bytes = xrpc_conversion::encode_response(response, kMaxPayload);
            // In the real client, this code comes from the fixed DPO header.
            const auto decoded = xrpc_conversion::decode_response(
                bytes.data(), bytes.size(), response.overall_code_, kMaxPayload);
            WriteResponseToJson(decoded_json, decoded);
            byte_count = bytes.size();
        } else {
            const auto request = MakeRequestFromJson(document);
            WriteRequestToJson(native_json, request);
            WriteAndCompare("build/ratelimit_wire_output/request_native.json", document, native_json);
            const auto bytes = xrpc_conversion::encode_request(request, kMaxPayload);
            const auto decoded = xrpc_conversion::decode_request(bytes.data(), bytes.size(), kMaxPayload);
            WriteRequestToJson(decoded_json, decoded);
            byte_count = bytes.size();
        }
        WriteAndCompare("build/ratelimit_wire_output/" + kind + "_out.json", document, decoded_json);
        std::cout << "Encoded and decoded " << byte_count << " payload bytes\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
