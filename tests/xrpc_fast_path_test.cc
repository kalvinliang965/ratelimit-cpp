#include "xrpc_conversion.h"
#include <cassert>
#include <stdexcept>
#include <string>

using namespace xrpc_conversion;
using Response = rlc::RateLimitResponse;
int main() {
    rlc::RateLimitRequest request;

    // max bytes it accept
    request.domain_ = std::string(xrpc_conversion::kFastPathStringBytes, 'd');
    request.hits_addend_ = 1;

    rlc::RateLimitDescriptor descriptor;
    // make sure values can contain embedded null bytes
    descriptor.entries_.emplace_back("key", std::string("a\0b", 3));
    descriptor.limit_.emplace(5, rlc::RateLimitUnit::MINUTE);

    request.descriptors_.push_back(descriptor);

    const auto eligible_request = request;
    auto encoded = encode_request_with_fast_path(request, 1500);

    assert(encoded.fast_path && encoded.fast_path->domain.length == 16);
    assert(encoded.fast_path->entry_value.length == 3 && encoded.fast_path->entry_value.bytes[1] == 0); // make sure \0 is encoded properly
	
    assert(encoded.fast_path->entry_key.bytes[15] == 0); // unused bytes are 0s.
	
    // payload should consist the regular path (s1) data.
    assert(encoded.payload == encode_request(request, 1500));

    auto decoded = decode_request(encoded.payload.data(), encoded.payload.size(), 1500);
    assert(decoded.domain() == request.domain() && decoded.descriptors()[0].entries()[0].value() == std::string("a\0b", 3));

    // fast path string overflow
    assert(encoded.fast_path->override_unit == static_cast<uint8_t>(rlc::RateLimitUnit::MINUTE));
    assert(encoded.fast_path->override_requests_per_unit == 5);
    for (int which = 0; which < 2; ++which) {
        auto candidate = eligible_request;
        auto& entry = candidate.descriptors_[0].entries_[0];
        auto& field = which == 0 ? entry.key_ : entry.value_;
        field = std::string(16, 'a');
        assert(encode_request_with_fast_path(candidate, 1500).fast_path);
        field.push_back('b');
        assert(!encode_request_with_fast_path(candidate, 1500).fast_path);
    }
    auto empty_request = eligible_request;
    empty_request.descriptors_.clear();
    assert(!encode_request_with_fast_path(empty_request, 1500).fast_path);
    empty_request = eligible_request;
    empty_request.descriptors_[0].entries_.clear();
    assert(!encode_request_with_fast_path(empty_request, 1500).fast_path);
    request.domain_.push_back('x');
    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    // negative hit is rejected from fast path
    // because request may not be overflow after negative hits
    request.domain_ = "d";
    request.descriptors_[0].is_negative_hits_ = true;
    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    // missing override is rejected from s0.
    // we need the unit to construct the key
    request.descriptors_[0].is_negative_hits_ = false;
    request.descriptors_[0].limit_.reset();
    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    // more than one entry is rejeected from s0
    request.descriptors_[0].limit_ = descriptor.limit_;
    request.descriptors_[0].entries_.emplace_back("second", "entry");
    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    // more than one descriptor is not allowed.
    request.descriptors_[0].entries_.pop_back();
    assert(encode_request_with_fast_path(request, 1500).fast_path);
    request.descriptors_.push_back(descriptor);
    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    bool rejected = false;
    try { 
	encode_request_with_fast_path(request, 1); 
    } catch (const std::runtime_error&) { 
	rejected = true; 
    }

    assert(rejected);

    Response response;
    response.set_overall_code(Response::Code::OVER_LIMIT);

    auto& status = response.add_statuses();
    status.set_code(Response::Code::OVER_LIMIT);
    status.set_limit_remaining(0);

    Response::RateLimit limit;

    limit.set_name(std::string(16, 'n'));
    limit.set_unit(Response::RateLimit::Unit::MINUTE);
    limit.set_requests_per_unit(5);

    status.set_current_limit(limit);
    status.set_duration_until_reset({12, 345});

    auto result = encode_response_with_fast_path(response, 1500);

    assert(result.fast_path && result.payload == encode_response(response, 1500));

    auto full = decode_response(result.payload.data(), result.payload.size(), Response::Code::OVER_LIMIT, 1500);
    auto fast = response_from_fast_path(*result.fast_path);

    assert(fast.overall_code_ == full.overall_code_ && fast.statuses_.size() == 1);
    assert(fast.statuses_[0].code() == full.statuses_[0].code());
    assert(fast.statuses_[0].current_limit().name() == full.statuses_[0].current_limit().name());
    assert(fast.statuses_[0].current_limit().unit() == full.statuses_[0].current_limit().unit());
    assert(fast.statuses_[0].current_limit().requests_per_unit() == 5);
    assert(fast.statuses_[0].duration_until_reset().seconds() == 12 && fast.statuses_[0].duration_until_reset().nanos() == 345);

    assert(encode_response(fast, 1500) == result.payload);
    const auto eligible_response = response;
    auto candidate = eligible_response;
    candidate.response_headers_to_add_.push_back({"x", "y", ""});
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.request_headers_to_add_.push_back({"x", "y", ""});
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.raw_body_ = "body";
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.quota_.emplace();
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.statuses_[0].quota_.emplace();
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.statuses_[0].current_limit_.reset();
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.statuses_[0].duration_until_reset_.reset();
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.statuses_.push_back(candidate.statuses_[0]);
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);
    candidate = eligible_response;
    candidate.statuses_[0].limit_remaining_ = 1;
    assert(!encode_response_with_fast_path(candidate, 1500).fast_path);

    response.dynamic_metadata_ = nlohmann::json::object();

    assert(!encode_response_with_fast_path(response, 1500).fast_path);

    response.dynamic_metadata_.reset();
    response.statuses_[0].current_limit_->set_name(std::string(17, 'n'));

    assert(!encode_response_with_fast_path(response, 1500).fast_path);

    auto check_invalid = [](const FastPathResponse& value) {
        bool failed = false;
        try { response_from_fast_path(value); } catch (const std::runtime_error&) { failed = true; }
        assert(failed);
    };
    auto bad = *result.fast_path;
    bad.overall_code = 1; check_invalid(bad);
    bad = *result.fast_path; bad.status_code = 1; check_invalid(bad);
    bad = *result.fast_path; bad.limit_remaining = 1; check_invalid(bad);
    bad = *result.fast_path; bad.current_limit_unit = 8; check_invalid(bad);
    bad = *result.fast_path; bad.duration_until_reset_seconds = -1; check_invalid(bad);
    bad = *result.fast_path; bad.duration_until_reset_nanos = -1; check_invalid(bad);
    bad = *result.fast_path; bad.duration_until_reset_nanos = 1000000000; check_invalid(bad);

    auto invalid = *result.fast_path;
    invalid.current_limit_name.length = 17;
    rejected = false;

    try { 
	response_from_fast_path(invalid); 
    } catch (const std::runtime_error&) { 
        rejected = true;
    }

    assert(rejected);
}
