#include "xrpc_conversion.h"
#include <cassert>
#include <stdexcept>
#include <string>

using namespace xrpc_conversion;
using Response = rlc::RateLimitResponse;
int main() {
    rlc::RateLimitRequest request;
    request.domain_ = std::string(16, 'd');
    request.hits_addend_ = 1;

    rlc::RateLimitDescriptor descriptor;
    descriptor.entries_.emplace_back("key", std::string("a\0b", 3));
    descriptor.limit_.emplace(5, rlc::RateLimitUnit::MINUTE);

    request.descriptors_.push_back(descriptor);

    auto encoded = encode_request_with_fast_path(request, 1500);

    assert(encoded.fast_path && encoded.fast_path->domain.length == 16);
    assert(encoded.fast_path->entry_value.length == 3 && encoded.fast_path->entry_value.bytes[1] == 0);
    assert(encoded.fast_path->entry_key.bytes[15] == 0);
    assert(encoded.payload == encode_request(request, 1500));

    auto decoded = decode_request(encoded.payload.data(), encoded.payload.size(), 1500);
    assert(decoded.domain() == request.domain() && decoded.descriptors()[0].entries()[0].value() == std::string("a\0b", 3));

    request.domain_.push_back('x');
    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    request.domain_ = "d";
    request.descriptors_[0].is_negative_hits_ = true;

    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    request.descriptors_[0].is_negative_hits_ = false;
    request.descriptors_[0].limit_.reset();

    assert(!encode_request_with_fast_path(request, 1500).fast_path);

    request.descriptors_[0].limit_ = descriptor.limit_;
    request.descriptors_[0].entries_.emplace_back("second", "entry");

    assert(!encode_request_with_fast_path(request, 1500).fast_path);

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

    response.dynamic_metadata_ = nlohmann::json::object();

    assert(!encode_response_with_fast_path(response, 1500).fast_path);

    response.dynamic_metadata_.reset();
    response.statuses_[0].current_limit_->set_name(std::string(17, 'n'));

    assert(!encode_response_with_fast_path(response, 1500).fast_path);

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
