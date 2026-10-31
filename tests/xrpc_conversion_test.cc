#include "xrpc_conversion.h"

#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace wire = xrpc_conversion;
using Response = rlc::RateLimitResponse;
static void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
static void rejects(const std::function<void()>& action) {
    try { action(); } catch (const std::runtime_error&) { return; }
    throw std::runtime_error("expected rejection");
}

static wire::Bytes metadata_payload(const std::string& json) {
    // No statuses/headers/body/quota; metadata is the only optional field.
    wire::Bytes bytes = {0, 1, 0, 0, 0, 0,
                        static_cast<uint8_t>(json.size()), static_cast<uint8_t>(json.size() >> 8)};
    bytes.insert(bytes.end(), json.begin(), json.end());
    return bytes;
}

static void full_response_tests() {
    constexpr size_t kExampleHeaderCount = 16;
    Response empty;
    const wire::Bytes empty_bytes(6, 0);
    check(wire::encode_response(empty) == empty_bytes, "empty full-response golden bytes");
    auto decoded = wire::decode_response(empty_bytes.data(), empty_bytes.size(), Response::Code::UNKNOWN);
    check(decoded.overall_code_ == Response::Code::UNKNOWN && decoded.statuses_.empty(), "empty error response");
    check(decoded.response_headers_to_add_.empty() && decoded.request_headers_to_add_.empty() &&
          decoded.raw_body_.empty() && !decoded.dynamic_metadata_ && !decoded.quota_, "absent response extras");

    Response response;
    response.overall_code_ = Response::Code::OVER_LIMIT;
    Response::DescriptorStatus status;
    status.set_code(Response::Code::OVER_LIMIT);
    status.set_limit_remaining(0);
    status.current_limit_ = Response::RateLimit{"named-limit", 5, Response::RateLimit::Unit::MINUTE};
    status.duration_until_reset_.emplace(17, 999);
    status.quota_ = Response::Quota{1, "descriptor-quota", Response::Timestamp{1234, 567}};
    response.statuses_ = {status};
    response.response_headers_to_add_ = {{"x-limit", "one", ""}, {"x-limit", "two", ""},
                                         {"x-binary", "", std::string("\0\xff", 2)}};
    response.request_headers_to_add_ = {{"x-policy", "basic", std::string("\0raw", 4)}};
    response.raw_body_ = std::string("\0denied\xff", 8);
    response.dynamic_metadata_ = nlohmann::json{
        {"tier", "gold"}, {"limited", true}, {"remaining", 0}, {"ratio", 0.5},
        {"nested", {{"empty", nlohmann::json::object()}}},
        {"list", nlohmann::json::array({1, nullptr, "hello"})}};
    response.quota_ = Response::Quota{17, "overall-quota", Response::Timestamp{-123, 456}};
    auto bytes = wire::encode_response(response);
    decoded = wire::decode_response(bytes.data(), bytes.size(), response.overall_code_);
    check(decoded.overall_code_ == Response::Code::OVER_LIMIT, "overall code supplied from header");
    check(decoded.statuses_.size() == 1 && decoded.statuses_[0].code() == status.code() &&
          decoded.statuses_[0].current_limit_->name() == "named-limit" &&
          decoded.statuses_[0].duration_until_reset_->nanos_ == 999 &&
          decoded.statuses_[0].quota_->id_ == "descriptor-quota", "nested status in full response");
    check(decoded.response_headers_to_add_.size() == 3 &&
          decoded.response_headers_to_add_[0].key_ == "x-limit" &&
          decoded.response_headers_to_add_[0].value_ == "one" &&
          decoded.response_headers_to_add_[1].key_ == "x-limit" &&
          decoded.response_headers_to_add_[1].value_ == "two", "duplicate headers and order");
    check(decoded.response_headers_to_add_[2].raw_value_ == std::string("\0\xff", 2), "binary response header");
    check(decoded.request_headers_to_add_.size() == 1 &&
          decoded.request_headers_to_add_[0].key_ == "x-policy" &&
          decoded.request_headers_to_add_[0].value_ == "basic" &&
          decoded.request_headers_to_add_[0].raw_value_ == std::string("\0raw", 4), "request header fields");
    check(decoded.raw_body_ == response.raw_body_, "binary raw body");
    check(decoded.dynamic_metadata_ == response.dynamic_metadata_, "nested JSON metadata");
    check(decoded.quota_ && decoded.quota_->requests_ == 17 && decoded.quota_->id_ == "overall-quota" &&
          decoded.quota_->valid_until_->seconds_ == -123 && decoded.quota_->valid_until_->nanos_ == 456,
          "response-level quota and signed timestamp");

    for (size_t size = 0; size < bytes.size(); ++size) {
        rejects([&] { wire::decode_response(bytes.data(), size, Response::Code::OVER_LIMIT); });
    }
    bytes.push_back(0);
    rejects([&] { wire::decode_response(bytes.data(), bytes.size(), Response::Code::OVER_LIMIT); });
    rejects([&] { wire::decode_response(nullptr, 1, Response::Code::OK); });
    rejects([&] { wire::decode_response(empty_bytes.data(), empty_bytes.size(), static_cast<Response::Code>(3)); });
    response.overall_code_ = static_cast<Response::Code>(3);
    rejects([&] { wire::encode_response(response); });

    // A present empty metadata object, quota, or timestamp is not absence.
    response = Response{};
    response.dynamic_metadata_ = nlohmann::json::object();
    response.quota_ = Response::Quota{};
    bytes = wire::encode_response(response);
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.dynamic_metadata_ && decoded.dynamic_metadata_->is_object() && decoded.dynamic_metadata_->empty(),
          "present empty metadata object");
    check(decoded.quota_ && decoded.quota_->requests_ == 0 && decoded.quota_->id_.empty() &&
          !decoded.quota_->valid_until_, "present zero quota with absent expiration");
    response.quota_->valid_until_ = Response::Timestamp{0, 0};
    bytes = wire::encode_response(response);
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.quota_->valid_until_ && decoded.quota_->valid_until_->seconds_ == 0, "present epoch expiration");

    for (size_t offset : {size_t(1), size_t(2), size_t(3)}) {
        bytes = empty_bytes;
        bytes[offset] = offset == 1 ? 4 : static_cast<uint8_t>(kExampleHeaderCount + 1);
        rejects([&] { wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK); });
    }
    bytes = {0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2};
    rejects([&] { wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK); });

    for (const auto& bad_json : std::vector<std::string>{"{", "[]", "null", "42", "{\"x\":NaN}",
                                                       std::string("{\"x\":\"\xff\"}", 9), std::string("{}\0junk", 7)}) {
        bytes = metadata_payload(bad_json);
        rejects([&] { wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK); });
    }
    bytes = metadata_payload(R"({"zero":"\u0000"})");
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.dynamic_metadata_ && decoded.dynamic_metadata_->at("zero").get<std::string>() == std::string("\0", 1),
          "escaped JSON NUL preserved");
    response = Response{};
    for (const auto& invalid : std::vector<nlohmann::json>{nullptr, nlohmann::json::array(), 7, "text"}) {
        response.dynamic_metadata_ = invalid;
        rejects([&] { wire::encode_response(response); });
    }
    response.dynamic_metadata_ = nlohmann::json{{"bad", std::numeric_limits<double>::infinity()}};
    rejects([&] { wire::encode_response(response); });
    response.dynamic_metadata_ = nlohmann::json{{"bad", std::numeric_limits<double>::quiet_NaN()}};
    rejects([&] { wire::encode_response(response); });
    response.dynamic_metadata_ = nlohmann::json{{"bad", std::string("\xff", 1)}};
    rejects([&] { wire::encode_response(response); });
    response.dynamic_metadata_ = nlohmann::json{{"binary", nlohmann::json::binary({0, 1, 2})}};
    rejects([&] { wire::encode_response(response); });
    nlohmann::json deep = nlohmann::json::object();
    for (int i = 0; i < 40; ++i) deep = nlohmann::json{{"child", std::move(deep)}};
    // Nested metadata is limited by the payload budget.
    response.dynamic_metadata_ = deep;
    bytes = wire::encode_response(response);
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.dynamic_metadata_ == deep, "nested metadata round-trip");

    // Boundary of the aggregate payload budget, not a blanket 64-byte blob cap.
    response = Response{};
    response.raw_body_ = std::string(wire::kMaxPayloadBytes - empty_bytes.size(), '\xff');
    bytes = wire::encode_response(response);
    check(bytes.size() == wire::kMaxPayloadBytes, "exact full-response payload limit");
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.raw_body_ == response.raw_body_, "large body preserved");
    response.raw_body_.push_back('x');
    rejects([&] { wire::encode_response(response); });
    bytes.push_back('x');
    rejects([&] { wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK); });
    response = Response{};
    response.response_headers_to_add_.resize(kExampleHeaderCount);
    response.request_headers_to_add_.resize(kExampleHeaderCount);
    bytes = wire::encode_response(response);
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.response_headers_to_add_.size() == kExampleHeaderCount &&
          decoded.request_headers_to_add_.size() == kExampleHeaderCount, "example header list counts");
    response.response_headers_to_add_.emplace_back();
    response.request_headers_to_add_.emplace_back();
    bytes = wire::encode_response(response);
    decoded = wire::decode_response(bytes.data(), bytes.size(), Response::Code::OK);
    check(decoded.response_headers_to_add_.size() == kExampleHeaderCount + 1 &&
          decoded.request_headers_to_add_.size() == kExampleHeaderCount + 1,
          "larger header lists round-trip");
    response.response_headers_to_add_.resize(256);
    rejects([&] { wire::encode_response(response); });
    response.response_headers_to_add_.clear();
    response.request_headers_to_add_.resize(256);
    rejects([&] { wire::encode_response(response); });
    response = Response{};
    response.dynamic_metadata_ = nlohmann::json{{"large", std::string(wire::kMaxPayloadBytes, 'x')}};
    rejects([&] { wire::encode_response(response); });
}

// Test application payloads only. The 35-byte DPO header is added separately.
static void small_packet_round_trip_test() {
    // A small request with one descriptor and one entry.
    rlc::RateLimitRequest small;
    small.domain_ = "m";
    small.hits_addend_ = 0;
    small.descriptors_.emplace_back().entries_.emplace_back("k", "v");
    const wire::Bytes small_request_expected = {
        1, 'm', 0, 0, 0, 0, 1, 1, 0, 1, 'k', 1, 'v'
    };
    auto bytes = wire::encode_request(small);
    check(bytes == small_request_expected, "small request wire bytes");
    auto decoded_request = wire::decode_request(bytes.data(), bytes.size());
    check(decoded_request.domain_ == "m" && decoded_request.hits_addend_ == 0 &&
          decoded_request.descriptors_.size() == 1 &&
          decoded_request.descriptors_[0].entries_.size() == 1 &&
          decoded_request.descriptors_[0].entries_[0].key_ == "k" &&
          decoded_request.descriptors_[0].entries_[0].value_ == "v",
          "small request round trip");

    // A small response: overall_code is supplied separately by the DPO header.
    Response small_response;
    small_response.overall_code_ = Response::Code::OK;
    auto& small_status = small_response.statuses_.emplace_back();
    small_status.set_code(Response::Code::OK);
    small_status.set_limit_remaining(4);
    const wire::Bytes small_response_expected = {
        1, 1, 4, 0, 0, 0, 0, // one status: code, remaining, flags
        0, 0, 0, 0, 0       // response flags, header counts, empty body length
    };
    bytes = wire::encode_response(small_response);
    check(bytes == small_response_expected, "small response wire bytes");
    auto decoded_response = wire::decode_response(bytes.data(), bytes.size(),
                                                 small_response.overall_code_);
    check(decoded_response.overall_code_ == Response::Code::OK &&
          decoded_response.statuses_.size() == 1 &&
          decoded_response.statuses_[0].code() == Response::Code::OK &&
          decoded_response.statuses_[0].limit_remaining() == 4,
          "small response round trip");

}

static void large_packet_round_trip_test() {
    // Three entries fill the request payload exactly to 1,465 bytes.
    // Each string length still fits in one byte.
    rlc::RateLimitRequest large;
    large.domain_ = "d";
    large.hits_addend_ = 0x12345678;
    auto& entries = large.descriptors_.emplace_back().entries_;
    entries.emplace_back(std::string(255, 'a'), std::string(255, 'b'));
    entries.emplace_back(std::string(255, 'c'), std::string(255, 'd'));
    entries.emplace_back(std::string(255, 'e'), std::string(175, 'f'));
    auto bytes = wire::encode_request(large);
    check(bytes.size() == wire::kMaxPayloadBytes, "exact maximum request payload size");
    auto decoded_request = wire::decode_request(bytes.data(), bytes.size());
    check(decoded_request.domain_ == large.domain_ &&
          decoded_request.hits_addend_ == large.hits_addend_ &&
          decoded_request.descriptors_.size() == 1 &&
          decoded_request.descriptors_[0].entries_.size() == entries.size(),
          "large request fields");
    for (size_t i = 0; i < entries.size(); ++i) {
        check(decoded_request.descriptors_[0].entries_[i].key_ == entries[i].key_ &&
              decoded_request.descriptors_[0].entries_[i].value_ == entries[i].value_,
              "large request entry contents");
    }
    check(wire::encode_request(decoded_request) == bytes, "large request re-encodes identically");
    entries.back().value_.push_back('f');
    rejects([&] { wire::encode_request(large); });
    bytes.push_back(0);
    rejects([&] { wire::decode_request(bytes.data(), bytes.size()); });

    // Fill a realistic response (status, header, metadata and binary body) to
    // the same boundary. The six-byte empty-response check is covered above.
    Response large_response;
    large_response.overall_code_ = Response::Code::OVER_LIMIT;
    auto& status = large_response.statuses_.emplace_back();
    status.set_code(Response::Code::OVER_LIMIT);
    status.set_limit_remaining(7);
    large_response.response_headers_to_add_.push_back({"x-limit", "5", ""});
    large_response.dynamic_metadata_ = nlohmann::json{{"reason", "quota"}};
    const auto overhead = wire::encode_response(large_response).size();
    check(overhead < wire::kMaxPayloadBytes, "large response has room for body");
    large_response.raw_body_.resize(wire::kMaxPayloadBytes - overhead);
    for (size_t i = 0; i < large_response.raw_body_.size(); ++i)
        large_response.raw_body_[i] = static_cast<char>(i % 128);
    bytes = wire::encode_response(large_response);
    check(bytes.size() == wire::kMaxPayloadBytes, "exact maximum response payload size");
    auto decoded_response = wire::decode_response(bytes.data(), bytes.size(), large_response.overall_code_);
    check(decoded_response.overall_code_ == large_response.overall_code_ &&
          decoded_response.statuses_.size() == 1 &&
          decoded_response.statuses_[0].code() == Response::Code::OVER_LIMIT &&
          decoded_response.statuses_[0].limit_remaining() == 7 &&
          decoded_response.response_headers_to_add_.size() == 1 &&
          decoded_response.response_headers_to_add_[0].key_ == "x-limit" &&
          decoded_response.response_headers_to_add_[0].value_ == "5" &&
          decoded_response.dynamic_metadata_ == large_response.dynamic_metadata_ &&
          decoded_response.raw_body_ == large_response.raw_body_,
          "large response round trip");
    check(wire::encode_response(decoded_response) == bytes, "large response re-encodes identically");
    large_response.raw_body_.push_back('x');
    rejects([&] { wire::encode_response(large_response); });
    bytes.push_back(0);
    rejects([&] { wire::decode_response(bytes.data(), bytes.size(), Response::Code::OVER_LIMIT); });

}

static bool run_round_trip_test(const char* name, const std::function<void()>& test) {
    try {
        test();
        std::cout << "PASS: " << name << '\n';
        return true;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << name << ": " << error.what() << '\n';
        return false;
    }
}

int main() {
    // Run both size cases independently, so either failure identifies its case.
    const bool small_passed = run_round_trip_test("small packet round trip", small_packet_round_trip_test);
    const bool large_passed = run_round_trip_test("large packet round trip", large_packet_round_trip_test);
    if (!small_passed || !large_passed) return 1;

    try {
        rlc::RateLimitRequest request;
        request.domain_ = "a";
        request.hits_addend_ = 2;
        auto& descriptor = request.descriptors_.emplace_back();
        descriptor.entries_.push_back({"k", "v"});
        descriptor.limit_ = rlc::RateLimitDescriptor::Override{5, rlc::RateLimitUnit::DAY};
        descriptor.hits_addend_ = 0;
        const wire::Bytes expected = {1,'a',2,0,0,0,1,1,3,5,0,0,0,4,
                                      0,0,0,0,0,0,0,0,1,'k',1,'v'};
        check(wire::encode_request(request) == expected, "request golden bytes");
        auto decoded = wire::decode_request(expected.data(), expected.size());
        check(decoded.domain() == "a" && decoded.hits_addend() == 2, "request values");
        check(decoded.descriptors()[0].hits_addend_ == 0, "present zero hits");
        request.descriptors_[0].hits_addend_.reset();
        request.descriptors_[0].is_negative_hits_ = true;
        auto bytes = wire::encode_request(request);
        decoded = wire::decode_request(bytes.data(), bytes.size());
        check(!decoded.descriptors()[0].has_hits_addend() && decoded.descriptors()[0].is_negative_hits(), "absent hits and negative flag");
        for (size_t size = 0; size < expected.size(); ++size) {
            rejects([&] { wire::decode_request(expected.data(), size); });
        }
        bytes = expected; bytes.push_back(0);
        rejects([&] { wire::decode_request(bytes.data(), bytes.size()); });
        bytes = expected; bytes[8] = 8;
        rejects([&] { wire::decode_request(bytes.data(), bytes.size()); });
        bytes = expected; bytes[13] = 7; // WEEK is response-only.
        rejects([&] { wire::decode_request(bytes.data(), bytes.size()); });
        request.domain_ = std::string(255, 'd');
        bytes = wire::encode_request(request);
        check(wire::decode_request(bytes.data(), bytes.size()).domain() == request.domain_,
              "one-byte domain length boundary");
        request.domain_.push_back('d');
        rejects([&] { wire::encode_request(request); });

        // Maximal request still fits the current 1500-byte socket buffer.
        request.domain_ = std::string(64, 'd');
        request.descriptors_.resize(5, request.descriptors_.front());
        for (auto& item : request.descriptors_) {
            item.entries_ = {{std::string(64, 'k'), std::string(64, 'v')},
                             {std::string(64, 'x'), std::string(64, 'y')}};
            item.hits_addend_ = std::numeric_limits<uint64_t>::max();
        }
        bytes = wire::encode_request(request);
        check(bytes.size() == 1445, "max request size");
        check(bytes.size() + wire::kHeaderSize <= wire::kMaxPacketBytes, "request packet bound");
        decoded = wire::decode_request(bytes.data(), bytes.size());
        check(decoded.descriptors().size() == 5 && decoded.descriptors()[4].entries()[1].value().size() == 64, "max request decode");
        request.descriptors_[0].entries_.push_back({"extra", "entry"});
        bytes = wire::encode_request(request);
        check(wire::decode_request(bytes.data(), bytes.size()).descriptors()[0].entries_size() == 3,
              "three entries round-trip");
        request.descriptors_.resize(6);
        bytes = wire::encode_request(request);
        check(wire::decode_request(bytes.data(), bytes.size()).descriptors_size() == 6,
              "six descriptors round-trip");
        request.descriptors_.resize(256);
        rejects([&] { wire::encode_request(request); });

        Response::DescriptorStatus simple;
        simple.set_code(Response::Code::OK);
        simple.set_limit_remaining(4);
        check(wire::encode_statuses({simple}) == wire::Bytes({1,1,4,0,0,0,0}), "response golden bytes");
        check(wire::encode_statuses({}) == wire::Bytes({0}), "empty status list");
        Response::DescriptorStatus full = simple;
        full.current_limit_ = Response::RateLimit{"weekly", 17, Response::RateLimit::Unit::WEEK};
        full.duration_until_reset_.emplace(-123, -456);
        full.quota_ = Response::Quota{0, std::string("q\0id", 4), Response::Timestamp{-42, 987}};
        bytes = wire::encode_statuses({full, simple});
        const auto statuses = wire::decode_statuses(bytes.data(), bytes.size());
        check(statuses.size() == 2 && statuses[0].current_limit_->unit() == Response::RateLimit::Unit::WEEK, "response WEEK");
        check(statuses[0].duration_until_reset_->seconds_ == -123 && statuses[0].duration_until_reset_->nanos_ == -456, "signed duration");
        check(statuses[0].quota_->id_ == std::string("q\0id", 4) && statuses[0].quota_->valid_until_->seconds_ == -42, "quota ID and timestamp");
        check(!statuses[1].current_limit_ && !statuses[1].duration_until_reset_ && !statuses[1].quota_, "absent response fields");
        for (size_t size = 0; size < bytes.size(); ++size) {
            rejects([&] { wire::decode_statuses(bytes.data(), size); });
        }
        bytes.push_back(0);
        rejects([&] { wire::decode_statuses(bytes.data(), bytes.size()); });
        bytes = wire::encode_statuses({simple}); bytes[6] = 8;
        rejects([&] { wire::decode_statuses(bytes.data(), bytes.size()); });
        bytes = wire::encode_statuses({simple}); bytes[1] = 3;
        rejects([&] { wire::decode_statuses(bytes.data(), bytes.size()); });
        full.quota_->valid_until_.reset();
        full.duration_until_reset_.emplace(0, 0);
        bytes = wire::encode_statuses({full});
        auto result = wire::decode_statuses(bytes.data(), bytes.size());
        check(result[0].duration_until_reset_ && result[0].duration_until_reset_->seconds_ == 0, "present zero duration");
        check(result[0].quota_ && !result[0].quota_->valid_until_, "absent quota expiration");
        full.quota_->valid_until_ = Response::Timestamp{0, 0};
        full.current_limit_->set_name(std::string(64, 'n'));
        full.quota_->id_ = std::string(64, 'i');
        bytes = wire::encode_statuses(wire::Statuses(5, full));
        check(bytes.size() == 851 && bytes.size() + wire::kHeaderSize <= wire::kMaxPacketBytes, "max response size");
        result = wire::decode_statuses(bytes.data(), bytes.size());
        check(result[4].quota_->valid_until_.has_value(), "present zero timestamp");
        bytes = wire::encode_statuses(wire::Statuses(6, full));
        check(wire::decode_statuses(bytes.data(), bytes.size()).size() == 6,
              "six statuses round-trip");
        rejects([&] { wire::encode_statuses(wire::Statuses(256, full)); });
        full.quota_->id_ = std::string(255, 'x');
        bytes = wire::encode_statuses({full});
        check(wire::decode_statuses(bytes.data(), bytes.size())[0].quota_->id_ == full.quota_->id_,
              "one-byte quota ID length boundary");
        full.quota_->id_.push_back('x');
        rejects([&] { wire::encode_statuses({full}); });
        bytes.resize(wire::kMaxPayloadBytes + 1);
        rejects([&] { wire::decode_statuses(bytes.data(), bytes.size()); });
        rejects([&] { wire::decode_request(nullptr, 1); });
        full_response_tests();
        std::cout << "PASS: wire bytes, optional values, full response fields, JSON, quotas, bounds and malformed payloads\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
