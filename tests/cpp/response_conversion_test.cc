#include "grpc_conversion.h"
#include <google/protobuf/util/message_differencer.h>
#include <cassert>
#include <iostream>
#include <limits>

using rlc::grpc_conversion::FromProto;
using rlc::grpc_conversion::ToProto;
using Response = rlc::RateLimitResponse;
using Proto = rlc::GrpcRateLimitResponse;
using Json = nlohmann::json;

void TestEmptyAndPresence() {
    const auto empty = FromProto(Proto{});
    assert(empty.overall_code_ == Response::Code::UNKNOWN);
    assert(empty.statuses_.empty());
    assert(!empty.dynamic_metadata_ && !empty.quota_);
    assert(ToProto(empty).SerializeAsString().empty());

    Proto source;
    source.mutable_dynamic_metadata();
    source.mutable_quota()->mutable_valid_until();
    auto* status = source.add_statuses();
    status->mutable_current_limit();
    status->mutable_duration_until_reset();
    status->mutable_quota();
    const auto native = FromProto(source);
    assert(native.dynamic_metadata_ && native.dynamic_metadata_->empty());
    assert(native.quota_ && native.quota_->valid_until_);
    assert(native.quota_->valid_until_->seconds_ == 0);
    assert(native.statuses_[0].current_limit_);
    assert(native.statuses_[0].duration_until_reset_);
    assert(native.statuses_[0].quota_ && !native.statuses_[0].quota_->valid_until_);
    assert(google::protobuf::util::MessageDifferencer::Equals(source, ToProto(native)));
}

void TestComplexResponse() {
    Response source;
    source.overall_code_ = Response::Code::OVER_LIMIT;
    Response::DescriptorStatus status;
    status.code_ = Response::Code::OVER_LIMIT;
    status.current_limit_ = Response::RateLimit{"weekly", 5, Response::RateLimit::Unit::WEEK};
    status.limit_remaining_ = std::numeric_limits<uint32_t>::max();
    status.duration_until_reset_ = Response::Duration{42, 123456789};
    status.quota_ = Response::Quota{7, "descriptor-quota", Response::Timestamp{1700000000, 987654321}};
    source.statuses_.push_back(status);
    source.statuses_.push_back(Response::DescriptorStatus{});
    source.response_headers_to_add_.push_back({"x-limit", "5", std::string("a\0b", 3)});
    source.response_headers_to_add_.push_back({"x-second", "yes", ""});
    source.request_headers_to_add_.push_back({"x-request", "allowed", ""});
    source.raw_body_ = std::string("\0\xffx", 3);
    source.quota_ = Response::Quota{9, "overall-quota", std::nullopt};

    Json metadata = {
        {"null", nullptr}, {"number", 3.25}, {"string", "hello"}, {"bool", false},
        {"integer", -42}, {"unsigned", uint64_t{123}},
        {"empty-object", Json::object()}, {"empty-list", Json::array()},
        {"nested", {{"items", Json::array({true, nullptr, {{"n", 2.0}}})}}}
    };
    source.dynamic_metadata_ = metadata;

    const auto proto = ToProto(source);
    assert(proto.overall_code() == Proto::OVER_LIMIT);
    assert(proto.statuses_size() == 2);
    const auto& first = proto.statuses(0);
    assert(first.code() == Proto::OVER_LIMIT);
    assert(first.current_limit().name() == "weekly");
    assert(first.current_limit().requests_per_unit() == 5);
    assert(first.current_limit().unit() == Proto::RateLimit::WEEK);
    assert(first.limit_remaining() == std::numeric_limits<uint32_t>::max());
    assert(first.duration_until_reset().seconds() == 42);
    assert(first.duration_until_reset().nanos() == 123456789);
    assert(first.quota().requests() == 7 && first.quota().id() == "descriptor-quota");
    assert(first.quota().valid_until().seconds() == 1700000000);
    assert(first.quota().valid_until().nanos() == 987654321);
    assert(!proto.statuses(1).has_current_limit());
    assert(proto.response_headers_to_add_size() == 2);
    assert(proto.response_headers_to_add(0).raw_value() == source.response_headers_to_add_[0].raw_value_);
    assert(proto.response_headers_to_add(1).key() == "x-second");
    assert(proto.request_headers_to_add(0).value() == "allowed");
    assert(proto.raw_body() == source.raw_body_);
    assert(proto.quota().requests() == 9 && !proto.quota().has_valid_until());
    const auto& fields = proto.dynamic_metadata().fields();
    assert(fields.at("null").kind_case() == google::protobuf::Value::kNullValue);
    assert(fields.at("number").number_value() == 3.25);
    assert(fields.at("bool").kind_case() == google::protobuf::Value::kBoolValue);
    assert(fields.at("empty-object").has_struct_value());
    assert(fields.at("empty-list").has_list_value());
    assert(fields.at("nested").struct_value().fields().at("items").list_value().values_size() == 3);

    const auto native = FromProto(proto);
    assert(native.raw_body_ == source.raw_body_);
    assert(native.statuses_[0].current_limit_->unit_ == Response::RateLimit::Unit::WEEK);
    assert(native.quota_->id_ == "overall-quota");
    assert(native.dynamic_metadata_->at("null").is_null());
    assert(native.dynamic_metadata_->at("string").get<std::string>() == "hello");
    // Compare messages, not serialized bytes: metadata maps have no wire order.
    assert(google::protobuf::util::MessageDifferencer::Equals(proto, ToProto(native)));
}

void TestReusableHelpers() {
    for (int unit : {0, 1, 2, 3, 4, 5, 6, 7, 99}) {
        Proto::RateLimit proto;
        proto.set_unit(static_cast<rlc::GrpcRateLimitUnit>(unit));
        assert(static_cast<int>(FromProto(proto).unit_) == unit);
        assert(static_cast<int>(ToProto(FromProto(proto)).unit()) == unit);
    }
    for (int code : {0, 1, 2, 99}) {
        rlc::GrpcRateLimitDescriptorStatus status;
        status.set_code(static_cast<Proto::Code>(code));
        assert(static_cast<int>(FromProto(status).code_) == code);
        assert(static_cast<int>(ToProto(FromProto(status)).code()) == code);
        Proto response;
        response.set_overall_code(static_cast<Proto::Code>(code));
        assert(static_cast<int>(ToProto(FromProto(response)).overall_code()) == code);
    }
    Response::Duration duration{-10, -123};
    assert(FromProto(ToProto(duration)).seconds_ == -10);
    assert(FromProto(ToProto(duration)).nanos_ == -123);
    Response::Timestamp timestamp{-10, 123};
    assert(FromProto(ToProto(timestamp)).seconds_ == -10);
    assert(FromProto(ToProto(timestamp)).nanos_ == 123);
}

void TestJsonMetadata() {
    using namespace rlc::grpc_conversion;
    google::protobuf::Value unset;
    assert(FromProto(unset).is_null());
    assert(ToProtoValue(FromProto(unset)).kind_case() == google::protobuf::Value::kNullValue);
    assert(ToProtoValue(Json(-42)).number_value() == -42.0);
    assert(ToProtoValue(Json(uint64_t{123})).number_value() == 123.0);
    assert(ToProtoList(Json::array()).values_size() == 0);
    auto rejects = [](auto operation) {
        bool rejected = false;
        try { operation(); } catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    };
    rejects([] { ToProtoStruct(Json::array()); });
    rejects([] { ToProtoList(Json::object()); });
    rejects([] { ToProtoValue(Json(std::numeric_limits<double>::infinity())); });
    rejects([] {
        Response response;
        response.dynamic_metadata_ = Json(nullptr);
        ToProto(response);
    });
}

int main() {
    TestJsonMetadata();
    TestEmptyAndPresence();
    TestComplexResponse();
    TestReusableHelpers();
    std::cout << "Response and nested-type conversion tests passed\n";
}
