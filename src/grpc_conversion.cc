#include "types.h"
#include "grpc_conversion.h"

#include <cassert>
#include <iostream>
#include <utility>
#include <cmath>
#include <stdexcept>

namespace rlc::grpc_conversion {


RateLimitStatus FromProto(const GrpcRateLimitStatus& status) {
	return RateLimitStatus(static_cast<RateLimitStatusCode>(status.error_code()), status.error_message());
}

GrpcRateLimitStatus ToProto(const RateLimitStatus& status) {
	return GrpcRateLimitStatus(
		static_cast<grpc::StatusCode>(status.code()),
		status.message()
	    );
}

RateLimitDescriptor FromProto(const GrpcRateLimitDescriptor& source) {
    RateLimitDescriptor result;
    result.entries_.reserve(source.entries_size());
    for (const auto& entry : source.entries()) {
        result.entries_.push_back({entry.key(), entry.value()});
    }
    if (source.has_limit()) {
        result.limit_ = RateLimitDescriptor::Override{
            source.limit().requests_per_unit(),
            static_cast<RateLimitUnit>(source.limit().unit())
        };
    }
    if (source.has_hits_addend()) {
        result.hits_addend_ = source.hits_addend().value();
    }
    result.is_negative_hits_ = source.is_negative_hits();
    return result;
}

GrpcRateLimitDescriptor ToProto(const RateLimitDescriptor& source) {
    GrpcRateLimitDescriptor result;
    for (const auto& entry : source.entries()) {
        auto* output = result.add_entries();
        output->set_key(entry.key());
        output->set_value(entry.value());
    }
    if (source.has_limit()) {
        auto* limit = result.mutable_limit();
        limit->set_requests_per_unit(source.limit().requests_per_unit());
        limit->set_unit(static_cast<envoy::type::v3::RateLimitUnit>(source.limit().unit()));
    }
    if (source.has_hits_addend()) {
        result.mutable_hits_addend()->set_value(source.hits_addend().value());
    }
    result.set_is_negative_hits(source.is_negative_hits());
    return result;
}

RateLimitRequest FromProto(const GrpcRateLimitRequest& request) {
    RateLimitRequest result;
    result.domain_ = request.domain();
    result.hits_addend_ = request.hits_addend();
    result.descriptors_.reserve(request.descriptors_size());
    for (const auto& descriptor : request.descriptors()) {
        result.descriptors_.push_back(FromProto(descriptor));
    }
    return result;
}

GrpcRateLimitRequest ToProto(const RateLimitRequest& request) {
    GrpcRateLimitRequest result;
    result.set_domain(request.domain());
    result.set_hits_addend(request.hits_addend());
    for (const auto& descriptor : request.descriptors()) {
        *result.add_descriptors() = ToProto(descriptor);
    }
    return result;
}

RateLimitResponse::Timestamp FromProto(const google::protobuf::Timestamp& source) {
    return {source.seconds(), source.nanos()};
}

google::protobuf::Timestamp ToProto(const RateLimitResponse::Timestamp& source) {
    google::protobuf::Timestamp result;
    result.set_seconds(source.seconds_);
    result.set_nanos(source.nanos_);
    return result;
}

RateLimitResponse::Duration FromProto(const google::protobuf::Duration& source) {
    return {source.seconds(), source.nanos()};
}

google::protobuf::Duration ToProto(const RateLimitResponse::Duration& source) {
    google::protobuf::Duration result;
    result.set_seconds(source.seconds_);
    result.set_nanos(source.nanos_);
    return result;
}

RateLimitResponse::RateLimit FromProto(const GrpcRateLimitResponse::RateLimit& source) {
    return {source.name(), source.requests_per_unit(),
            static_cast<RateLimitResponse::RateLimit::Unit>(source.unit())};
}

GrpcRateLimitResponse::RateLimit ToProto(const RateLimitResponse::RateLimit& source) {
    GrpcRateLimitResponse::RateLimit result;
    result.set_name(source.name_);
    result.set_requests_per_unit(source.requests_per_unit_);
    result.set_unit(static_cast<GrpcRateLimitUnit>(source.unit_));
    return result;
}

RateLimitResponse::Quota FromProto(const GrpcRateLimitResponse::Quota& source) {
    RateLimitResponse::Quota result;
    result.requests_ = source.requests();
    result.id_ = source.id();
    if (source.has_valid_until()) result.valid_until_ = FromProto(source.valid_until());
    return result;
}

GrpcRateLimitResponse::Quota ToProto(const RateLimitResponse::Quota& source) {
    GrpcRateLimitResponse::Quota result;
    result.set_requests(source.requests_);
    result.set_id(source.id_);
    if (source.valid_until_) *result.mutable_valid_until() = ToProto(*source.valid_until_);
    return result;
}

RateLimitResponse::HeaderValue FromProto(const envoy::config::core::v3::HeaderValue& source) {
    return {source.key(), source.value(), source.raw_value()};
}

envoy::config::core::v3::HeaderValue ToProto(const RateLimitResponse::HeaderValue& source) {
    envoy::config::core::v3::HeaderValue result;
    result.set_key(source.key_);
    result.set_value(source.value_);
    result.set_raw_value(source.raw_value_);
    return result;
}

RateLimitResponse::DescriptorStatus FromProto(const GrpcRateLimitDescriptorStatus& source) {
    RateLimitResponse::DescriptorStatus result;
    result.code_ = static_cast<RateLimitResponse::Code>(source.code());
    result.limit_remaining_ = source.limit_remaining();
    if (source.has_current_limit()) result.current_limit_ = FromProto(source.current_limit());
    if (source.has_duration_until_reset()) result.duration_until_reset_ = FromProto(source.duration_until_reset());
    if (source.has_quota()) result.quota_ = FromProto(source.quota());
    return result;
}

GrpcRateLimitDescriptorStatus ToProto(const RateLimitResponse::DescriptorStatus& source) {
    GrpcRateLimitDescriptorStatus result;
    result.set_code(static_cast<GrpcRateLimitResponse::Code>(source.code_));
    result.set_limit_remaining(source.limit_remaining_);
    if (source.current_limit_) *result.mutable_current_limit() = ToProto(*source.current_limit_);
    if (source.duration_until_reset_) *result.mutable_duration_until_reset() = ToProto(*source.duration_until_reset_);
    if (source.quota_) *result.mutable_quota() = ToProto(*source.quota_);
    return result;
}

nlohmann::json FromProto(const google::protobuf::Struct& source) {
    auto result = nlohmann::json::object();
    for (const auto& field : source.fields()) result[field.first] = FromProto(field.second);
    return result;
}

nlohmann::json FromProto(const google::protobuf::ListValue& source) {
    auto result = nlohmann::json::array();
    for (const auto& value : source.values()) result.push_back(FromProto(value));
    return result;
}

nlohmann::json FromProto(const google::protobuf::Value& source) {
    switch (source.kind_case()) {
        case google::protobuf::Value::kNullValue:
        case google::protobuf::Value::KIND_NOT_SET:
            // JSON has no separate representation for an unset protobuf kind.
            return nullptr;
        case google::protobuf::Value::kNumberValue:
            if (!std::isfinite(source.number_value()))
                throw std::invalid_argument("metadata numbers must be finite");
            return source.number_value();
        case google::protobuf::Value::kStringValue: return source.string_value();
        case google::protobuf::Value::kBoolValue: return source.bool_value();
        case google::protobuf::Value::kStructValue: return FromProto(source.struct_value());
        case google::protobuf::Value::kListValue: return FromProto(source.list_value());
    }
    throw std::invalid_argument("unsupported protobuf metadata kind");
}

// JSON uses one C++ type for objects, lists, and scalars, so these functions
// have distinct names rather than overloads distinguished only by return type.
google::protobuf::Struct ToProtoStruct(const nlohmann::json& source) {
    if (!source.is_object()) throw std::invalid_argument("metadata must be a JSON object");
    google::protobuf::Struct result;
    for (const auto& field : source.items()) {
        (*result.mutable_fields())[field.key()] = ToProtoValue(field.value());
    }
    return result;
}

google::protobuf::ListValue ToProtoList(const nlohmann::json& source) {
    if (!source.is_array()) throw std::invalid_argument("metadata list must be a JSON array");
    google::protobuf::ListValue result;
    for (const auto& value : source) *result.add_values() = ToProtoValue(value);
    return result;
}

google::protobuf::Value ToProtoValue(const nlohmann::json& source) {
    google::protobuf::Value result;
    if (source.is_null()) result.set_null_value(google::protobuf::NULL_VALUE);
    else if (source.is_boolean()) result.set_bool_value(source.get<bool>());
    else if (source.is_number()) {
        // protobuf Value has only double, even when JSON stores an integer.
        const double number = source.get<double>();
        if (!std::isfinite(number)) throw std::invalid_argument("metadata numbers must be finite");
        result.set_number_value(number);
    } else if (source.is_string()) result.set_string_value(source.get<std::string>());
    else if (source.is_object()) *result.mutable_struct_value() = ToProtoStruct(source);
    else if (source.is_array()) *result.mutable_list_value() = ToProtoList(source);
    else throw std::invalid_argument("unsupported JSON metadata type");
    return result;
}

RateLimitResponse FromProto(const GrpcRateLimitResponse& source) {
    RateLimitResponse result;
    result.overall_code_ = static_cast<RateLimitResponse::Code>(source.overall_code());
    result.statuses_.reserve(source.statuses_size());
    for (const auto& status : source.statuses()) result.statuses_.push_back(FromProto(status));
    for (const auto& header : source.response_headers_to_add()) result.response_headers_to_add_.push_back(FromProto(header));
    for (const auto& header : source.request_headers_to_add()) result.request_headers_to_add_.push_back(FromProto(header));
    result.raw_body_ = source.raw_body();
    if (source.has_dynamic_metadata()) result.dynamic_metadata_ = FromProto(source.dynamic_metadata());
    if (source.has_quota()) result.quota_ = FromProto(source.quota());
    return result;
}

GrpcRateLimitResponse ToProto(const RateLimitResponse& source) {
    GrpcRateLimitResponse result;
    result.set_overall_code(static_cast<GrpcRateLimitResponse::Code>(source.overall_code_));
    for (const auto& status : source.statuses_) *result.add_statuses() = ToProto(status);
    for (const auto& header : source.response_headers_to_add_) *result.add_response_headers_to_add() = ToProto(header);
    for (const auto& header : source.request_headers_to_add_) *result.add_request_headers_to_add() = ToProto(header);
    result.set_raw_body(source.raw_body_);
    if (source.dynamic_metadata_) *result.mutable_dynamic_metadata() = ToProtoStruct(*source.dynamic_metadata_);
    if (source.quota_) *result.mutable_quota() = ToProto(*source.quota_);
    return result;
}


}  // namespace rlc::grpc_conversion

// Standalone exercise: compile this file separately from src/main.cc.
#ifdef RATELIMIT_CONVERSION_TEST
using rlc::RateLimitRequest;
using rlc::RateLimitDescriptor;
using rlc::RateLimitUnit;
void TestDescriptorConversions() {
    using rlc::grpc_conversion::FromProto;
    using rlc::grpc_conversion::ToProto;

    const auto empty = FromProto(rlc::GrpcRateLimitDescriptor{});
    assert(empty.entries().empty());
    assert(!empty.has_limit() && !empty.has_hits_addend());
    assert(!empty.is_negative_hits());
    assert(ToProto(empty).SerializeAsString().empty());

    // Test independently of request conversion, including every declared unit
    // and an unknown proto3 enum value that must survive conversion unchanged.
    for (int unit : {0, 1, 2, 3, 4, 5, 6, 99}) {
        RateLimitDescriptor native;
        native.entries_ = {{"message_type", "marketing"}, {"to_number", "12345"}};
        native.limit_ = RateLimitDescriptor::Override{5, static_cast<RateLimitUnit>(unit)};
        native.hits_addend_ = uint64_t{1} << 40;
        native.is_negative_hits_ = true;
        const auto proto = ToProto(native);
        assert(proto.entries_size() == 2);
        assert(proto.entries(0).key() == "message_type");
        assert(proto.entries(0).value() == "marketing");
        assert(proto.entries(1).key() == "to_number");
        assert(proto.entries(1).value() == "12345");
        assert(proto.has_limit() && proto.limit().requests_per_unit() == 5);
        assert(static_cast<int>(proto.limit().unit()) == unit);
        assert(proto.has_hits_addend() && proto.hits_addend().value() == (uint64_t{1} << 40));
        assert(proto.is_negative_hits());

        const auto restored = FromProto(proto);
        assert(restored.entries_size() == 2);
        assert(restored.entries(0).key() == "message_type");
        assert(restored.entries(0).value() == "marketing");
        assert(restored.entries(1).key() == "to_number");
        assert(restored.entries(1).value() == "12345");
        assert(restored.has_limit() && restored.limit().requests_per_unit() == 5);
        assert(static_cast<int>(restored.limit().unit()) == unit);
        assert(restored.has_hits_addend() && restored.hits_addend().value() == (uint64_t{1} << 40));
        assert(restored.is_negative_hits());
    }

    rlc::GrpcRateLimitDescriptor present_zero;
    present_zero.mutable_limit();
    present_zero.mutable_hits_addend()->set_value(0);
    const auto native_zero = FromProto(present_zero);
    assert(native_zero.has_limit() && native_zero.limit().requests_per_unit() == 0);
    assert(native_zero.has_hits_addend() && native_zero.hits_addend().value() == 0);
    const auto proto_zero = ToProto(native_zero);
    assert(proto_zero.has_limit() && proto_zero.has_hits_addend());
    assert(proto_zero.SerializeAsString() == present_zero.SerializeAsString());
    std::cout << "GrpcRateLimitDescriptor conversion tests passed\n";
}

int main() {
    TestDescriptorConversions();
    using rlc::grpc_conversion::FromProto;
    using rlc::GrpcRateLimitRequest;
    using rlc::grpc_conversion::ToProto;

    GrpcRateLimitRequest request;
    request.set_domain("messaging");
    request.set_hits_addend(3);

    auto* first = request.add_descriptors();
    auto* entry = first->add_entries();
    entry->set_key("message_type");
    entry->set_value("marketing");
    entry = first->add_entries();
    entry->set_key("to_number");
    entry->set_value("12345");
    first->mutable_limit()->set_requests_per_unit(5);
    first->mutable_limit()->set_unit(envoy::type::v3::DAY);
    first->mutable_hits_addend()->set_value(0);
    first->set_is_negative_hits(true);

    // A second descriptor deliberately leaves both optional fields absent.
    auto* second = request.add_descriptors();
    entry = second->add_entries();
    entry->set_key("user");
    entry->set_value("alice");

    const auto converted = FromProto(request);
    assert(converted.domain_ == "messaging");
    assert(converted.hits_addend_ == 3);
    assert(converted.descriptors_.size() == 2);
    const auto& a = converted.descriptors_[0];
    assert(a.entries_.size() == 2);
    assert(a.entries_[0].key_ == "message_type");
    assert(a.entries_[0].value_ == "marketing");
    assert(a.entries_[1].key_ == "to_number");
    assert(a.entries_[1].value_ == "12345");
    assert(a.limit_.has_value());
    assert(a.limit_->requests_per_unit_ == 5);
    assert(a.limit_->unit_ == RateLimitUnit::DAY);
    assert(a.hits_addend_.has_value() && *a.hits_addend_ == 0);
    assert(a.is_negative_hits_);
    const auto& b = converted.descriptors_[1];
    assert(b.entries_.size() == 1);
    assert(b.entries_[0].key_ == "user" && b.entries_[0].value_ == "alice");
    assert(!b.limit_.has_value());
    assert(!b.hits_addend_.has_value());
    assert(!b.is_negative_hits_);

    const auto empty = FromProto(GrpcRateLimitRequest{});
    assert(empty.domain_.empty());
    assert(empty.descriptors_.empty());
    assert(empty.hits_addend_ == 0);

    const auto round_trip = ToProto(converted);
    assert(round_trip.SerializeAsString() == request.SerializeAsString());
    assert(round_trip.descriptors(0).has_limit());
    assert(round_trip.descriptors(0).has_hits_addend());
    assert(round_trip.descriptors(0).hits_addend().value() == 0);
    assert(!round_trip.descriptors(1).has_limit());
    assert(!round_trip.descriptors(1).has_hits_addend());
    assert(ToProto(empty).SerializeAsString().empty());

    // Build a native request directly, including an explicitly present default
    // override and a count that would be truncated if converted through uint32.
    RateLimitRequest native;
    native.domain_ = "native";
    native.hits_addend_ = 2;
    RateLimitDescriptor descriptor;
    descriptor.entries_.push_back({"user", "bob"});
    descriptor.limit_ = RateLimitDescriptor::Override{};
    descriptor.hits_addend_ = uint64_t{1} << 40;
    native.descriptors_.push_back(descriptor);
    const auto proto = ToProto(native);
    assert(proto.domain() == "native" && proto.hits_addend() == 2);
    assert(proto.descriptors_size() == 1);
    const auto& output = proto.descriptors(0);
    assert(output.entries_size() == 1);
    assert(output.entries(0).key() == "user");
    assert(output.entries(0).value() == "bob");
    assert(output.has_limit());
    assert(output.limit().requests_per_unit() == 0);
    assert(output.limit().unit() == envoy::type::v3::UNKNOWN);
    assert(output.has_hits_addend());
    assert(output.hits_addend().value() == (uint64_t{1} << 40));
    assert(!output.is_negative_hits());

    assert(converted.domain() == "messaging");
    assert(converted.hits_addend() == 3);
    assert(converted.descriptors_size() == 2);
    assert(&converted.descriptors() == &converted.descriptors_);
    const auto& accessed = converted.descriptors(0);
    assert(accessed.entries_size() == 2);
    assert(accessed.entries(0).key() == "message_type");
    assert(accessed.entries(0).value() == "marketing");
    assert(accessed.has_limit() && accessed.limit().requests_per_unit() == 5);
    assert(accessed.limit().unit() == RateLimitUnit::DAY);
    assert(accessed.has_hits_addend() && accessed.hits_addend().value() == 0);
    assert(accessed.is_negative_hits());
    assert(!converted.descriptors(1).has_limit());
    assert(converted.descriptors(1).limit().requests_per_unit() == 0);
    assert(!converted.descriptors(1).has_hits_addend());

    std::cout << "FromProto and ToProto request tests passed\n";
    return 0;
}
#endif
