#include "types.h"
#include "grpc_conversion.h"

#include <cassert>
#include <iostream>
#include <utility>

namespace rlc::grpc_conversion {


Status FromProto(const GrpcStatus& status) {
	return Status(static_cast<StatusCode>(status.error_code()), status.error_message());
}

GrpcStatus ToProto(const Status& status) { 
	return GrpcStatus(
		static_cast<grpc::StatusCode>(status.code()),
		status.message()
	    );
}

RateLimitDescriptor FromProto(const common::RateLimitDescriptor& source) {
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

common::RateLimitDescriptor ToProto(const RateLimitDescriptor& source) {
    common::RateLimitDescriptor result;
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

Request FromProto(const GrpcRequest& request) {
    Request result;
    result.domain_ = request.domain();
    result.hits_addend_ = request.hits_addend();
    result.descriptors_.reserve(request.descriptors_size());
    for (const auto& descriptor : request.descriptors()) {
        result.descriptors_.push_back(FromProto(descriptor));
    }
    return result;
}

GrpcRequest ToProto(const Request& request) {
    GrpcRequest result;
    result.set_domain(request.domain());
    result.set_hits_addend(request.hits_addend());
    for (const auto& descriptor : request.descriptors()) {
        *result.add_descriptors() = ToProto(descriptor);
    }
    return result;
}

// TODO: Implement response conversions once the shared response type is defined.

}  // namespace rlc::grpc_conversion

// Standalone exercise: compile this file separately from src/main.cc.
#ifdef RATELIMIT_CONVERSION_TEST
void TestDescriptorConversions() {
    using rlc::grpc_conversion::FromProto;
    using rlc::grpc_conversion::ToProto;

    const auto empty = FromProto(rlc::common::RateLimitDescriptor{});
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

    rlc::common::RateLimitDescriptor present_zero;
    present_zero.mutable_limit();
    present_zero.mutable_hits_addend()->set_value(0);
    const auto native_zero = FromProto(present_zero);
    assert(native_zero.has_limit() && native_zero.limit().requests_per_unit() == 0);
    assert(native_zero.has_hits_addend() && native_zero.hits_addend().value() == 0);
    const auto proto_zero = ToProto(native_zero);
    assert(proto_zero.has_limit() && proto_zero.has_hits_addend());
    assert(proto_zero.SerializeAsString() == present_zero.SerializeAsString());
    std::cout << "Descriptor conversion tests passed\n";
}

int main() {
    TestDescriptorConversions();
    using rlc::grpc_conversion::FromProto;
    using rlc::GrpcRequest;
    using rlc::grpc_conversion::ToProto;

    GrpcRequest request;
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

    const auto empty = FromProto(GrpcRequest{});
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
