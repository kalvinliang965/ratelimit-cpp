#include "types.h"
#include "common.h"

#include <cassert>
#include <iostream>
#include <utility>

namespace rlc::grpc_conversion {

using GrpcRequest = pb::RateLimitRequest;

RateLimitRequest FromProto(const GrpcRequest& request) {
    RateLimitRequest result;
    result.domain = request.domain();
    result.hits_addend = request.hits_addend();
    result.descriptors.reserve(request.descriptors_size());

    for (const auto& grpc_descriptor : request.descriptors()) {
        RateLimitDescriptor descriptor;
        descriptor.entries.reserve(grpc_descriptor.entries_size());
        for (const auto& grpc_entry : grpc_descriptor.entries()) {
            descriptor.entries.push_back({grpc_entry.key(), grpc_entry.value()});
        }

        if (grpc_descriptor.has_limit()) {
            const auto& grpc_limit = grpc_descriptor.limit();
            descriptor.limit = RateLimitDescriptor::Override{
                grpc_limit.requests_per_unit(),
                static_cast<RateLimitUnit>(grpc_limit.unit())
            };
        }
        if (grpc_descriptor.has_hits_addend()) {
            descriptor.hits_addend = grpc_descriptor.hits_addend().value();
        }
        descriptor.is_negative_hits = grpc_descriptor.is_negative_hits();
        result.descriptors.push_back(std::move(descriptor));
    }
    return result;
}

GrpcRequest ToProto(const RateLimitRequest& request) {
    GrpcRequest result;
    result.set_domain(request.domain);
    result.set_hits_addend(request.hits_addend);

    for (const auto& descriptor : request.descriptors) {
        auto* grpc_descriptor = result.add_descriptors();
        for (const auto& entry : descriptor.entries) {
            auto* grpc_entry = grpc_descriptor->add_entries();
            grpc_entry->set_key(entry.key);
            grpc_entry->set_value(entry.value);
        }
        if (descriptor.limit.has_value()) {
            auto* grpc_limit = grpc_descriptor->mutable_limit();
            grpc_limit->set_requests_per_unit(descriptor.limit->requests_per_unit);
            grpc_limit->set_unit(
                static_cast<envoy::type::v3::RateLimitUnit>(descriptor.limit->unit));
        }
        if (descriptor.hits_addend.has_value()) {
            grpc_descriptor->mutable_hits_addend()->set_value(*descriptor.hits_addend);
        }
        grpc_descriptor->set_is_negative_hits(descriptor.is_negative_hits);
    }
    return result;
}

// TODO: Implement response conversions once the shared response type is defined.

}  // namespace rlc::grpc_conversion

// Standalone exercise: compile this file separately from src/main.cc.
int main() {
    using rlc::grpc_conversion::FromProto;
    using rlc::grpc_conversion::GrpcRequest;
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
    assert(converted.domain == "messaging");
    assert(converted.hits_addend == 3);
    assert(converted.descriptors.size() == 2);
    const auto& a = converted.descriptors[0];
    assert(a.entries.size() == 2);
    assert(a.entries[0].key == "message_type");
    assert(a.entries[0].value == "marketing");
    assert(a.entries[1].key == "to_number");
    assert(a.entries[1].value == "12345");
    assert(a.limit.has_value());
    assert(a.limit->requests_per_unit == 5);
    assert(a.limit->unit == RateLimitUnit::DAY);
    assert(a.hits_addend.has_value() && *a.hits_addend == 0);
    assert(a.is_negative_hits);
    const auto& b = converted.descriptors[1];
    assert(b.entries.size() == 1);
    assert(b.entries[0].key == "user" && b.entries[0].value == "alice");
    assert(!b.limit.has_value());
    assert(!b.hits_addend.has_value());
    assert(!b.is_negative_hits);

    const auto empty = FromProto(GrpcRequest{});
    assert(empty.domain.empty());
    assert(empty.descriptors.empty());
    assert(empty.hits_addend == 0);

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
    native.domain = "native";
    native.hits_addend = 2;
    RateLimitDescriptor descriptor;
    descriptor.entries.push_back({"user", "bob"});
    descriptor.limit = RateLimitDescriptor::Override{};
    descriptor.hits_addend = uint64_t{1} << 40;
    native.descriptors.push_back(descriptor);
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

    std::cout << "FromProto and ToProto request tests passed\n";
    return 0;
}
