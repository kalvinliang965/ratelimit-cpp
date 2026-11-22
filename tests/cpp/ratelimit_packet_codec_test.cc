#include "ratelimit_packet_codec.h"
#include "dpo_socket_packet.h"
#include <cassert>
#include <stdexcept>

int main() {
    using namespace ratelimit_packet_codec;
    rlc::RateLimitRequest request;
    request.domain_ = std::string(16, 'd');
    rlc::RateLimitDescriptor descriptor;
    descriptor.entries_.emplace_back(std::string(16, 'k'), std::string("a\0b", 3));
    descriptor.limit_.emplace(0x12345678, rlc::RateLimitUnit::MINUTE);
    request.descriptors_.push_back(descriptor);
    auto packet = encode_request_packet(request, 0x1234, 0x5678, 1, 1500);
    assert(packet[14] == 0x34 && packet[15] == 0x12 && packet[22] == 0x78);
    assert(packet[32] == 1 && packet[33] == 16 && packet[50] == 16 && packet[67] == 3);
    assert(packet[68] == 'a' && packet[69] == 0 && packet[70] == 'b');
    assert(packet[85] == 0x78 && packet[88] == 0x12);
    auto fallback = request; fallback.domain_.push_back('x');
    assert(encode_request_packet(fallback, 0, 0, 1, 1500)[32] == 0);
    auto rejects = [](auto f) { bool failed = false; try { f(); } catch (const std::runtime_error&) { failed = true; } assert(failed); };
    rejects([&] { encode_request_packet(request, 0, 0, 1, packet.size() - 1); });
    auto reply = xrpc_conversion::Bytes(dpo_socket::kHeaderSize, 0);
    reply[0] = 1; reply[14] = 0x34; reply[15] = 0x12; reply[22] = 0x78; reply[23] = 0x56;
    reply[31] = 2; reply[89] = 1; reply[90] = 2; reply[91] = 1; reply[92] = 'n';
    reply[108] = 5; reply[112] = 2; reply[117] = 12;
    auto decoded = decode_response_packet(reply.data(), reply.size(), 0x1234, 0x5678, 1500);
    assert(decoded.statuses_[0].current_limit().name() == "n");
    rejects([&] { decode_response_packet(reply.data(), reply.size()-1, 0x1234, 0x5678, 1500); });
    rejects([&] { decode_response_packet(reply.data(), reply.size(), 0x1235, 0x5678, 1500); });
    rejects([&] { decode_response_packet(reply.data(), reply.size(), 0x1234, 0x5678, reply.size()-1); });
    reply[0] = 0; rejects([&] { decode_response_packet(reply.data(), reply.size(), 0x1234, 0x5678, 1500); }); reply[0] = 1;
    reply[89] = 2; rejects([&] { decode_response_packet(reply.data(), reply.size(), 0x1234, 0x5678, 1500); }); reply[89] = 1;
    reply[91] = 17; rejects([&] { decode_response_packet(reply.data(), reply.size(), 0x1234, 0x5678, 1500); }); reply[91] = 1;
    reply.push_back(0);
    auto decoded_with_payload = decode_response_packet(reply.data(), reply.size(), 0x1234, 0x5678, 1500);
    assert(decoded_with_payload.statuses_[0].current_limit().name() == "n");
}
