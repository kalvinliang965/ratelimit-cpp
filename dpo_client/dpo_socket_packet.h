#pragma once

#include <cstddef>

// Client-side sizes of the rate-limit DPO socket packet:
//   UDP datagram = [generated DPO header][xrpc_conversion payload]
//
// The header layout is owned by smartnic-generic-rpc: ratelimit.dpo is compiled
// into protocols/ratelimit/ratelimit_header.h, whose packed ratelimitHeader the
// socket runtime reads in place.  The DPO server static_asserts that header's
// size and field offsets (ratelimit_server.cc), and protocols/ratelimit/
// ratelimit_packet.h holds the server's copy of these sizes.  Update all of
// them together when ratelimit.dpo changes.
namespace dpo_socket {
inline constexpr std::size_t kHeaderSize = 129;
// The DPO UDP socket backend receives into 1,500-byte buffers.
inline constexpr std::size_t kMaxPacketBytes = 1500;
inline constexpr std::size_t kMaxPayloadBytes = kMaxPacketBytes - kHeaderSize;
}
