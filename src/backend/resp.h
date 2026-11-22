// RESP (Redis serialization protocol) over a Stream: command encoding and
// reply parsing.  Error replies are values (type '-'); command() throws them.
#pragma once
#include "stream.h"
#include <string>
#include <vector>

namespace rlc::backend {

// type is the RESP prefix ('+', '-', ':', '$', '*'), or '_' for a nil bulk string.
struct Reply {
  char type;
  std::string text;
  std::vector<Reply> array;
};

Reply readReply(Stream& stream, unsigned depth = 0);
std::string encode(const std::vector<std::string>& args);
Reply command(Stream& stream, const std::vector<std::string>& args);
// AUTH with "password" or "user:password"; nothing when auth is empty.
void authenticate(Stream& stream, const std::string& auth);

}  // namespace rlc::backend
