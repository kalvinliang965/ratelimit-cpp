#include "resp.h"
#include <cstdint>
#include <stdexcept>

namespace rlc::backend {

Reply readReply(Stream& stream, unsigned depth) {
  if (depth > 64) throw std::runtime_error("Redis protocol nesting limit");
  auto line = stream.line();
  if (line.empty()) throw std::runtime_error("empty Redis reply");
  Reply r{line[0], line.substr(1), {}};
  if (r.type == '$') {
    const int64_t size = std::stoll(r.text);
    if (size == -1) { r.type = '_'; r.text.clear(); }
    else {
      if (size < 0 || size > 512*1024*1024) throw std::runtime_error("invalid Redis bulk size");
      r.text = stream.bytes(size);
      if (stream.bytes(2) != "\r\n") throw std::runtime_error("invalid Redis bulk terminator");
    }
  } else if (r.type == '*') {
    const int64_t count = std::stoll(r.text);
    if (count < -1 || count > 1024*1024) throw std::runtime_error("invalid Redis array size");
    for (int64_t i = 0; i < count; ++i) r.array.push_back(readReply(stream, depth+1));
  } else if (r.type != '+' && r.type != '-' && r.type != ':') throw std::runtime_error("invalid Redis reply type");
  return r;
}

std::string encode(const std::vector<std::string>& args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& arg : args) out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
  return out;
}

Reply command(Stream& stream, const std::vector<std::string>& args) {
  stream.write(encode(args));
  auto reply = readReply(stream);
  if (reply.type == '-') throw std::runtime_error(reply.text);
  return reply;
}

void authenticate(Stream& stream, const std::string& auth) {
  if (auth.empty()) return;
  const auto colon = auth.find(':');
  if (colon == std::string::npos) command(stream, {"AUTH", auth});
  else command(stream, {"AUTH", auth.substr(0, colon), auth.substr(colon+1)});
}

}  // namespace rlc::backend
