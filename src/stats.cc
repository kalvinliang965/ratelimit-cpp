#include "stats.h"

#include "backend.h"
#include "common.h"

#include <arpa/inet.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <map>
#include <mutex>
#include <netdb.h>
#include <poll.h>
#include <regex>
#include <sstream>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rlc {
namespace {

std::string trim(std::string value) {
  const auto first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return "";
  const auto last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1);
}

std::string tagValue(std::string value) {
  for (char& c : value) if (c == '.' || c == ':' || c == '|') c = '_';
  return value;
}

std::string tags() {
  std::vector<std::pair<std::string, std::string>> parsed;
  std::string input = env("EXTRA_TAGS");
  size_t begin = 0;
  while (begin <= input.size()) {
    const auto end = input.find(',', begin);
    const auto item = trim(input.substr(begin, end == std::string::npos ? end : end - begin));
    const auto separator = item.find(':');
    if (separator != std::string::npos) {
      const auto key = trim(item.substr(0, separator));
      const auto value = trim(item.substr(separator + 1));
      if (!key.empty() && !value.empty()) parsed.emplace_back(key, tagValue(value));
    }
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  std::sort(parsed.begin(), parsed.end());
  std::string output;
  for (const auto& [key, value] : parsed) output += ".__" + key + "=" + value;
  return output;
}

bool isUdp(const std::string& protocol) {
  return protocol == "udp" || protocol == "udp4" || protocol == "udp6";
}

int connectStatsd(const std::string& host, const std::string& port, const std::string& protocol) {
  addrinfo hints{}, *addresses = nullptr;
  hints.ai_family = protocol == "tcp4" || protocol == "udp4" ? AF_INET :
                    protocol == "tcp6" || protocol == "udp6" ? AF_INET6 : AF_UNSPEC;
  hints.ai_socktype = isUdp(protocol) ? SOCK_DGRAM : SOCK_STREAM;
  const int resolution = getaddrinfo(host.c_str(), port.c_str(), &hints, &addresses);
  if (resolution != 0) return -1;
  int result = -1;
  for (auto* address = addresses; address && result < 0; address = address->ai_next) {
    const int fd = ::socket(address->ai_family, address->ai_socktype | SOCK_CLOEXEC, address->ai_protocol);
    if (fd < 0) continue;
    if (isUdp(protocol)) {
      if (::connect(fd, address->ai_addr, address->ai_addrlen) == 0) result = fd;
      else ::close(fd);
      continue;
    }
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      ::close(fd);
      continue;
    }
    int connected = ::connect(fd, address->ai_addr, address->ai_addrlen);
    if (connected < 0 && errno == EINPROGRESS) {
      pollfd event{fd, POLLOUT, 0};
      connected = ::poll(&event, 1, 1000);
      if (connected > 0) {
        int error = 0;
        socklen_t error_length = sizeof(error);
        connected = getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_length) == 0 && error == 0 ? 0 : -1;
      }
    }
    if (connected == 0 && fcntl(fd, F_SETFL, flags) == 0) result = fd;
    else ::close(fd);
  }
  freeaddrinfo(addresses);
  return result;
}

bool writeAll(int fd, const std::string& data, bool udp) {
  if (udp) return ::send(fd, data.data(), data.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(data.size());
  size_t offset = 0;
  while (offset < data.size()) {
    const auto wrote = ::send(fd, data.data() + offset, data.size() - offset, MSG_NOSIGNAL);
    if (wrote < 0 && errno == EINTR) continue;
    if (wrote <= 0) return false;
    offset += static_cast<size_t>(wrote);
  }
  return true;
}

}  // namespace

std::string sanitizeStatsName(std::string value) {
  for (char& c : value) if (c == ':' || c == '|') c = '_';
  // This deliberately mirrors utils.SanitizeStatName in Go: any substring
  // shaped like an IPv4 address has its dots replaced, without validating the
  // numerical range of its octets.
  static const std::regex ipv4(R"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})");
  for (auto it = std::sregex_iterator(value.begin(), value.end(), ipv4);
       it != std::sregex_iterator(); ++it) {
    const auto position = static_cast<size_t>(it->position());
    const auto length = static_cast<size_t>(it->length());
    for (size_t index = position; index < position + length; ++index)
      if (value[index] == '.') value[index] = '_';
  }
  return value;
}

struct Stats::Impl {
  const bool enabled = !envBool("DISABLE_STATS") && envBool("USE_STATSD", true);
  const std::string host = env("STATSD_HOST", "localhost");
  const std::string port = std::to_string(envInt("STATSD_PORT", 8125));
  const std::string protocol = env("STATSD_PROTOCOL", "tcp");
  const std::string suffix = tags();
  const std::chrono::milliseconds interval = [] {
    const auto seconds = durationSeconds(env("STATS_FLUSH_INTERVAL", "10s"));
    if (seconds < 0) throw std::runtime_error("invalid duration: " + env("STATS_FLUSH_INTERVAL"));
    return std::chrono::milliseconds(static_cast<int64_t>(seconds * 1000));
  }();
  mutable std::mutex mutex;
  std::condition_variable changed;
  std::map<std::string, std::uint64_t> values;
  std::map<std::string, std::uint64_t> pending;
  bool stopping = false;
  int fd = -1;
  std::thread worker;

  Impl() {
    if (enabled) worker = std::thread([this] { run(); });
  }
  ~Impl() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      stopping = true;
      changed.notify_all();
    }
    if (worker.joinable()) worker.join();
    if (fd >= 0) ::close(fd);
  }

  bool flush(const std::map<std::string, std::uint64_t>& batch) {
    if (batch.empty()) return true;
    if (fd < 0) fd = connectStatsd(host, port, protocol);
    if (fd < 0) return false;
    std::string wire;
    for (const auto& [name, value] : batch) wire += name + ":" + std::to_string(value) + "|c\n";
    const bool ok = writeAll(fd, wire, isUdp(protocol));
    if (!ok) {
      ::close(fd);
      fd = -1;
    }
    return ok;
  }

  void run() {
    for (;;) {
      std::map<std::string, std::uint64_t> batch;
      {
        std::unique_lock<std::mutex> lock(mutex);
        // Wake promptly after a request, while retaining STATS_FLUSH_INTERVAL
        // as the maximum delay.  Go batches these counters, but StatsD counter
        // semantics make earlier emission equivalent and avoid losing data on
        // a short-lived process.
        const auto delay = std::max(interval, std::chrono::milliseconds(1));
        changed.wait_for(lock, delay, [this] { return stopping || !pending.empty(); });
        if (stopping && pending.empty()) return;
        batch.swap(pending);
      }
      if (!flush(batch)) {
        std::unique_lock<std::mutex> lock(mutex);
        for (const auto& [name, value] : batch) pending[name] += value;
        if (stopping) return;
        changed.wait_for(lock, std::chrono::milliseconds(200), [this] { return stopping; });
      }
    }
  }
};

Stats::Stats() : impl_(std::make_unique<Impl>()) {}
Stats::~Stats() = default;

void Stats::add(const std::string& name, std::uint64_t value) {
  if (!impl_->enabled || value == 0) return;
  const auto metric = name + impl_->suffix;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->values[metric] += value;
    impl_->pending[metric] += value;
  }
  impl_->changed.notify_one();
}

std::string Stats::dump() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::ostringstream output;
  output << "{\n";
  bool first = true;
  for (const auto& [name, value] : impl_->values) {
    if (!first) output << ",\n";
    first = false;
    output << "  " << std::quoted(name) << ": " << value;
  }
  output << "\n}\n";
  return output.str();
}

}  // namespace rlc
