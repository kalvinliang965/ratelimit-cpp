#pragma once

#include "service.h"
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace rlc {
using ConfigDocuments = std::vector<std::pair<std::string, std::string>>;
ConfigDocuments readConfigDocuments(const std::filesystem::path& path);

// Shared application lifetime for gRPC or DPO. Does not install signal
// handlers or run an RPC event loop. Stop the frontend before destroying it.
class RateLimitRuntime {
 public:
  explicit RateLimitRuntime(std::string config_path);
  ~RateLimitRuntime();
  RateLimitRuntime(const RateLimitRuntime&) = delete;
  RateLimitRuntime& operator=(const RateLimitRuntime&) = delete;

  // Call lifecycle methods from the owning application thread.
  void start();
  void stop();
  Service& service();
  bool healthy() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace rlc
