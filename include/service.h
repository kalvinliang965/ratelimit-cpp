#pragma once
#include "config.h"
#include "envoy/service/ratelimit/v3/rls.grpc.pb.h"
#include <mutex>
#include "common.h"

namespace rlc {

class Service {
 public:
  Service();
  ~Service();
  void setConfig(std::shared_ptr<Config> config);
  std::shared_ptr<Config> getConfig() const;
  bool backendHealthy() const;
  void recordConfigLoad(bool success);
  std::string statsDump() const;
  Status ShouldRateLimit(grpc::ServerContext*, const Request*, Response*);
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}
