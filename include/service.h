#pragma once
#include "config.h"
#include "envoy/service/ratelimit/v3/rls.grpc.pb.h"
#include <mutex>
namespace rlc {

// struct RateLimitRequest {
// 	std::string domain;
// 	std::vector<std::pair<std::string, std::string>> descriptors;
// 	int descriptors_size;
// };

// class GRPCAdaptor final: public pb::RateLimitService::Service {
//  public:
//
//   Service();
//   ~Service();
//   void setConfig(std::shared_ptr<Config> config);
//   std::shared_ptr<Config> getConfig() const;
//   bool backendHealthy() const;
//   void recordConfigLoad(bool success);
//   std::string statsDump() const;
//   grpc::Status ShouldRateLimit(grpc::ServerContext*, const Request*, Response*) override;
//
//  private:
//   struct Impl;
//   std::unique_ptr<Impl> impl_;
// };
// };

class Service final : public pb::RateLimitService::Service {
 public:
  Service();
  ~Service();
  void setConfig(std::shared_ptr<Config> config);
  std::shared_ptr<Config> getConfig() const;
  bool backendHealthy() const;
  void recordConfigLoad(bool success);
  std::string statsDump() const;
  grpc::Status ShouldRateLimit(grpc::ServerContext*, const Request*, Response*) override;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}
