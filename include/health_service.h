#pragma once

#include "grpc/health/v1/health.grpc.pb.h"

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace rlc {

// A local implementation is used instead of gRPC C++'s process-global default
// health service.  The latter is not safe to access through
// Server::GetHealthCheckService with the supported gRPC 1.30 runtime.
class HealthService final : public grpc::health::v1::Health::Service {
 public:
  explicit HealthService(std::string service_name = "ratelimit");

  void setServing(bool serving);
  void shutdown();

  grpc::Status Check(grpc::ServerContext* context,
                     const grpc::health::v1::HealthCheckRequest* request,
                     grpc::health::v1::HealthCheckResponse* response) override;
  grpc::Status Watch(
      grpc::ServerContext* context,
      const grpc::health::v1::HealthCheckRequest* request,
      grpc::ServerWriter<grpc::health::v1::HealthCheckResponse>* writer) override;
  grpc::Status List(grpc::ServerContext* context,
                    const grpc::health::v1::HealthListRequest* request,
                    grpc::health::v1::HealthListResponse* response) override;

 private:
  using ServingStatus = grpc::health::v1::HealthCheckResponse::ServingStatus;

  ServingStatus statusLocked(const std::string& service) const;
  void setStatusLocked(const std::string& service, ServingStatus status);

  const std::string service_name_;
  mutable std::mutex mutex_;
  std::condition_variable updated_;
  std::unordered_map<std::string, ServingStatus> statuses_;
  std::uint64_t generation_ = 0;
  bool stopped_ = false;
};

}  // namespace rlc
