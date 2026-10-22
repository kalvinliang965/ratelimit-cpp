#pragma once

#include "service.h"

#include <grpcpp/grpcpp.h>

#include <thread>
#include <vector>

namespace rlc {

// The generated synchronous gRPC binding discards the protobuf parser error
// when a request is malformed.  The Go server exposes that error to clients,
// so accept the method as raw bytes and decode it explicitly before delegating
// to the shared rate-limit implementation.
class RawGrpcService final
    : public pb::RateLimitService::WithRawMethod_ShouldRateLimit<
          pb::RateLimitService::Service> {
 public:
  explicit RawGrpcService(Service& implementation);
  ~RawGrpcService() override;

  RawGrpcService(const RawGrpcService&) = delete;
  RawGrpcService& operator=(const RawGrpcService&) = delete;

  // Call after ServerBuilder::BuildAndStart. The caller shuts down the server
  // and completion queue before calling join().
  void start(grpc::ServerCompletionQueue* completion_queue);
  void join();

 private:
  class Call;

  Service& implementation_;
  grpc::ServerCompletionQueue* completion_queue_ = nullptr;
  std::vector<std::thread> workers_;
};

}  // namespace rlc
