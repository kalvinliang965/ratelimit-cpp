#include "health_service.h"

#include <chrono>

namespace rlc {
namespace {
constexpr const char* kEnded = "Stream has ended.";
using HealthResponse = grpc::health::v1::HealthCheckResponse;
constexpr auto kUnknown = HealthResponse::UNKNOWN;
constexpr auto kServing = HealthResponse::SERVING;
constexpr auto kNotServing = HealthResponse::NOT_SERVING;
constexpr auto kServiceUnknown = HealthResponse::SERVICE_UNKNOWN;
}

HealthService::HealthService(std::string service_name)
    : service_name_(std::move(service_name)) {
  // grpc-go's health.NewServer() exposes the empty/default service as serving
  // before components set the named application's status.
  statuses_.emplace("", kServing);
}

HealthService::ServingStatus HealthService::statusLocked(const std::string& service) const {
  const auto found = statuses_.find(service);
  return found == statuses_.end() ? kServiceUnknown : found->second;
}

void HealthService::setStatusLocked(const std::string& service, ServingStatus status) {
  const auto found = statuses_.find(service);
  if (found != statuses_.end() && found->second == status) return;
  statuses_[service] = status;
  ++generation_;
  updated_.notify_all();
}

void HealthService::setServing(bool serving) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) return;
  setStatusLocked(service_name_, serving ? kServing : kNotServing);
}

void HealthService::shutdown() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) return;
  stopped_ = true;
  bool changed = false;
  for (auto& [name, status] : statuses_) {
    if (status != kNotServing) {
      status = kNotServing;
      changed = true;
    }
  }
  if (changed) ++generation_;
  updated_.notify_all();
}

grpc::Status HealthService::Check(
    grpc::ServerContext*, const grpc::health::v1::HealthCheckRequest* request,
    grpc::health::v1::HealthCheckResponse* response) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = statuses_.find(request->service());
  if (found == statuses_.end()) return {grpc::StatusCode::NOT_FOUND, "unknown service"};
  response->set_status(found->second);
  return grpc::Status::OK;
}

grpc::Status HealthService::Watch(
    grpc::ServerContext* context, const grpc::health::v1::HealthCheckRequest* request,
    grpc::ServerWriter<grpc::health::v1::HealthCheckResponse>* writer) {
  const std::string service = request->service();
  ServingStatus sent = kUnknown;
  bool have_sent = false;
  std::uint64_t observed = 0;
  for (;;) {
    ServingStatus status;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (have_sent) {
        // C++ synchronous ServerContext has no cancellation notification for a
        // condition variable.  A short timed wait lets canceled watches leave
        // promptly without keeping stale handlers alive until the next change.
        updated_.wait_for(lock, std::chrono::milliseconds(100), [&] {
          return generation_ != observed || context->IsCancelled();
        });
      }
      if (context->IsCancelled()) return {grpc::StatusCode::CANCELLED, kEnded};
      observed = generation_;
      status = statusLocked(service);
    }
    if (!have_sent || status != sent) {
      grpc::health::v1::HealthCheckResponse response;
      response.set_status(status);
      if (!writer->Write(response)) return {grpc::StatusCode::CANCELLED, kEnded};
      sent = status;
      have_sent = true;
    }
  }
}

grpc::Status HealthService::List(
    grpc::ServerContext*, const grpc::health::v1::HealthListRequest*,
    grpc::health::v1::HealthListResponse* response) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& [service, status] : statuses_) {
    (*response->mutable_statuses())[service].set_status(status);
  }
  return grpc::Status::OK;
}

}  // namespace rlc
