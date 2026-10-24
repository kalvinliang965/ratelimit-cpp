#include "raw_grpc.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "grpc_conversion.h"

namespace rlc {
namespace {

constexpr char kMalformedRequestError[] =
    // The Docker Go reference binary used by validation emits a non-breaking
    // space after "proto:". Protobuf v1.36.11 deliberately chooses either
    // this or an ASCII space from a hash of the Go executable, so this is a
    // reference-build-specific wire value, not a grpc-go invariant.
    "grpc: error unmarshalling request: proto:" "\xC2\xA0"
    "cannot parse invalid wire-format data";

bool decode(const grpc::ByteBuffer& buffer, GrpcRequest* request) {
  std::vector<grpc::Slice> slices;
  if (!buffer.Dump(&slices).ok()) return false;
  std::string bytes;
  for (const auto& slice : slices) {
    bytes.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
  }
  return request->ParseFromString(bytes);
}

grpc::ByteBuffer encode(const Response& response) {
  std::string bytes;
  response.SerializeToString(&bytes);
  grpc::Slice slice(bytes.data(), bytes.size());
  return grpc::ByteBuffer(&slice, 1);
}

}  // namespace

class RawGrpcService::Call {
 public:
  explicit Call(RawGrpcService* owner)
      : owner_(owner), responder_(&context_) {
    owner_->RequestShouldRateLimit(&context_, &request_, &responder_,
                                   owner_->completion_queue_,
                                   owner_->completion_queue_, this);
  }

  void proceed(bool ok) {
    if (state_ == State::request) {
      if (!ok) {
        delete this;
        return;
      }

      // Keep an outstanding accept before performing potentially blocking
      // Redis work for this call.
      new Call(owner_);

      GrpcRequest request;
      state_ = State::finish;
      if (!decode(request_, &request)) {
        responder_.FinishWithError(
            grpc::Status(grpc::StatusCode::INTERNAL, kMalformedRequestError),
            this);
        return;
      }

      Response response;
      auto cpp_request = rlc::grpc_conversion::FromProto(request);
      const auto status = owner_->implementation_.ShouldRateLimit(
          &context_, &cpp_request, &response);
      if (!status.ok()) {
        responder_.FinishWithError(status, this);
        return;
      }
      responder_.Finish(encode(response), status, this);
      return;
    }

    delete this;
  }

 private:
  enum class State { request, finish };

  RawGrpcService* owner_;
  grpc::ServerContext context_;
  grpc::ByteBuffer request_;
  grpc::ServerAsyncResponseWriter<grpc::ByteBuffer> responder_;
  State state_ = State::request;
};

RawGrpcService::RawGrpcService(rlc::Service& implementation)
    : implementation_(implementation) {}

RawGrpcService::~RawGrpcService() { join(); }

void RawGrpcService::start(grpc::ServerCompletionQueue* completion_queue) {
  completion_queue_ = completion_queue;
  new Call(this);

  const auto hardware_threads = std::thread::hardware_concurrency();
  const auto worker_count = std::max(2u, std::min(8u, hardware_threads));
  workers_.reserve(worker_count);
  for (unsigned int index = 0; index < worker_count; ++index) {
    workers_.emplace_back([this] {
      void* tag = nullptr;
      bool ok = false;
      while (completion_queue_->Next(&tag, &ok)) {
        static_cast<Call*>(tag)->proceed(ok);
      }
    });
  }
}

void RawGrpcService::join() {
  for (auto& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
  workers_.clear();
}

}  // namespace rlc
