#include "xds.h"

#include "api/ratelimit/config/ratelimit/v3/rls_conf.pb.h"
#include "envoy/service/discovery/v3/ads.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <google/protobuf/util/json_util.h>

#include <chrono>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace rlc {
namespace {
using DiscoveryRequest = envoy::service::discovery::v3::DiscoveryRequest;
using DiscoveryResponse = envoy::service::discovery::v3::DiscoveryResponse;
using Ads = envoy::service::discovery::v3::AggregatedDiscoveryService;
using XdsConfig = ratelimit::config::ratelimit::v3::RateLimitConfig;
using XdsDescriptor = ratelimit::config::ratelimit::v3::RateLimitDescriptor;
using XdsPolicy = ratelimit::config::ratelimit::v3::RateLimitPolicy;
constexpr char kTypeUrl[] = "type.googleapis.com/ratelimit.config.ratelimit.v3.RateLimitConfig";

std::string quote(const std::string& value) {
  std::string out{"\""};
  for (const char c : value) {
    switch (c) {
      case '\\': out += "\\\\"; break;
      case '\"': out += "\\\""; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default: out += c;
    }
  }
  return out + '"';
}
void indent(std::string& out, int level) { out.append(static_cast<size_t>(level) * 2, ' '); }
void appendPolicy(std::string& out, const XdsPolicy& policy, int level) {
  indent(out, level); out += "rate_limit:\n";
  indent(out, level + 1); out += "unit: " + ratelimit::config::ratelimit::v3::RateLimitUnit_Name(policy.unit()) + "\n";
  indent(out, level + 1); out += "requests_per_unit: " + std::to_string(policy.requests_per_unit()) + "\n";
  if (policy.unlimited()) { indent(out, level + 1); out += "unlimited: true\n"; }
  if (!policy.name().empty()) { indent(out, level + 1); out += "name: " + quote(policy.name()) + "\n"; }
  if (policy.replaces_size()) {
    indent(out, level + 1); out += "replaces:\n";
    for (const auto& replacement : policy.replaces()) { indent(out, level + 2); out += "- name: " + quote(replacement.name()) + "\n"; }
  }
}
void appendDescriptor(std::string& out, const XdsDescriptor& descriptor, int level) {
  indent(out, level); out += "- key: " + quote(descriptor.key()) + "\n";
  if (!descriptor.value().empty()) { indent(out, level + 1); out += "value: " + quote(descriptor.value()) + "\n"; }
  if (descriptor.has_rate_limit()) appendPolicy(out, descriptor.rate_limit(), level + 1);
  if (descriptor.shadow_mode()) { indent(out, level + 1); out += "shadow_mode: true\n"; }
  if (descriptor.quota_mode()) { indent(out, level + 1); out += "quota_mode: true\n"; }
  if (descriptor.detailed_metric()) { indent(out, level + 1); out += "detailed_metric: true\n"; }
  if (descriptor.has_metadata()) {
    std::string json;
    google::protobuf::util::MessageToJsonString(descriptor.metadata(), &json);
    indent(out, level + 1); out += "metadata: " + json + "\n";
  }
  if (descriptor.descriptors_size()) {
    indent(out, level + 1); out += "descriptors:\n";
    for (const auto& child : descriptor.descriptors()) appendDescriptor(out, child, level + 1);
  }
}
std::pair<std::string, std::string> yamlFor(const XdsConfig& config) {
  std::string yaml = "domain: " + quote(config.domain()) + "\ndescriptors:\n";
  for (const auto& descriptor : config.descriptors()) appendDescriptor(yaml, descriptor, 0);
  return {config.name(), std::move(yaml)};
}
std::shared_ptr<grpc::ChannelCredentials> xdsCredentials() {
  if (!envBool("CONFIG_GRPC_XDS_SERVER_USE_TLS")) return grpc::InsecureChannelCredentials();
  grpc::SslCredentialsOptions options;
  const auto ca = env("CONFIG_GRPC_XDS_SERVER_TLS_CACERT");
  if (!ca.empty()) { std::ifstream input(ca); options.pem_root_certs.assign(std::istreambuf_iterator<char>(input), {}); }
  const auto cert = env("CONFIG_GRPC_XDS_CLIENT_TLS_CERT"), key = env("CONFIG_GRPC_XDS_CLIENT_TLS_KEY");
  if (!cert.empty() || !key.empty()) {
    std::ifstream certificate(cert), private_key(key);
    options.pem_cert_chain.assign(std::istreambuf_iterator<char>(certificate), {});
    options.pem_private_key.assign(std::istreambuf_iterator<char>(private_key), {});
  }
  return grpc::SslCredentials(options);
}
}

struct XdsProvider::Impl {
  explicit Impl(Service& target) : service(target) {}
  Service& service;
  std::atomic<bool> stopping{false};
  std::thread worker;
  std::mutex context_mutex;
  grpc::ClientContext* active_context = nullptr;
  void run() {
    const auto endpoint = env("CONFIG_GRPC_XDS_SERVER_URL", "localhost:18000");
    while (!stopping) {
      grpc::ChannelArguments args;
      if (const auto bytes = envInt("XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES", 0); bytes > 0) args.SetMaxReceiveMessageSize(static_cast<int>(bytes));
      const auto san = env("CONFIG_GRPC_XDS_SERVER_TLS_SAN");
      if (!san.empty()) args.SetString(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, san);
      auto channel = grpc::CreateCustomChannel(endpoint, xdsCredentials(), args);
      auto stub = Ads::NewStub(channel);
      grpc::ClientContext context;
      {
        std::lock_guard<std::mutex> lock(context_mutex);
        if (stopping) break;
        active_context = &context;
      }
      std::istringstream headers(env("CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS"));
      for (std::string header; std::getline(headers, header, ',');) {
        const auto separator = header.find('=');
        if (separator != std::string::npos) context.AddMetadata(header.substr(0, separator), header.substr(separator + 1));
      }
      auto stream = stub->StreamAggregatedResources(&context);
      DiscoveryRequest request;
      request.set_type_url(kTypeUrl);
      request.mutable_node()->set_id(env("CONFIG_GRPC_XDS_NODE_ID", "default"));
      if (!stream->Write(request)) {
        stream->WritesDone(); stream->Finish();
        { std::lock_guard<std::mutex> lock(context_mutex); active_context = nullptr; }
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue;
      }
      DiscoveryResponse response;
      while (!stopping && stream->Read(&response)) {
        std::vector<std::pair<std::string, std::string>> documents;
        std::string error;
        for (const auto& resource : response.resources()) {
          XdsConfig config;
          if (!resource.UnpackTo(&config)) { error = "cannot unpack xDS rate limit resource: " + resource.type_url(); break; }
          documents.push_back(yamlFor(config));
        }
        DiscoveryRequest acknowledgement;
        acknowledgement.set_type_url(response.type_url());
        acknowledgement.set_version_info(response.version_info());
        acknowledgement.set_response_nonce(response.nonce());
        acknowledgement.mutable_node()->set_id(env("CONFIG_GRPC_XDS_NODE_ID", "default"));
        try {
          if (!error.empty()) throw std::runtime_error(error);
          service.setConfig(Config::load(documents, envBool("MERGE_DOMAIN_CONFIG")));
          service.recordConfigLoad(true);
        } catch (const std::exception& e) {
          service.recordConfigLoad(false);
          acknowledgement.mutable_error_detail()->set_message(e.what());
          std::cerr << "xDS config rejected: " << e.what() << '\n';
        }
        if (!stream->Write(acknowledgement)) break;
      }
      context.TryCancel(); stream->WritesDone(); stream->Finish();
      { std::lock_guard<std::mutex> lock(context_mutex); active_context = nullptr; }
      if (!stopping) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
};
XdsProvider::XdsProvider(Service& service) : impl_(std::make_unique<Impl>(service)) {}
XdsProvider::~XdsProvider() { stop(); }
void XdsProvider::start() { impl_->worker = std::thread([this] { impl_->run(); }); }
void XdsProvider::stop() {
  impl_->stopping = true;
  { std::lock_guard<std::mutex> lock(impl_->context_mutex); if (impl_->active_context) impl_->active_context->TryCancel(); }
  if (impl_->worker.joinable()) impl_->worker.join();
}
}
