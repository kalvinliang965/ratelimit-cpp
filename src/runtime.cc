#include "runtime.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace rlc {
namespace fs = std::filesystem;
namespace {
std::string readFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("cannot read " + path);
  std::ostringstream out; 
  out << input.rdbuf(); 
  return out.str();
}

} // namespace

ConfigDocuments readConfigDocuments(const std::filesystem::path& path) {
  ConfigDocuments result;
  if (fs::is_regular_file(path)) return {{path.filename().string(),readFile(path.string())}};
  if (!fs::exists(path)) throw std::runtime_error("config directory does not exist: " + path.string());
  const bool ignore = envBool("RUNTIME_IGNOREDOTFILES");
  fs::recursive_directory_iterator it(path, fs::directory_options::follow_directory_symlink), end;
  for (; it != end; ++it) {
    if (ignore && it->path().filename().string().front() == '.') {
      if (it->is_directory()) it.disable_recursion_pending();
      continue;
    }
    if (it->is_regular_file()) {
      auto name = fs::relative(it->path(),path).string();
      std::replace(name.begin(),name.end(),'/','.');
      result.emplace_back(name,readFile(it->path().string()));
    }
  }
  std::sort(result.begin(),result.end());
  return result;
}

namespace {
// File updates replace one immutable snapshot. Invalid updates retain the last
// good configuration, so readers never see partially parsed policies.
class FileProvider {
 public:
  FileProvider(Service& service, fs::path path) : service_(service),path_(std::move(path)) { reload(); }
  ~FileProvider() { stop_ = true; if (worker_.joinable()) worker_.join(); }
  void start() { worker_ = std::thread([this] {
    while (!stop_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      try { reload(); } catch (const std::exception& e) { std::cerr << "config: " << e.what() << '\n'; }
    }
  }); }
 private:
  void reload() {
    auto next = readConfigDocuments(path_);
    if (seen_ && next == previous_) return;
    previous_ = next; seen_ = true;
    try {
      service_.setConfig(Config::load(next,envBool("MERGE_DOMAIN_CONFIG")));
      service_.recordConfigLoad(true);
    } catch (const std::exception& e) {
      service_.recordConfigLoad(false);
      std::cerr << "configuration rejected: " << e.what() << '\n';
    }
  }
  Service& service_; 
  fs::path path_; 
  ConfigDocuments previous_; 
  bool seen_ = false; 
  std::atomic<bool> stop_{false}; std::thread worker_;
};

} // namespace

struct RateLimitRuntime::Impl {
  explicit Impl(std::string path) : config_path(std::move(path)) {}

  Service service;
  std::string config_path;
  // Declared after Service so the provider is destroyed first.
  std::unique_ptr<FileProvider> files;
  std::atomic<bool> started{false};
};

RateLimitRuntime::RateLimitRuntime(std::string config_path)
    : impl_(std::make_unique<Impl>(std::move(config_path))) {}

RateLimitRuntime::~RateLimitRuntime() { stop(); }

void RateLimitRuntime::start() {
  if (impl_->started.load()) return;
  const auto type = env("CONFIG_TYPE", "FILE");
  if (type == "FILE") {
    auto files = std::make_unique<FileProvider>(impl_->service, impl_->config_path);
    files->start();
    impl_->files = std::move(files);
  } else if (type == "GRPC_XDS_SOTW") {
    // Preserve the existing placeholder: no xDS provider is started yet.
  } else {
    throw std::runtime_error("invalid CONFIG_TYPE: " + type);
  }
  impl_->started = true;
}

void RateLimitRuntime::stop() {
  impl_->started = false;
  impl_->files.reset(); // Stops and joins the config-reload thread.
}

Service& RateLimitRuntime::service() { return impl_->service; }

bool RateLimitRuntime::healthy() const {
  if (!impl_->started.load()) return false;
  const auto config = impl_->service.getConfig();
  return impl_->service.backendHealthy() &&
      (!envBool("HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED") ||
       (config && !config->empty()));
}
} // namespace rlc
