#include "service.h"
#include "health_service.h"
#include "raw_grpc.h"
#include "tls_proxy.h"
#include "xds.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <google/protobuf/util/json_util.h>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
#include <functional>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;
using namespace rlc;

namespace {

volatile std::sig_atomic_t quit = 0;
void signalHandler(int) {
	quit = 1; 
}

std::string readFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("cannot read " + path);
  std::ostringstream out; 
  out << input.rdbuf(); 
  return out.str();
}

std::string address(const std::string& host, int64_t port) {
  return (host.find(':') != std::string::npos ? "[" + host + "]" : host) + ":" + std::to_string(port);
}

// The TLS client-SAN compatibility path decrypts gRPC before forwarding it to
// the normal gRPC server. Keep that hop off TCP entirely: a new 0700 directory
// prevents other local users from traversing to the socket, and the socket is
// tightened to 0600 as soon as gRPC has bound it. The object is declared
// before the server in main(), so exception unwinding only removes the socket
// after the server object has been destroyed; normal shutdown calls cleanup()
// explicitly after Server::Wait().
class PrivateUnixSocket {
 public:
  PrivateUnixSocket() {
    const auto pattern = (fs::temp_directory_path() / "ratelimit-cpp-tls-proxy-XXXXXX").string();
    std::vector<char> writable_pattern(pattern.begin(), pattern.end());
    writable_pattern.push_back('\0');
    char* created = ::mkdtemp(writable_pattern.data());
    if (!created) {
      throw std::runtime_error("cannot create private TLS proxy directory: " +
                               std::string(std::strerror(errno)));
    }
    directory_ = created;
    socket_ = directory_ / "grpc.sock";
    try {
      if (socket_.string().size() >= sizeof(sockaddr_un::sun_path)) {
        throw std::runtime_error("private TLS proxy socket pathname is too long");
      }
      verifyDirectory();
    } catch (...) {
      cleanup();
      throw;
    }
  }

  ~PrivateUnixSocket() { cleanup(); }
  PrivateUnixSocket(const PrivateUnixSocket&) = delete;
  PrivateUnixSocket& operator=(const PrivateUnixSocket&) = delete;

  std::string grpcAddress() const { return "unix:" + socket_.string(); }
  const fs::path& socketPath() const { return socket_; }

  // Call after BuildAndStart(), when gRPC has created its pathname socket and
  // before the TLS proxy begins accepting public connections.
  void lockDownSocket() {
    struct stat metadata {};
    if (::lstat(socket_.c_str(), &metadata) != 0) {
      throw std::runtime_error("cannot inspect private TLS proxy socket: " +
                               std::string(std::strerror(errno)));
    }
    if (!S_ISSOCK(metadata.st_mode) || metadata.st_uid != ::geteuid()) {
      throw std::runtime_error("private TLS proxy socket was not created by this process");
    }
    if (::chmod(socket_.c_str(), S_IRUSR | S_IWUSR) != 0) {
      throw std::runtime_error("cannot restrict private TLS proxy socket: " +
                               std::string(std::strerror(errno)));
    }
    if (::lstat(socket_.c_str(), &metadata) != 0 || !S_ISSOCK(metadata.st_mode) ||
        metadata.st_uid != ::geteuid() || (metadata.st_mode & 0777) != (S_IRUSR | S_IWUSR)) {
      throw std::runtime_error("private TLS proxy socket permissions could not be verified");
    }
  }

  void cleanup() noexcept {
    if (!socket_.empty()) ::unlink(socket_.c_str());
    if (!directory_.empty()) ::rmdir(directory_.c_str());
    socket_.clear();
    directory_.clear();
  }

 private:
  void verifyDirectory() {
    if (::chmod(directory_.c_str(), S_IRWXU) != 0) {
      throw std::runtime_error("cannot restrict private TLS proxy directory: " +
                               std::string(std::strerror(errno)));
    }
    struct stat metadata {};
    if (::lstat(directory_.c_str(), &metadata) != 0 || !S_ISDIR(metadata.st_mode) ||
        metadata.st_uid != ::geteuid() || (metadata.st_mode & 0777) != S_IRWXU) {
      throw std::runtime_error("private TLS proxy directory permissions could not be verified");
    }
  }

  fs::path directory_;
  fs::path socket_;
};

int64_t durationMillis(std::string value) {
  // Go-style composite durations used for gRPC connection aging.
  double result = 0; size_t pos = 0;
  while (pos < value.size()) {
    size_t used = 0; double n = std::stod(value.substr(pos), &used); pos += used;
    if (value.compare(pos,2,"ms") == 0) { result += n; pos += 2; }
    else if (value.compare(pos,2,"us") == 0) { result += n/1000; pos += 2; }
    else if (value.compare(pos,2,"ns") == 0) { result += n/1000000; pos += 2; }
    else if (value.compare(pos,1,"s") == 0) { result += n*1000; ++pos; }
    else if (value.compare(pos,1,"m") == 0) { result += n*60000; ++pos; }
    else if (value.compare(pos,1,"h") == 0) { result += n*3600000; ++pos; }
    else if (n != 0 || pos != value.size()) throw std::runtime_error("invalid duration: " + value);
  }
  return static_cast<int64_t>(result);
}
using Documents = std::vector<std::pair<std::string, std::string>>;
Documents documents(const fs::path& path) {
  Documents result;
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
    auto next = documents(path_);
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
  Documents previous_; 
  bool seen_ = false; 
  std::atomic<bool> stop_{false}; std::thread worker_;
};

class HttpServer {
 public:
  using Handler = std::function<void(const http::request<http::string_body>&,http::response<http::string_body>&)>;
  HttpServer(std::string host, uint16_t port, Handler handler) : acceptor_(io_), handler_(std::move(handler)) {
    tcp::resolver resolver(io_);
    auto endpoints = resolver.resolve(host,std::to_string(port));
    const auto endpoint = endpoints.begin()->endpoint();
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint); acceptor_.listen();
    worker_ = std::thread([this] { run(); });
  }
  ~HttpServer() { stop_=true; if(worker_.joinable()) worker_.join(); }
 private:
  void run() {
    while (!stop_) {
      pollfd pfd{acceptor_.native_handle(),POLLIN,0};
      if (::poll(&pfd,1,100) <= 0) continue;
      boost::system::error_code ec;
      tcp::socket socket(io_); acceptor_.accept(socket,ec); if(ec) continue;
      timeval timeout{1,0};
      setsockopt(socket.native_handle(),SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
      setsockopt(socket.native_handle(),SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));
      boost::beast::flat_buffer buffer;
      http::request_parser<http::string_body> parser; parser.body_limit(4*1024*1024);
      http::read(socket,buffer,parser,ec);
      if(ec) continue;
      auto req=parser.release();
      http::response<http::string_body> res{http::status::ok,req.version()};
      res.set(http::field::content_type,"text/plain; charset=utf-8");
      res.keep_alive(false);
      try { handler_(req,res); }
      catch(const std::exception& e) { res.result(http::status::internal_server_error); res.body()=e.what(); }
      res.prepare_payload(); http::write(socket,res,ec);
      socket.shutdown(tcp::socket::shutdown_both,ec);
    }
  }
  boost::asio::io_context io_; tcp::acceptor acceptor_; Handler handler_;
  std::atomic<bool> stop_{false}; std::thread worker_;
};

std::shared_ptr<grpc::ServerCredentials> credentials() {
  if (!envBool("GRPC_SERVER_USE_TLS")) return grpc::InsecureServerCredentials();
  grpc::SslServerCredentialsOptions options;
  options.pem_key_cert_pairs.push_back({readFile(env("GRPC_SERVER_TLS_KEY")),readFile(env("GRPC_SERVER_TLS_CERT"))});
  if (!env("GRPC_CLIENT_TLS_CACERT").empty()) {
    options.pem_root_certs = readFile(env("GRPC_CLIENT_TLS_CACERT"));
    options.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
  }
  return grpc::SslServerCredentials(options);
}
void usage() {
  std::cout << "Standalone C++ Envoy rate limit service\n"
    "  ratelimit-cpp [--config FILE_OR_DIRECTORY] [--check-config]\n"
    "  ratelimit-cpp --grpc-client HOST:PORT --request JSON\n"
    "Configuration: same RUNTIME_ROOT/RUNTIME_SUBDIRECTORY/RUNTIME_APPDIRECTORY,\n"
    "REDIS_*, GRPC_*, LIMIT_*, SHADOW_MODE, QUOTA_MODE and CONFIG_TYPE settings.\n"
    "Default listeners: gRPC :8081, HTTP :8080, debug :6070.\n";
}

}

void init_signal() {
    std::signal(SIGPIPE,SIG_IGN);
    std::signal(SIGTERM,signalHandler); 
    std::signal(SIGINT,signalHandler); 
    std::signal(SIGHUP,signalHandler);
}

enum class Command {
	HELP,
	VERSION,
	CHECKCONFIG,
	GRPC_CLIENT, // testing purposes
	SERVER // default. spawn ratelmit server
};

struct Options {
	Command command = Command::SERVER;
	std::string config_path;
	std::string client;
	std::string request;
};

Options parseArgs(std::vector<std::string> args) {
    Options opts;
    for(int i = 1; i < args.size(); ++i) {
      auto arg = args[i];
      auto value = [&]() -> std::string {
	if(++i>=args.size()) throw std::runtime_error("missing value for "+arg); 
	return args[i]; 
      };
      auto check = [&]() {
	if (opts.command != Command::SERVER) {
	    throw std::runtime_error(
                "Only one command may be selected: --help, --version, "
                "--check-config, or --grpc-client.");
	}
      };
      if(arg=="--help" || arg=="-h") {
	check();
	opts.command = Command::HELP;
      } else if (arg=="--version") {
	check();
	opts.command = Command::VERSION;
      } else if(arg=="--check-config") {
	check();
	opts.command = Command::CHECKCONFIG;
      } else if(arg=="--grpc-client") {
	check();
	opts.command = Command::GRPC_CLIENT;
        opts.client=value();
      } else if(arg=="--request") {
	opts.request=value();
      } else if(arg=="--config" || arg=="-config_dir") {
	opts.config_path = value();
      } else {
	throw std::runtime_error("unknown argument: "+arg);
      }
    }
    return opts;
}

void resolveOptions(Options& opts) {
    if(opts.config_path.empty()) {
	opts.config_path=(fs::path(env("RUNTIME_ROOT","/srv/runtime_data/current"))/env("RUNTIME_SUBDIRECTORY")/env("RUNTIME_APPDIRECTORY","config")).string();
    }
    if (opts.client.empty() && !opts.request.empty()) {
	throw std::runtime_error(
		"--request requires a client command (--grpc-client)."
	);
    }
    if (!opts.client.empty() && opts.request.empty()) {
	throw std::runtime_error(
		"--grpc-client requires --request JSON."
	);
    }
}


void run_grpc_client(Options& opts) {
	assert(!opts.client.empty());
	Request req; Response res;
	auto parsed=google::protobuf::util::JsonStringToMessage(opts.request,&req);
	if(!parsed.ok()) throw std::runtime_error(parsed.ToString());
	auto stub=pb::RateLimitService::NewStub(grpc::CreateChannel(opts.client,grpc::InsecureChannelCredentials()));
	grpc::ClientContext context;
	context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(10));
	auto status=stub->ShouldRateLimit(&context,req,&res);
	if(!status.ok()) throw std::runtime_error(status.error_message());
	std::string json; google::protobuf::util::MessageToJsonString(res,&json);
	std::cout<<json<<'\n';
	return;
}

void run_grpc_server(
	Service& service,
	Options& opts,
	std::function<bool()> healthy
) {
	const auto grpc_uds=env("GRPC_UDS"); // client socket
			
	const auto client_ca=env("GRPC_CLIENT_TLS_CACERT"); 
	const auto require_client_san=!env("GRPC_CLIENT_TLS_SAN").empty();
	const auto tls_san_proxy=envBool("GRPC_SERVER_USE_TLS") && require_client_san && !client_ca.empty();

	std::unique_ptr<PrivateUnixSocket> tls_proxy_upstream;
	if (tls_san_proxy) tls_proxy_upstream=std::make_unique<PrivateUnixSocket>();

	const std::string grpc_host = env("GRPC_HOST", "0.0.0.0");
	const uint64_t grpc_port = envInt("GRPC_PORT", 8081);
	const auto grpc_client_address = address(grpc_host, grpc_port);
	const auto listen = grpc_uds.empty() ? grpc_client_address : "unix:" + grpc_uds;

	grpc::ServerBuilder builder;
 	builder.AddChannelArgument(GRPC_ARG_ENABLE_PER_MESSAGE_COMPRESSION,0);
	builder.AddChannelArgument(GRPC_ARG_ENABLE_PER_MESSAGE_DECOMPRESSION,1);
	builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_DEFLATE,false);
	builder.SetCompressionAlgorithmSupportStatus(GRPC_COMPRESS_GZIP,true);
	builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_AGE_MS,static_cast<int>(durationMillis(env("GRPC_MAX_CONNECTION_AGE","24h"))));
	builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_AGE_GRACE_MS,static_cast<int>(durationMillis(env("GRPC_MAX_CONNECTION_AGE_GRACE","1h"))));
	builder.SetMaxReceiveMessageSize(4*1024*1024);
	RawGrpcService grpc_service(service);
	HealthService health_service;
	health_service.setServing(healthy());
	auto completion_queue=builder.AddCompletionQueue();
	if(tls_san_proxy)  {
		builder.AddListeningPort(tls_proxy_upstream->grpcAddress(),grpc::InsecureServerCredentials());
	} else {
		builder.AddListeningPort(listen,credentials());
	}
	builder.RegisterService(&grpc_service); builder.RegisterService(&health_service);

	auto server = builder.BuildAndStart();
	if(!server) {
		throw std::runtime_error("cannot start gRPC server at "+listen);
	}
	if(tls_san_proxy) {
		tls_proxy_upstream->lockDownSocket();
	}
	grpc_service.start(completion_queue.get());
	std::unique_ptr<TlsProxy> tls_proxy;
	if(tls_san_proxy) {
		TlsProxyConfig proxy_config;
		if(grpc_uds.empty()) {
			proxy_config.listen_host=env("GRPC_HOST","0.0.0.0");
			proxy_config.listen_port=static_cast<uint16_t>(envInt("GRPC_PORT",8081));
		} else {
			proxy_config.listen_unix_socket=grpc_uds;
		}
		proxy_config.upstream_unix_socket=tls_proxy_upstream->socketPath().string();
		proxy_config.server_certificate=env("GRPC_SERVER_TLS_CERT");
		proxy_config.server_private_key=env("GRPC_SERVER_TLS_KEY");
		proxy_config.client_ca_certificate=client_ca;
		proxy_config.required_client_san=env("GRPC_CLIENT_TLS_SAN");
		tls_proxy=std::make_unique<TlsProxy>(std::move(proxy_config));
		try { tls_proxy->start(); }
		catch(...) {
			server->Shutdown(std::chrono::system_clock::now()+std::chrono::seconds(5));
			completion_queue->Shutdown(); grpc_service.join(); server->Wait(); throw;
		}
	}
	while(!quit) { 
		health_service.setServing(healthy());
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
	if(tls_proxy) tls_proxy->stop();
	health_service.shutdown();
	server->Shutdown(std::chrono::system_clock::now()+std::chrono::seconds(5));
	completion_queue->Shutdown();
	grpc_service.join();
	server->Wait();
	if(tls_proxy_upstream) tls_proxy_upstream->cleanup();
}

void run_server(Options& opts) {
	Service service;
	std::unique_ptr<FileProvider> files;
	// std::unique_ptr<XdsProvider> xds;
	auto type = env("CONFIG_TYPE", "FILE");
	if (type == "FILE") {
		// FileProvider constantly poll if data has change and update
		// config inside service.
		files = std::make_unique<FileProvider>(service, opts.config_path);
		files->start();
	} else if (type == "GRPC_XDS_SOTW") {
		// TODO: receive configuration from remote xds server
		// xds=std::make_unique_ptr<XdsProvider>(service);
		// xds->start();
	} else {
		throw std::runtime_error("invalid CONFIG_TYPE: " + type);
	}
	// TODO: ???? 
	if (quit) return;

	auto healthy=[&]() {
		auto config = service.getConfig();
		return !quit && service.backendHealthy() &&
			(
				!envBool("HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED") || 
				(config && !config->empty())
			);
	};
	const std::string rpc_transport = env("RPC_TRANSPORT", "GRPC");
	if (rpc_transport == "GRPC") {
		run_grpc_server(service, opts, healthy);
	} else if (rpc_transport == "XRPC") {
		std::cout << "TODO" << std::endl;
		return;
	} else {
		throw std::runtime_error("invalid rpc_transport: " + rpc_transport);
	}
	// if(xds) xds->stop();
	return;
}

void dispatch(Options& opts) {
	switch(opts.command) {
	case Command::HELP: {
		usage();
		return;
	}
	case Command::VERSION: {
		std::cout<<"ratelimit-cpp 0.1.0\n";
		return;
	}
	case Command::CHECKCONFIG: {
	      	auto config = Config::load(documents(opts.config_path),envBool("MERGE_DOMAIN_CONFIG")); 
		std::cout<<config->dump()<<"all rate limit configs ok\n";
		return;
	} 
	case Command::GRPC_CLIENT: {
		run_grpc_client(opts);
		return;
	}
	case Command::SERVER:
	default:
		run_server(opts);
		return;
	}
}


void run(int argc, char* argv[]) {
	std::vector<std::string> args(argv, argv + argc);
	Options opts = parseArgs(std::move(args));
	resolveOptions(opts);
	dispatch(opts);
}

int main(int argc, char* argv[]) {
  try {
    init_signal();
    run(argc, argv);
    return 0;
  } catch(const std::exception& e) {
    std::cerr<<"ratelimit-cpp: "<<e.what()<<'\n';return 1;
  }
}







