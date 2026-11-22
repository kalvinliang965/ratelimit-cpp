// Sends one rate-limit request to a DPO UDP socket server and prints the reply.
// usage: ratelimit_client_socket [--host IPV4] [--port PORT] [--json] [request.json | -]
#include "xrpc_conversion.h"
#include "request_json.h"
#include "dpo_socket_packet.h"
#include "ratelimit_packet_codec.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <limits>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>

namespace wire = xrpc_conversion;

static const char* kUsage =
    "usage: ratelimit_client_socket [--host IPV4] [--port PORT] [--json] [request.json | -]";

static rlc::RateLimitRequest read_request(const char* path) {
    nlohmann::json document;
    if (std::string(path) == "-") {
        std::cin >> document;
    } else {
        std::ifstream input(path);
        if (!input) throw std::runtime_error(std::string("cannot open request file: ") + path);
        input >> document;
    }
    return rlc::MakeRequestFromJson(document);
}

static const char* code_name(rlc::RateLimitResponse::Code code) {
    switch (code) {
        case rlc::RateLimitResponse::Code::OK: return "OK";
        case rlc::RateLimitResponse::Code::OVER_LIMIT: return "OVER_LIMIT";
        default: return "UNKNOWN";
    }
}

static std::string escaped_bytes(const std::string& value) {
    std::string result = "\"";
    const char* hex = "0123456789abcdef";
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') { result += '\\'; result += c; }
        else if (c >= 32 && c < 127) result += c;
        else { result += "\\x"; result += hex[c >> 4]; result += hex[c & 15]; }
    }
    return result + '"';
}

static void print_headers(const char* name, const std::vector<rlc::RateLimitResponse::HeaderValue>& headers) {
    std::cout << "  " << name << ": count=" << headers.size() << '\n';
    for (const auto& header : headers) {
        std::cout << "    key=" << escaped_bytes(header.key_)
                  << ", value=" << escaped_bytes(header.value_)
                  << ", raw_value=" << escaped_bytes(header.raw_value_) << '\n';
    }
}

static void print_response_fields(const rlc::RateLimitResponse& response) {
    print_headers("response_headers_to_add", response.response_headers_to_add_);
    print_headers("request_headers_to_add", response.request_headers_to_add_);
    std::cout << "  raw_body: " << escaped_bytes(response.raw_body_) << '\n';
    if (response.dynamic_metadata_) {
        std::cout << "  dynamic_metadata: " << response.dynamic_metadata_->dump() << '\n';
    } else std::cout << "  dynamic_metadata: absent\n";
    if (response.quota_) {
        const auto& quota = *response.quota_;
        std::cout << "  response_quota: requests=" << quota.requests_
                  << ", id=" << escaped_bytes(quota.id_) << '\n';
        if (quota.valid_until_) {
            std::cout << "    valid_until: seconds=" << quota.valid_until_->seconds_
                      << ", nanos=" << quota.valid_until_->nanos_ << '\n';
        } else std::cout << "    valid_until: absent\n";
    } else std::cout << "  response_quota: absent\n";
}

static void print_statuses(const wire::Statuses& statuses) {
    for (size_t i = 0; i < statuses.size(); ++i) {
        const auto& status = statuses[i];
        std::cout << "  status[" << i << "]: code=" << code_name(status.code())
                  << ", limit_remaining=" << status.limit_remaining() << '\n';
        if (status.current_limit_) {
            const auto& limit = *status.current_limit_;
            std::cout << "    current_limit: name=" << std::quoted(limit.name())
                      << ", requests_per_unit=" << limit.requests_per_unit()
                      << ", unit=" << rlc::to_string(limit.unit()) << '\n';
        } else std::cout << "    current_limit: absent\n";
        if (status.duration_until_reset_) {
            std::cout << "    duration_until_reset: seconds=" << status.duration_until_reset_->seconds_
                      << ", nanos=" << status.duration_until_reset_->nanos_ << '\n';
        } else std::cout << "    duration_until_reset: absent\n";
        if (status.quota_) {
            const auto& quota = *status.quota_;
            std::cout << "    quota: requests=" << quota.requests_ << ", id=" << std::quoted(quota.id_) << '\n';
            if (quota.valid_until_) {
                std::cout << "      valid_until: seconds=" << quota.valid_until_->seconds_
                          << ", nanos=" << quota.valid_until_->nanos_ << '\n';
            } else std::cout << "      valid_until: absent\n";
        } else std::cout << "    quota: absent\n";
    }
}

// --json prints envoy.service.ratelimit.v3.RateLimitResponse in the protobuf
// JSON mapping with proto field names, so a parity test can load it with
// google.protobuf.json_format.ParseDict.  Message fields appear only when
// present, preserving field presence.
using OrderedJson = nlohmann::ordered_json;

static std::string base64(const std::string& value) {
    static const char* alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string result;
    size_t i = 0;
    for (; i + 2 < value.size(); i += 3) {
        const uint32_t n = (uint8_t(value[i]) << 16) | (uint8_t(value[i + 1]) << 8) | uint8_t(value[i + 2]);
        result += alphabet[n >> 18]; result += alphabet[(n >> 12) & 63];
        result += alphabet[(n >> 6) & 63]; result += alphabet[n & 63];
    }
    if (i + 1 == value.size()) {
        const uint32_t n = uint8_t(value[i]) << 16;
        result += alphabet[n >> 18]; result += alphabet[(n >> 12) & 63]; result += "==";
    } else if (i + 2 == value.size()) {
        const uint32_t n = (uint8_t(value[i]) << 16) | (uint8_t(value[i + 1]) << 8);
        result += alphabet[n >> 18]; result += alphabet[(n >> 12) & 63]; result += alphabet[(n >> 6) & 63]; result += '=';
    }
    return result;
}

static std::string duration_json(int64_t seconds, int32_t nanos) {
    if (nanos <= -1000000000 || nanos >= 1000000000 || (seconds > 0 && nanos < 0) || (seconds < 0 && nanos > 0))
        throw std::runtime_error("invalid duration");
    const bool negative = seconds < 0 || nanos < 0;
    const uint64_t whole = negative ? 0 - static_cast<uint64_t>(seconds) : static_cast<uint64_t>(seconds);
    const uint32_t fraction = static_cast<uint32_t>(nanos < 0 ? -nanos : nanos);
    char text[48];
    if (fraction == 0) std::snprintf(text, sizeof text, "%s%llus", negative ? "-" : "", static_cast<unsigned long long>(whole));
    else std::snprintf(text, sizeof text, "%s%llu.%09us", negative ? "-" : "", static_cast<unsigned long long>(whole), fraction);
    return text;
}

static std::string timestamp_json(int64_t seconds, int32_t nanos) {
    // RFC 3339 in UTC, limited to the protobuf Timestamp range 0001..9999.
    if (seconds < -62135596800LL || seconds > 253402300799LL || nanos < 0 || nanos >= 1000000000)
        throw std::runtime_error("timestamp out of range");
    const std::time_t time = static_cast<std::time_t>(seconds);
    std::tm utc{};
    if (!gmtime_r(&time, &utc)) throw std::runtime_error("cannot format timestamp");
    char text[48];
    std::snprintf(text, sizeof text, "%04d-%02d-%02dT%02d:%02d:%02d", utc.tm_year + 1900, utc.tm_mon + 1,
                  utc.tm_mday, utc.tm_hour, utc.tm_min, utc.tm_sec);
    std::string result = text;
    if (nanos != 0) { std::snprintf(text, sizeof text, ".%09d", nanos); result += text; }
    return result + 'Z';
}

static OrderedJson quota_json(const rlc::RateLimitResponse::Quota& quota) {
    OrderedJson out;
    out["requests"] = quota.requests_;
    if (quota.valid_until_) out["valid_until"] = timestamp_json(quota.valid_until_->seconds_, quota.valid_until_->nanos_);
    out["id"] = quota.id_;
    return out;
}

static OrderedJson headers_json(const std::vector<rlc::RateLimitResponse::HeaderValue>& headers) {
    OrderedJson out = OrderedJson::array();
    for (const auto& header : headers)
        out.push_back({{"key", header.key_}, {"value", header.value_}, {"raw_value", base64(header.raw_value_)}});
    return out;
}

static OrderedJson response_json(const rlc::RateLimitResponse& response) {
    OrderedJson out;
    out["overall_code"] = code_name(response.overall_code_);
    out["statuses"] = OrderedJson::array();
    for (const auto& status : response.statuses_) {
        OrderedJson item;
        item["code"] = code_name(status.code());
        if (status.current_limit_) {
            const auto& limit = *status.current_limit_;
            item["current_limit"] = {{"name", limit.name()}, {"requests_per_unit", limit.requests_per_unit()},
                                     {"unit", rlc::to_string(limit.unit())}};
        }
        item["limit_remaining"] = status.limit_remaining();
        if (status.duration_until_reset_)
            item["duration_until_reset"] = duration_json(status.duration_until_reset_->seconds_,
                                                         status.duration_until_reset_->nanos_);
        if (status.quota_) item["quota"] = quota_json(*status.quota_);
        out["statuses"].push_back(std::move(item));
    }
    out["response_headers_to_add"] = headers_json(response.response_headers_to_add_);
    out["request_headers_to_add"] = headers_json(response.request_headers_to_add_);
    out["raw_body"] = base64(response.raw_body_);
    if (response.dynamic_metadata_) out["dynamic_metadata"] = OrderedJson::parse(response.dynamic_metadata_->dump());
    if (response.quota_) out["quota"] = quota_json(*response.quota_);
    return out;
}

static uint16_t parse_port(const char* text) {
    char* end = nullptr;
    errno = 0;
    const unsigned long port = std::strtoul(text, &end, 10);
    if (errno || end == text || *end || port == 0 || port > 65535)
        throw std::runtime_error(std::string("invalid port: ") + text);
    return static_cast<uint16_t>(port);
}

int main(int argc, char** argv) {
    int fd = -1;
    try {
        // Defaults match the DPO socket demo (scripts/run_client_ratelimit.sh).
        const char* host = "127.0.0.1";
        uint16_t port = 8500;
        bool json_output = false;
        const char* request_path = nullptr;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if ((arg == "--host" || arg == "--port") && i + 1 < argc) {
                if (arg == "--host") host = argv[++i];
                else port = parse_port(argv[++i]);
            } else if (arg == "--json") {
                json_output = true;
            } else if (arg == "-h" || arg == "--help") {
                std::cout << kUsage << '\n';
                return 0;
            } else if ((arg == "-" || arg.rfind("--", 0) != 0) && !request_path) {
                request_path = argv[i];
            } else {
                throw std::runtime_error(kUsage);
            }
        }
        const auto request = read_request(request_path ? request_path : "config/ratelimit/request.json");

        const uint64_t transaction_id = 1, timestamp = 0;
        const auto packet = ratelimit_packet_codec::encode_request_packet(
            request, transaction_id, timestamp, 1, dpo_socket::kMaxPacketBytes);

        auto check = [](bool success, const char* operation) {
            if (!success) throw std::runtime_error(std::string(operation) + ": " + std::strerror(errno));
        };
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        check(fd != -1, "socket");
        timeval timeout{};
        timeout.tv_sec = 3;
        check(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != -1, "setsockopt");
        sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(port);
        if (inet_pton(AF_INET, host, &server.sin_addr) != 1)
            throw std::runtime_error(std::string("invalid IPv4 address: ") + host);
        check(connect(fd, reinterpret_cast<sockaddr*>(&server), sizeof(server)) != -1, "connect");
        const auto sent = send(fd, packet.data(), packet.size(), 0);
        check(sent != -1, "send");
        if (sent != static_cast<ssize_t>(packet.size())) throw std::runtime_error("unexpected send length");

        std::array<uint8_t, dpo_socket::kMaxPacketBytes> response{};
        const auto received = recv(fd, response.data(), response.size(), MSG_TRUNC);
        check(received != -1, "recv");
        close(fd);
        fd = -1;
        const auto decoded = ratelimit_packet_codec::decode_response_packet(
            response.data(), static_cast<size_t>(received), transaction_id, timestamp,
            dpo_socket::kMaxPacketBytes);
        const auto code = static_cast<uint8_t>(decoded.overall_code_);
        const auto& statuses = decoded.statuses_;
        if (code == 0) {
            if (!statuses.empty() || !decoded.response_headers_to_add_.empty() ||
                !decoded.request_headers_to_add_.empty() || !decoded.raw_body_.empty() ||
                decoded.dynamic_metadata_ || decoded.quota_) {
                throw std::runtime_error("UNKNOWN response must have an empty body");
            }
        } else if (statuses.size() != request.descriptors().size()) {
            throw std::runtime_error("status count differs from request descriptor count");
        }
        if (json_output) {
            std::cout << response_json(decoded).dump() << '\n';
            return code == 0 ? 1 : 0;
        }
        std::cout << "RateLimitResponse: xid=1, overall_code="
                  << code_name(static_cast<rlc::RateLimitResponse::Code>(code))
                  << ", statuses=" << statuses.size() << '\n';
        print_statuses(statuses);
        print_response_fields(decoded);
        return code == 0 ? 1 : 0;
    } catch (const std::exception& error) {
        if (fd != -1) close(fd);
        std::cerr << error.what() << '\n';
        return 1;
    }
}
