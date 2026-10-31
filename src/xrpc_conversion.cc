// Encodes and decodes rate-limit requests and responses using the XRPC payload format.
// The reader and writer check field widths, payload bounds, and malformed input.
#include "xrpc_conversion.h"

#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>
#include <cstring>


#include <iostream>

namespace xrpc_conversion {
	
namespace {

using Request = rlc::RateLimitRequest;
using Response = rlc::RateLimitResponse;

class BufferWriter {
private:
    Bytes buffer_;

    void require_space(size_t count) const {
        if (count > kMaxPayloadBytes - buffer_.size()) {
            throw std::runtime_error("BufferWriter: payload is too large");
        }
    }

    // Write the least significant byte first, regardless of the host's byte order.
    void write_little_endian(uint64_t value, size_t count) {
        require_space(count);
        for (size_t i = 0; i < count; ++i) {
            buffer_.push_back(static_cast<uint8_t>(value >> (8 * i)));
        }
    }

public:
    void write_u8(uint8_t value) { write_little_endian(value, 1); }
    void write_u16(uint16_t value) { write_little_endian(value, 2); }
    void write_u32(uint32_t value) { write_little_endian(value, 4); }
    void write_u64(uint64_t value) { write_little_endian(value, 8); }

    // A wire string is one length byte, followed by its bytes (no terminator).
    void write_string(const std::string& value) {
        if (value.size() > std::numeric_limits<uint8_t>::max()) {
            throw std::runtime_error("BufferWriter: string exceeds one-byte length");
        }
        require_space(1 + value.size());
        write_u8(static_cast<uint8_t>(value.size()));
        buffer_.insert(buffer_.end(), value.begin(), value.end());
    }

    // A blob has a two-byte length; it can contain arbitrary bytes, including NUL.
    void write_blob(const std::string& value) {
        if (value.size() > std::numeric_limits<uint16_t>::max()) throw std::runtime_error("blob exceeds two-byte length");
        require_space(2 + value.size());
        write_u16(static_cast<uint16_t>(value.size()));
        buffer_.insert(buffer_.end(), value.begin(), value.end());
    }

    Bytes release() { return std::move(buffer_); }
}; // BufferWriter

// A view of the caller's bytes; it must not outlive that buffer.
class BufferReader {
private:
    const uint8_t* data_;
    size_t size_;
    size_t offset_ = 0;

    void require_bytes(size_t count) const {
        // offset_ is advanced only after this check succeeds.
        if (count > size_ - offset_) {
            throw std::runtime_error("BufferReader: truncated payload");
        }
    }

    uint64_t read_little_endian(size_t count) {
        require_bytes(count);
        uint64_t value = 0;
        for (size_t i = 0; i < count; ++i) {
            value |= static_cast<uint64_t>(data_[offset_++]) << (8 * i);
        }
        return value;
    }

public:
    BufferReader(const uint8_t* data, size_t size) : data_(data), size_(size) {
        if (data == nullptr && size != 0) {
            throw std::runtime_error("BufferReader: null data with nonzero size");
        }
        if (size > kMaxPayloadBytes) {
            throw std::runtime_error("BufferReader: payload is too large");
        }
    }

    uint8_t read_u8() { return static_cast<uint8_t>(read_little_endian(1)); }
    uint16_t read_u16() { return static_cast<uint16_t>(read_little_endian(2)); }
    uint32_t read_u32() { return static_cast<uint32_t>(read_little_endian(4)); }
    uint64_t read_u64() { return read_little_endian(8); }

    // A wire string is one length byte, followed by exactly that many bytes.
    std::string read_string() {
        const size_t length = read_u8();
        require_bytes(length);
        if (length == 0) return {};
        const char* start = reinterpret_cast<const char*>(data_ + offset_);
        std::string value(start, length);
        offset_ += length;
        return value;
    }

    std::string read_blob() {
        const size_t length = read_u16();
        require_bytes(length);
        if (length == 0) return {};
        const char* start = reinterpret_cast<const char*>(data_ + offset_);
        std::string value(start, length);
        offset_ += length;
        return value;
    }

    void require_finished() const {
        if (offset_ != size_) {
            throw std::runtime_error("BufferReader: trailing payload bytes");
        }
    }
}; // BufferReader


} // end of name space


// [domain: string]
// [request hits_addend: u32]
// [descriptor count: u8]
//
// for each descriptor:
//     [entry count: u8]
//     [flags: u8]
//
//     if flags & HAS_LIMIT:
//         [requests_per_unit: u32]
//         [unit: u8]
//
//     if flags & HAS_HITS_ADDEND:
//         [descriptor hits_addend: u64]
//
//     IS_NEGATIVE_HITS is a flag bit; it has no separate value byte.
//
//     for each entry:
//         [key: string]
//         [value: string]
enum class DescriptorFlag : uint8_t {
	HAS_LIMIT = 1 << 0,
	HAS_HITS_ADDEND = 1 << 1,
	IS_NEGATIVE_HITS = 1 << 2
};

Bytes encode_request(const Request& request) {
    BufferWriter writer;
    writer.write_string(request.domain());
    writer.write_u32(request.hits_addend());

    if (request.descriptors().size() > std::numeric_limits<uint8_t>::max()) {
        throw std::runtime_error("descriptor count does not fit in one byte");
    }
    writer.write_u8(static_cast<uint8_t>(request.descriptors().size()));

    for (const auto& descriptor : request.descriptors()) {
        if (descriptor.entries().size() > std::numeric_limits<uint8_t>::max()) {
            throw std::runtime_error("entry count does not fit in one byte");
        }
        writer.write_u8(static_cast<uint8_t>(descriptor.entries().size()));

        uint8_t flags = 0;
        if (descriptor.has_limit()) flags |= static_cast<uint8_t>(DescriptorFlag::HAS_LIMIT);
        if (descriptor.has_hits_addend()) flags |= static_cast<uint8_t>(DescriptorFlag::HAS_HITS_ADDEND);
        if (descriptor.is_negative_hits()) flags |= static_cast<uint8_t>(DescriptorFlag::IS_NEGATIVE_HITS);
        writer.write_u8(flags);

        if (descriptor.has_limit()) {
            const auto& limit = descriptor.limit();
            const int32_t unit = static_cast<int32_t>(limit.unit());
            if (unit < 0 || unit > 6) throw std::runtime_error("invalid request rate-limit unit");
            writer.write_u32(limit.requests_per_unit());
            writer.write_u8(static_cast<uint8_t>(unit));
        }
        if (descriptor.has_hits_addend()) {
            writer.write_u64(*descriptor.hits_addend());
        }
        for (const auto& entry : descriptor.entries()) {
            writer.write_string(entry.key());
            writer.write_string(entry.value());
        }
    }
    return writer.release();
}

Request decode_request(const uint8_t* data, size_t size) {
    BufferReader reader(data, size);
    Request request;
    request.domain_ = reader.read_string();
    request.hits_addend_ = reader.read_u32();
    const uint8_t descriptor_count = reader.read_u8();

    for (unsigned d = 0; d < descriptor_count; ++d) {
        const uint8_t entry_count = reader.read_u8();
        const uint8_t flags = reader.read_u8();
        // Only the lowest three bits have meanings in this format.
        if ((flags & 0xf8u) != 0) {
            throw std::runtime_error("invalid descriptor flags");
        }
        const auto has_flag = [flags](DescriptorFlag flag) {
            return (flags & static_cast<uint8_t>(flag)) != 0;
        };

        rlc::RateLimitDescriptor descriptor;
        if (has_flag(DescriptorFlag::HAS_LIMIT)) {
            const uint32_t requests_per_unit = reader.read_u32();
            const uint8_t unit = reader.read_u8();
            if (unit > 6) throw std::runtime_error("invalid request rate-limit unit");
            descriptor.limit_.emplace(requests_per_unit, static_cast<rlc::RateLimitUnit>(unit));
        }
        if (has_flag(DescriptorFlag::HAS_HITS_ADDEND)) {
            // A present zero is different from an absent hits_addend.
            descriptor.hits_addend_ = reader.read_u64();
        }
        descriptor.is_negative_hits_ = has_flag(DescriptorFlag::IS_NEGATIVE_HITS);

        for (unsigned e = 0; e < entry_count; ++e) {
            std::string key = reader.read_string();
            std::string value = reader.read_string();
            descriptor.entries_.emplace_back(std::move(key), std::move(value));
        }
        request.descriptors_.push_back(std::move(descriptor));
    }
    reader.require_finished();
    return request;
}

// Response wire layout (all multibyte integers are little-endian):
// [status count: u8] [status ...]
// Each status: [code: u8] [remaining: u32] [flags: u8], followed by:
//   flag 1: [limit name: string] [requests: u32] [unit: u8]
//   flag 2: [duration seconds: i64] [duration nanos: i32]
//   flag 4: quota
// A quota is [requests: u32] [id: string] [flags: u8], followed when
// flag 1 is set by [timestamp seconds: i64] [timestamp nanos: i32].
//
// The statuses are followed by [response flags: u8], response headers,
// request headers, and [body: blob]. Each header list starts with a u8 count;
// each header contains [key: blob] [value: blob] [raw value: blob].
// Response flag 1 adds [metadata JSON: blob], and flag 2 adds another quota.
// A string has a u8 byte length; a blob has a u16 byte length.
// overall_code is carried separately in the fixed DPO header.
namespace {
void require(bool valid, const char* error) {
    if (!valid) throw std::runtime_error(error);
}

// Decode two's-complement values without an out-of-range unsigned-to-signed cast.
int64_t signed64(uint64_t bits) {
    return bits >> 63 ? -1 - static_cast<int64_t>(~bits) : static_cast<int64_t>(bits);
}
int32_t signed32(uint32_t bits) {
    return bits >> 31 ? -1 - static_cast<int32_t>(static_cast<uint32_t>(~bits)) : static_cast<int32_t>(bits);
}
void write_time(BufferWriter& out, int64_t seconds, int32_t nanos) {
    out.write_u64(static_cast<uint64_t>(static_cast<uint64_t>(seconds)));
    out.write_u32(static_cast<uint32_t>(static_cast<uint32_t>(nanos)));
}
Response::Timestamp read_time(BufferReader& in) {
    const auto seconds = signed64(in.read_u64());
    const auto nanos = signed32(static_cast<uint32_t>(in.read_u32()));
    return {seconds, nanos};
}

void write_limit(BufferWriter& out, const Response::RateLimit& limit) {
    const auto unit = static_cast<uint32_t>(limit.unit());
    require(unit <= 7, "invalid response unit");
    out.write_string(limit.name());
    out.write_u32(static_cast<uint32_t>(limit.requests_per_unit()));
    out.write_u8(static_cast<uint8_t>(unit));
}
Response::RateLimit read_limit(BufferReader& in) {
    Response::RateLimit limit;
    limit.set_name(in.read_string());
    limit.set_requests_per_unit(static_cast<uint32_t>(in.read_u32()));
    const auto unit = in.read_u8();
    require(unit <= 7, "invalid response unit");
    limit.set_unit(static_cast<Response::RateLimit::Unit>(unit));
    return limit;
}
void write_quota(BufferWriter& out, const Response::Quota& quota) {
    out.write_u32(static_cast<uint32_t>(quota.requests_));
    out.write_string(quota.id_);
    out.write_u8(static_cast<uint8_t>(quota.valid_until_.has_value()));
    if (quota.valid_until_) write_time(out, quota.valid_until_->seconds_, quota.valid_until_->nanos_);
}
Response::Quota read_quota(BufferReader& in) {
    Response::Quota quota;
    quota.requests_ = static_cast<uint32_t>(in.read_u32());
    quota.id_ = in.read_string();
    const auto flags = in.read_u8();
    require(flags <= 1, "invalid quota flags");
    if (flags) quota.valid_until_ = read_time(in);
    return quota;
}
void write_statuses(BufferWriter& out, const Statuses& statuses) {
    require(statuses.size() <= std::numeric_limits<uint8_t>::max(), "too many descriptor statuses");
    out.write_u8(static_cast<uint8_t>(statuses.size()));
    for (const auto& status : statuses) {
        const auto code = static_cast<uint32_t>(status.code());
        require(code <= 2, "invalid descriptor status code");
        out.write_u8(static_cast<uint8_t>(code));
        out.write_u32(static_cast<uint32_t>(status.limit_remaining()));
        out.write_u8(static_cast<uint8_t>((status.current_limit_ ? 1 : 0) | (status.duration_until_reset_ ? 2 : 0) |
                   (status.quota_ ? 4 : 0)));
        if (status.current_limit_) write_limit(out, *status.current_limit_);
        if (status.duration_until_reset_) write_time(out, status.duration_until_reset_->seconds_,
                                                    status.duration_until_reset_->nanos_);
        if (status.quota_) write_quota(out, *status.quota_);
    }
}

Statuses read_statuses(BufferReader& in) {
    const auto count = in.read_u8();
    Statuses statuses;
    for (size_t i = 0; i < count; ++i) {
        const auto code = in.read_u8();
        require(code <= 2, "invalid descriptor status code");
        auto& status = statuses.emplace_back();
        status.set_code(static_cast<Response::Code>(code));
        status.set_limit_remaining(static_cast<uint32_t>(in.read_u32()));
        const auto flags = in.read_u8();
        require(flags <= 7, "invalid descriptor status flags");
        if (flags & 1) status.current_limit_ = read_limit(in);
        if (flags & 2) {
            const auto time = read_time(in);
            status.duration_until_reset_.emplace(time.seconds_, time.nanos_);
        }
        if (flags & 4) status.quota_ = read_quota(in);
    }
    return statuses;
}

void write_headers(BufferWriter& out, const std::vector<Response::HeaderValue>& headers) {
    require(headers.size() <= std::numeric_limits<uint8_t>::max(), "too many headers");
    out.write_u8(static_cast<uint8_t>(headers.size()));
    for (const auto& header : headers) {
        out.write_blob(header.key_);
        out.write_blob(header.value_);
        out.write_blob(header.raw_value_);
    }
}
std::vector<Response::HeaderValue> read_headers(BufferReader& in) {
    const auto count = in.read_u8();
    std::vector<Response::HeaderValue> headers;
    for (size_t i = 0; i < count; ++i) {
        auto& header = headers.emplace_back();
        header.key_ = in.read_blob();
        header.value_ = in.read_blob();
        header.raw_value_ = in.read_blob();
    }
    return headers;
}

void validate_json(const nlohmann::json& value) {
    require(!value.is_discarded() && !value.is_binary(), "unsupported metadata value");
    if (value.is_number_float()) {
        require(std::isfinite(value.get<double>()), "non-finite metadata number");
    }
    if (value.is_structured()) {
        for (const auto& child : value) validate_json(child);
    }
}
std::string encode_metadata(const nlohmann::json& metadata) {
    require(metadata.is_object(), "metadata must be a JSON object");
    validate_json(metadata);
    try {
        return metadata.dump(); // Strict UTF-8; preserves present empty objects.
    } catch (const nlohmann::json::exception& error) {
        throw std::runtime_error(std::string("invalid metadata JSON: ") + error.what());
    }
}
nlohmann::json decode_metadata(const std::string& data) {
    require(data.find('\0') == std::string::npos, "unescaped NUL in metadata JSON");
    try {
        auto metadata = nlohmann::json::parse(data);
        require(metadata.is_object(), "metadata must be a JSON object");
        validate_json(metadata);
        return metadata;
    } catch (const nlohmann::json::exception& error) {
        throw std::runtime_error(std::string("invalid metadata JSON: ") + error.what());
    }
}
} // namespace

enum class ResponseFlag : uint8_t {
	HAS_METADATA = 1 << 0, // 1
	HAS_QUOTA = 1 << 1 // 2
};

Bytes encode_statuses(const Statuses& statuses) {
    BufferWriter out;
    write_statuses(out, statuses);
    return out.release();
}
Statuses decode_statuses(const uint8_t* data, size_t size) {
    BufferReader in(data, size);
    auto statuses = read_statuses(in);
    in.require_finished();
    return statuses;
}

Bytes encode_response(const Response& response) {
    require(static_cast<uint32_t>(response.overall_code_) <= 2, "invalid overall code");
    BufferWriter out;
    write_statuses(out, response.statuses_);

    uint8_t flags = 0;

    if (response.dynamic_metadata_) flags |= static_cast<uint8_t>(ResponseFlag::HAS_METADATA);
    if (response.quota_)            flags |= static_cast<uint8_t>(ResponseFlag::HAS_QUOTA);
    out.write_u8(flags);

    write_headers(out, response.response_headers_to_add_);
    write_headers(out, response.request_headers_to_add_);
    out.write_blob(response.raw_body_);
    if (response.dynamic_metadata_) out.write_blob(encode_metadata(*response.dynamic_metadata_));
    if (response.quota_) write_quota(out, *response.quota_);
    return out.release();
}
Response decode_response(const uint8_t* data, size_t size, Response::Code overall_code) {
    require(static_cast<uint32_t>(overall_code) <= 2, "invalid overall code");
    BufferReader in(data, size);
    Response response;
    response.set_overall_code(overall_code);
    response.statuses_ = read_statuses(in);
    const auto flags = in.read_u8();
    require(flags <= 3, "invalid response flags");
    response.response_headers_to_add_ = read_headers(in);
    response.request_headers_to_add_ = read_headers(in);
    response.raw_body_ = in.read_blob();
    if (flags & static_cast<uint8_t>(ResponseFlag::HAS_METADATA)) response.dynamic_metadata_ = decode_metadata(in.read_blob());
    if (flags & static_cast<uint8_t>(ResponseFlag::HAS_QUOTA)) response.quota_ = read_quota(in);
    in.require_finished();
    return response;
}
} // namespace xrpc_conversion
