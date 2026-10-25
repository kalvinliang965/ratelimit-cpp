#include "service.h"
#include "grpc_conversion.h"
#include "backend.h"
#include "stats.h"
#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>
#include <list>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include "common.h"
#include <functional>

namespace rlc {
namespace {

constexpr auto OK = RateLimitResponse::Code::OK;
constexpr auto OVER = RateLimitResponse::Code::OVER_LIMIT;

using RateLimitResponseUnit = RateLimitResponse::RateLimit::Unit;

using RateLimitDescriptorStatus = RateLimitResponse::DescriptorStatus;

int64_t nowSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t divider(RateLimitResponseUnit unit) {
  switch (unit) {
    case RateLimitResponseUnit::SECOND: return 1;
    case RateLimitResponseUnit::MINUTE: return 60;
    case RateLimitResponseUnit::HOUR: return 3600;
    case RateLimitResponseUnit::DAY: return 86400;
    case RateLimitResponseUnit::WEEK: return 7*86400;
    case RateLimitResponseUnit::MONTH: return 30*86400;
    case RateLimitResponseUnit::YEAR: return 365*86400;
    default: throw std::runtime_error("unknown rate limit unit");
  }
}

int64_t monthStart(int64_t now, bool next = false) {
  time_t time = now;
  tm date{}; gmtime_r(&time, &date);
  date.tm_mday = 1; date.tm_hour = date.tm_min = date.tm_sec = 0;
  if (next) ++date.tm_mon;
  return timegm(&date);
}

int64_t reset(RateLimitResponseUnit unit, bool calendar) {
  const auto now = nowSeconds();
  if (calendar && unit == RateLimitResponseUnit::MONTH) return monthStart(now, true)-now;
  const auto seconds = divider(unit); return seconds-now%seconds;
}

int64_t expiration(RateLimitResponseUnit unit, bool calendar) {
  if (calendar && unit == RateLimitResponseUnit::MONTH) {
	return reset(unit, true);
  }
  return divider(unit);
}

std::string keyFor(
	const RateLimitRequest& request, int index, const Limit& limit, 
	int64_t now, const std::string& prefix, bool calendar
) {

  std::string key = prefix + request.domain() + "_";

  const auto& entries = request.descriptors(index).entries();
  for (int i = 0; i < entries.size(); ++i) {
    const auto& pattern = static_cast<size_t>(i) < limit.share_patterns.size() ? limit.share_patterns[i] : std::string();
    key += entries[i].key() + "_" + (pattern.empty() ? entries[i].value() : pattern) + "_";
  }
  int64_t start;
  if (calendar && limit.limit.unit() == RateLimitResponseUnit::MONTH) start = monthStart(now);
  else { const auto duration = divider(limit.limit.unit()); start = now/duration*duration; }
  return key + std::to_string(start);
}
std::string groupKey(const GrpcRateLimitDescriptor& descriptor) {
  std::string backend, model, all;
  for (const auto& entry : descriptor.entries()) {
    if (entry.key() == "backend_name") backend = entry.value();
    else if (entry.key() == "model_name_override") model = entry.value();
    all += entry.key() + "=" + entry.value() + ";";
  }
  return backend.empty() && model.empty() ? all : backend + "|" + model;
}
void mergeMetadata(nlohmann::json& destination, const nlohmann::json& source) {
  for (const auto& [key, value] : source.items()) {
    auto found = destination.find(key);
    if (found == destination.end()) {
      destination[key] = value;
    } else if (found->is_object() && value.is_object()) {
      mergeMetadata(*found, value);
    }
  }
}
std::string overrideString(const GrpcRateLimitDescriptor& descriptor) {
  const auto& limit = descriptor.limit();
  std::string value;
  if (limit.requests_per_unit()) value = "requests_per_unit:" + std::to_string(limit.requests_per_unit());
  if (limit.unit()) {
    if (!value.empty()) value += " ";
    const auto* field = limit.GetDescriptor()->FindFieldByName("unit");
    const auto* unit = field->enum_type()->FindValueByNumber(limit.unit());
    value += "unit:" + (unit ? unit->name() : std::to_string(limit.unit()));
  }
  return value;
}
nlohmann::json metadataFor(const RateLimitRequest& request, const std::vector<int>& passed,
                           const std::vector<std::optional<Limit>>& limits, bool quota) {
  nlohmann::json out = nlohmann::json::object();
  out["domain"] = request.domain();
  out["descriptors"] = nlohmann::json::array();
  for (const auto& descriptor : request.descriptors()) {
    nlohmann::json item = nlohmann::json::object();
    item["entries"] = nlohmann::json::array();
    for (const auto& entry : descriptor.entries()) {
      item["entries"].push_back(entry.key() + "=" + entry.value());
    }
    if (descriptor.has_limit()) item["limit"] = overrideString(grpc_conversion::ToProto(descriptor));
    if (descriptor.has_hits_addend()) item["hitsAddend"] = static_cast<double>(descriptor.hits_addend().value());
    out["descriptors"].push_back(std::move(item));
  }
  if (request.hits_addend()) out["hitsAddend"] = request.hits_addend();
  nlohmann::json passed_metadata = nlohmann::json::object();
  for (int index : passed) if (limits[index] && limits[index]->metadata) mergeMetadata(passed_metadata, *limits[index]->metadata);
  if (!passed_metadata.empty()) out["metadata"] = std::move(passed_metadata);
  if (quota) {
    std::unordered_set<std::string> seen;
    nlohmann::json passed_backends = nlohmann::json::array();
    for (int index : passed) {
      const auto& descriptor = request.descriptors(index);
      const auto key = groupKey(grpc_conversion::ToProto(descriptor));
      if (seen.count(key)) continue;
      std::string backend, model;
      for (const auto& entry : descriptor.entries()) {
        if (entry.key() == "backend_name") backend = entry.value();
        else if (entry.key() == "model_name_override") model = entry.value();
      }
      if (backend.empty() && model.empty()) continue;
      seen.insert(key);
      nlohmann::json entry = nlohmann::json::object();
      entry["backend_name"] = backend;
      entry["model_name_override"] = model;
      passed_backends.push_back(std::move(entry));
    }
    if (!passed_backends.empty()) out["passedBackends"] = std::move(passed_backends);
  }
  return out;
}

const char* decrementScript = R"lua(
local current = redis.call('GET', KEYS[1])
if current == false then return 0 end
local new_val = math.floor(math.max(0, tonumber(current) - tonumber(ARGV[1])))
redis.call('SET', KEYS[1], tostring(new_val))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return new_val
)lua";

struct ResponseSettings {
  bool shadow = envBool("SHADOW_MODE"), quota = envBool("QUOTA_MODE"), metadata = envBool("RESPONSE_DYNAMIC_METADATA");
  bool headers = envBool("LIMIT_RESPONSE_HEADERS_ENABLED"), request_headers = envBool("LIMIT_REQUEST_HEADERS_ENABLED");
  bool per_unit_headers = envBool("LIMIT_PER_UNIT_HEADERS_ENABLED"), calendar = envBool("USE_CALENDAR_MONTH_RATE_LIMIT");
  std::string limit_header = env("LIMIT_LIMIT_HEADER", "RateLimit-Limit");
  std::string remaining_header = env("LIMIT_REMAINING_HEADER", "RateLimit-Remaining");
  std::string reset_header = env("LIMIT_RESET_HEADER", "RateLimit-Reset");
  std::string request_limit_header = env("LIMIT_REQUEST_LIMIT_HEADER", "RateLimit-Limit");
  std::string request_remaining_header = env("LIMIT_REQUEST_REMAINING_HEADER", "RateLimit-Remaining");
  std::string request_reset_header = env("LIMIT_REQUEST_RESET_HEADER", "RateLimit-Reset");
};
}

struct Service::Impl {
  mutable std::mutex config_mutex;
  std::shared_ptr<Config> config;
  ResponseSettings settings;
  std::unique_ptr<Redis> redis, per_second;
  std::unique_ptr<Memcache> memcache;
  Stats stats;
  bool stop_increment = envBool("STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT");
  bool calendar = envBool("USE_CALENDAR_MONTH_RATE_LIMIT");
  float near_limit_ratio = std::stof(env("NEAR_LIMIT_RATIO", "0.8"));
  int64_t jitter_max = envInt("EXPIRATION_JITTER_MAX_SECONDS", 300);
  std::string prefix = env("CACHE_KEY_PREFIX");
  size_t cache_budget = std::max<int64_t>(0, envInt("LOCAL_CACHE_SIZE_IN_BYTES", 0));
  std::mutex cache_mutex, random_mutex;
  std::mt19937_64 random{std::random_device{}()};
  struct LocalEntry { int64_t expires; size_t size; std::list<std::string>::iterator position; };
  std::list<std::string> lru;
  std::unordered_map<std::string, LocalEntry> local;
  size_t cache_bytes = 0;
  void rateMetric(const Limit& limit, const char* suffix, uint64_t value) {
    stats.add("ratelimit.service.rate_limit." + sanitizeStatsName(limit.full_key) + "." + suffix, value);
  }

  Impl() {
    const auto backend = env("BACKEND_TYPE", "redis");
    if (backend == "redis") {
      if (envBool("REDIS_PERSECOND")) {
	per_second = std::make_unique<Redis>(true);
      }
      redis = std::make_unique<Redis>();
    } else if (backend == "memcache") {
	memcache = std::make_unique<Memcache>();
    } else {
	throw std::runtime_error("unknown backend type: " + backend);
    }
  }

  bool cached(const std::string& key) {
    if (!cache_budget) return false;
    std::lock_guard<std::mutex> lock(cache_mutex);
    const auto found = local.find(key);
    if (found == local.end()) {
	return false;
    }
    if (found->second.expires <= nowSeconds()) {
      cache_bytes -= found->second.size; 
      lru.erase(found->second.position); 
      local.erase(found); return false;
    }
    lru.splice(lru.begin(), lru, found->second.position);
    return true;
  }

  void cache(const std::string& key, RateLimitResponseUnit unit) {
    if (!cache_budget) return;
    const size_t size = key.size()+24;
    // freecache reserves at least 512 KiB and rejects entries exceeding 1/1024
    // of its buffer, including its 24-byte entry header.
    const size_t effective_budget = std::max<size_t>(cache_budget, 512*1024);
    if (size > effective_budget/1024 || key.size() > 65535) return;
    std::lock_guard<std::mutex> lock(cache_mutex);
    const auto found = local.find(key);
    if (found != local.end()) {
      found->second.expires = nowSeconds()+expiration(unit, calendar);
      lru.splice(lru.begin(), lru, found->second.position); return;
    }
    while (!lru.empty() && cache_bytes+size > effective_budget) {
      auto entry = local.find(lru.back()); cache_bytes -= entry->second.size; local.erase(entry); lru.pop_back();
    }
    lru.push_front(key);
    local.emplace(key, LocalEntry{nowSeconds()+expiration(unit, calendar), size, lru.begin()});
    cache_bytes += size;
  }
  int64_t ttl(RateLimitResponseUnit unit) {
    int64_t value = expiration(unit, calendar);
    if (jitter_max > 0) {
      std::lock_guard<std::mutex> lock(random_mutex);
      value += std::uniform_int_distribution<int64_t>(0, jitter_max-1)(random);
    }
    return value;
  }
  std::vector<RateLimitDescriptorStatus> limit(const RateLimitRequest& request, const std::vector<std::optional<Limit>>& limits) {
    const size_t count = limits.size();

    std::vector<std::string> keys(count);
    std::vector<uint64_t> hits(count), results(count), current(count), applied(count);
    std::vector<bool> negative(count), local_over(count), near(count);

    bool any_over = false, any_near = false;

    const auto now = nowSeconds();

    for (size_t i = 0; i < count; ++i) {
      const auto& descriptor = request.descriptors(i);
      hits[i] = descriptor.has_hits_addend() ? descriptor.hits_addend().value() : std::max<uint64_t>(1, request.hits_addend());
      negative[i] = descriptor.is_negative_hits();

      if (!limits[i]) {
	continue;
      }

      keys[i] = keyFor(request, i, *limits[i], now, prefix, calendar);

      if (negative[i]) {
	rateMetric(*limits[i], "total_negative_hits", hits[i]);
      } else {
	rateMetric(*limits[i], "total_hits", hits[i]);
      }      
      if (!negative[i] && cached(keys[i])) { 
	local_over[i] = true; any_over = true; 
      }
    }

    if (redis) {
      std::vector<RedisCommand> main_commands, second_commands;
      auto pipeline = [&](size_t i) -> std::vector<RedisCommand>& {
        return per_second && limits[i]->limit.unit() == RateLimitResponseUnit::SECOND ? second_commands : main_commands;
      };
      auto execute = [&] {
        redis->pipeline(main_commands);
        if (per_second) per_second->pipeline(second_commands);
        main_commands.clear(); second_commands.clear();
      };
      if (stop_increment && !any_over) {
        for (size_t i = 0; i < count; ++i) if (!keys[i].empty() && !negative[i])
          pipeline(i).push_back({keys[i], {"GET", keys[i]}, &current[i]});
        execute();
        for (size_t i = 0; i < count; ++i) if (!keys[i].empty() && !negative[i]) {
          if (current[i]+hits[i] > limits[i]->limit.requests_per_unit()) { near[i] = true; any_near = true; }
        }
      }
      for (size_t i = 0; i < count; ++i) {
        if (keys[i].empty() || local_over[i]) continue;
        auto& commands = pipeline(i);
        const auto expiry = std::to_string(ttl(limits[i]->limit.unit()));
        if (negative[i]) commands.push_back({keys[i], {"EVAL", decrementScript, "1", keys[i], std::to_string(hits[i]), expiry}, &results[i]});
        else {
          uint64_t increment = hits[i];
          if (stop_increment && (any_over || (any_near && !near[i]))) increment = 0;
          applied[i] = increment;
          commands.push_back({keys[i], {"INCRBY", keys[i], std::to_string(increment)}, &results[i]});
          commands.push_back({keys[i], {"EXPIRE", keys[i], expiry}, nullptr});
        }
      }
      execute();
    } else {
      std::vector<std::string> to_get;
      for (size_t i = 0; i < count; ++i) if (!keys[i].empty() && !local_over[i]) to_get.push_back(keys[i]);
      auto values = memcache->get(to_get);
      for (size_t i = 0; i < count; ++i) {
        const auto before = values[keys[i]];
        applied[i] = hits[i];
        results[i] = negative[i] ? (before > hits[i] ? before-hits[i] : 0) : before+hits[i];
      }
    }
    std::vector<RateLimitDescriptorStatus> statuses(count);

    std::vector<MemcacheUpdate> updates;
    for (size_t i = 0; i < count; ++i) {
      auto& status = statuses[i]; 
      status.set_code(OK);

      if (keys[i].empty()) continue;
      const auto& limit = *limits[i];
      status.set_current_limit(limit.limit);
      rlc::RateLimitResponse::Duration dur(
	reset(limit.limit.unit(), calendar),
	0
      );
      status.set_duration_until_reset(dur);

      const uint64_t threshold = limit.limit.requests_per_unit();
      if (negative[i]) {
        status.set_limit_remaining(threshold > results[i] ? threshold-results[i] : 0);
      } else if (local_over[i] || results[i] > threshold) {
        if (!limit.shadow) status.set_code(OVER);
        if (!local_over[i]) cache(keys[i], limit.limit.unit());
      } else status.set_limit_remaining(threshold-results[i]);

      if (!negative[i]) {
        // Go calculates these counters from the unbounded backend value, even
        // when a shadow rule changes the response code to OK.  The configured
        // request addend, rather than the potentially suppressed increment,
        // is what is reported as a hit.
        if (local_over[i]) {
          rateMetric(limit, "over_limit", hits[i]);
          rateMetric(limit, "over_limit_with_local_cache", hits[i]);
          if (limit.shadow) rateMetric(limit, "shadow_mode", hits[i]);
        } else {
          uint64_t before = results[i] >= applied[i] ? results[i] - applied[i] : 0;
          if (stop_increment && !any_over) before = current[i];
          const uint64_t near_threshold = static_cast<uint64_t>(std::floor(
              static_cast<float>(threshold) * near_limit_ratio));
          if (results[i] > threshold) {
            const uint64_t over = before >= threshold ? hits[i] : results[i] - threshold;
            rateMetric(limit, "over_limit", over);
            if (before < threshold) {
              const uint64_t start = std::max(near_threshold, before);
              rateMetric(limit, "near_limit", threshold > start ? threshold - start : 0);
            }
            if (limit.shadow) rateMetric(limit, "shadow_mode", over);
          } else {
            rateMetric(limit, "within_limit", hits[i]);
            if (results[i] > near_threshold) {
              const uint64_t near_hits = before >= near_threshold ? hits[i] : results[i] - near_threshold;
              rateMetric(limit, "near_limit", near_hits);
            }
          }
        }
      }

      if (memcache && !local_over[i]) {
	updates.push_back({keys[i], hits[i], ttl(limit.limit.unit()), negative[i]});
      }
    }

    if (memcache) {
	memcache->update(std::move(updates));
    }
    return statuses;
  }
};

Service::Service() : impl_(std::make_unique<Impl>()) {}
Service::~Service() = default;
void Service::setConfig(std::shared_ptr<Config> config) {
  ResponseSettings settings;
  std::lock_guard<std::mutex> lock(impl_->config_mutex);
  impl_->config = std::move(config); 
  impl_->settings = std::move(settings);
}
std::shared_ptr<Config> Service::getConfig() const {
  std::lock_guard<std::mutex> lock(impl_->config_mutex); 
  return impl_->config;
}

bool Service::backendHealthy() const {
  return (!impl_->redis || impl_->redis->healthy()) && (!impl_->per_second || impl_->per_second->healthy());
}
void Service::recordConfigLoad(bool success) {
  impl_->stats.add(success ? "ratelimit.service.config_load_success" : "ratelimit.service.config_load_error");
}
std::string Service::statsDump() const { return impl_->stats.dump(); }



RateLimitStatus Service::ShouldRateLimit(const RateLimitRequest* request, RateLimitResponse* response) {
  try {
    if (request->domain().empty()) {
	return {
		RateLimitStatusCode::UNKNOWN, "rate limit domain must not be empty"
	};
    }

    if (request->descriptors().empty()) {
	return {
	    RateLimitStatusCode::UNKNOWN, "rate limit descriptor list must not be empty"
	};
    }    

    std::shared_ptr<Config> config;
    ResponseSettings settings;
    {
      std::lock_guard<std::mutex> lock(impl_->config_mutex);
      config = impl_->config; 
      settings = impl_->settings;
    }
    if (!config) {
	return {RateLimitStatusCode::UNKNOWN, "no rate limit configuration loaded"};
    }

    const int count = request->descriptors_size();

    std::vector<std::optional<Limit>> limits(count);
    std::vector<bool> unlimited(count);
    std::unordered_set<std::string> replaces;

    for (int i = 0; i < count; ++i) {
      limits[i] = config->find(request->domain(), request->descriptors(i));
      if (limits[i]) {
        for (const auto& name : limits[i]->replaces) replaces.insert(name);
        if (limits[i]->unlimited) { unlimited[i] = true; limits[i].reset(); }
      }
    }

    for (auto& limit : limits) {
	if (limit && !limit->limit.name().empty() && replaces.count(limit->limit.name())) {
		limit.reset();
	}
    }

    const auto statuses = impl_->limit(*request, limits);
    uint32_t min_remaining = std::numeric_limits<uint32_t>::max();
    const RateLimitDescriptorStatus* minimum = nullptr;
    std::vector<int> passed;
    std::unordered_map<std::string, bool> quota_groups;
    std::vector<std::string> descriptor_groups(count);
    bool ordinary_over = false;

    *response = RateLimitResponse{}; // clear state

    for (int i = 0; i < count; ++i) {
      const auto& status = statuses[i];
      if ((settings.headers || settings.request_headers) && status.has_current_limit() && status.limit_remaining() < min_remaining) {
        min_remaining = status.limit_remaining(); minimum = &status;
      }

      auto& output = response->add_statuses();

      if (unlimited[i]) { 
	output.set_code(OK); 
	output.set_limit_remaining(std::numeric_limits<uint32_t>::max()); 
	continue; 
      }
      output = status;

      const bool is_quota = settings.quota || (limits[i] && limits[i]->quota);
      const bool over = status.code() == OVER;
      if (!over) passed.push_back(i);
      if (is_quota) {
        const auto key = groupKey(grpc_conversion::ToProto(request->descriptors(i)));
        descriptor_groups[i] = key;
        quota_groups[key] = quota_groups[key] || over;
      } else if (over) { ordinary_over = true; minimum = &status; min_remaining = 0; }
    }
    bool quota_over = !quota_groups.empty();

    for (const auto& [key, over] : quota_groups) {
	if (!over) {
		quota_over = false;
	}
    }

    auto final_code = ordinary_over || quota_over ? OVER : OK;

    auto add_header = [](auto& header, const std::string& key, const std::string& value) {
      	header.set_key(key); 
	header.set_value(value);
    };

    if (settings.per_unit_headers) {
      std::vector<RateLimitResponseUnit> units;
      std::unordered_map<RateLimitResponseUnit, const RateLimitDescriptorStatus*> closest;
      for (const auto& status : statuses) if (status.has_current_limit()) {
        const auto unit = static_cast<RateLimitResponseUnit>(status.current_limit().unit());
        const auto found = closest.find(unit);
        if (found == closest.end()) units.push_back(unit);
        if (found == closest.end() || status.limit_remaining() < found->second->limit_remaining()) closest[unit] = &status;
      }
      for (RateLimitResponseUnit unit : units) {
        const auto* status = closest[unit];

        std::string suffix = rlc::to_string(unit);

        for (size_t i = 1; i < suffix.size(); ++i) suffix[i] = std::tolower(static_cast<unsigned char>(suffix[i]));
        suffix += "s";

	auto& header1 = response->add_response_header();
        add_header(
		header1,
		"RateLimit-Limit-"+suffix, 
		std::to_string(status->current_limit().requests_per_unit())
	);
	auto& header2 = response->add_response_header();
        add_header(
		header2,
		"RateLimit-Remaining-"+suffix, 
		std::to_string(status->limit_remaining())
	);
      }
    }
    auto add_standard_headers = [&](
    	auto get_header, const std::string& limit, 
	const std::string& remaining, const std::string& reset_name
    ) {
      auto& header1 = get_header();
      add_header(header1, limit, std::to_string(minimum->current_limit().requests_per_unit()));

      auto& header2 = get_header();
      add_header(header2, remaining, std::to_string(minimum->limit_remaining()));

      auto& header3 = get_header();
      add_header(header3, reset_name, std::to_string(reset(static_cast<RateLimitResponseUnit>(minimum->current_limit().unit()), settings.calendar)));
    };

    if (settings.headers && minimum) {
	add_standard_headers(
		[&]() -> RateLimitResponse::HeaderValue& { return response->add_response_header();}, 
		settings.limit_header, settings.remaining_header, settings.reset_header
	);
    }

    if (settings.request_headers && minimum) {
	add_standard_headers(
		[&]() -> RateLimitResponse::HeaderValue& { return response->add_request_header(); },
		settings.request_limit_header, settings.request_remaining_header, settings.request_reset_header);
    }

    if (final_code == OVER && settings.shadow) {
      impl_->stats.add("ratelimit.service.global_shadow_mode");
      final_code = OK;
    }

    if (settings.metadata) {
      std::vector<int> available;
      for (int index : passed) {
        const auto& key = descriptor_groups[index];
        if (!key.empty() && quota_groups.count(key) && quota_groups[key]) {
		continue;
	}
        available.push_back(index);
      }
    response->dynamic_metadata_ = metadataFor(*request, available, limits, !quota_groups.empty());
    }
    response->set_overall_code(final_code);
    return {RateLimitStatusCode::OK, ""};
  } catch (const std::exception& e) {
    const std::string message = e.what();
    const bool backend_error = message.rfind("backend ", 0) == 0 ||
        message.rfind("response returned from Conn:", 0) == 0;
    impl_->stats.add(backend_error ? "ratelimit.service.call.should_rate_limit.redis_error"
                                   : "ratelimit.service.call.should_rate_limit.service_error");
    *response = RateLimitResponse{}; 
    return {RateLimitStatusCode::UNKNOWN, e.what()};
  }
}
}
