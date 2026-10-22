#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace rlc {

// Small StatsD counter sink compatible with the gostats sink used by the Go
// service.  Counters are accumulated locally and emitted as StatsD `|c`
// samples without participating in request latency.
class Stats {
 public:
  Stats();
  ~Stats();

  Stats(const Stats&) = delete;
  Stats& operator=(const Stats&) = delete;

  void add(const std::string& name, std::uint64_t value = 1);
  std::string dump() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// gostats sanitizes descriptor metric names after config lookup.  Keep this
// separate from Config's optional dot sanitization: matching must still use
// the original descriptor values.
std::string sanitizeStatsName(std::string value);

}  // namespace rlc
