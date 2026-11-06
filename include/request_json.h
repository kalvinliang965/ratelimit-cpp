#pragma once

#include "types.h"
#include <nlohmann/json.hpp>

namespace rlc {
// Convert the JSON request format used by the XRPC examples to native types.
RateLimitRequest MakeRequestFromJson(const nlohmann::json& document);
}
