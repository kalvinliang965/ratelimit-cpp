"""Bytes used to compare C++ and Go results, exact except for two Go formatting choices.

Go's protobuf library randomizes some formatting per binary (internal/detrand),
so two equally correct Go builds can disagree.  Only these spots are handled:

1. RateLimitResponse.dynamic_metadata (a google.protobuf.Struct) field
   descriptors[i].limit holds Go's RateLimitDescriptor.Limit.String(): protobuf
   text format of envoy.extensions.common.ratelimit.v3.RateLimitDescriptor.
   RateLimitOverride, whose field separator may be one or two spaces.  The
   string is parsed with that message type and compared as a message.  A string
   that does not parse strictly is compared byte-for-byte.
2. A gRPC error's details may contain protobuf-go's error prefix, "proto: " or
   "proto:\\u00a0" (a non-breaking space).  Only that prefix is normalized.

Everything else - every other field, Struct value, header, error code, and
error text - is compared exactly as encoded() would.
"""

from google.protobuf import text_format

from common.support import encoded

GO_PROTO_ERROR_PREFIX_NBSP = "proto: "
GO_PROTO_ERROR_PREFIX = "proto: "


def _override_type():
    # Imported lazily: the generated modules are on sys.path only after load_protos().
    from envoy.extensions.common.ratelimit.v3 import ratelimit_pb2
    return ratelimit_pb2.RateLimitDescriptor.RateLimitOverride


def parse_limit_text(text):
    """Serialized RateLimitOverride for strictly valid text, else None."""
    override = _override_type()()
    try:
        text_format.Parse(text, override)
    except text_format.ParseError:
        return None
    return override.SerializeToString(deterministic=True)


def canonical_error_details(details):
    return details.replace(GO_PROTO_ERROR_PREFIX_NBSP, GO_PROTO_ERROR_PREFIX)


def comparable(result):
    """Comparison key for a call result: (exact bytes, parsed descriptor limits).

    A parsed limit's string is blanked in the bytes and its message is listed
    separately with its descriptor index, so it can never equal a raw string.
    """
    if isinstance(result, tuple):
        code, details = result
        return encoded((code, canonical_error_details(details))), ()
    if isinstance(result, bytes) or not result.HasField("dynamic_metadata"):
        return encoded(result), ()
    # Look up map keys with `in` first: indexing a missing key would insert it.
    fields = result.dynamic_metadata.fields
    if "descriptors" not in fields or not fields["descriptors"].HasField("list_value"):
        return encoded(result), ()
    copy = type(result)()
    copy.CopyFrom(result)
    parsed = []
    for index, item in enumerate(copy.dynamic_metadata.fields["descriptors"].list_value.values):
        if not item.HasField("struct_value") or "limit" not in item.struct_value.fields:
            continue
        limit = item.struct_value.fields["limit"]
        if not limit.HasField("string_value"):
            continue
        message = parse_limit_text(limit.string_value)
        if message is not None:
            limit.string_value = ""
            parsed.append((index, message))
    return encoded(copy), tuple(parsed)
