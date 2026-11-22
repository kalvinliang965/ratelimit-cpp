"""Check the DPO UDP socket client against an independent model of its packets.

The expected bytes come from this file, not from the C++ codec:
  * LAYOUT is the packed ratelimitHeader, derived by hand from ratelimit.dpo.
    With --dpo-root it is also compared with ratelimit.dpo itself and with the
    generated ratelimit_header.h (its phit bit ranges, packed field order, and
    get_length()).
  * encode_request_payload()/encode_response_payload() follow the documented
    xrpc payload layout in src/xrpc_conversion.cc.
A mock UDP peer captures the packet that the real client executable sends and
answers with controlled response packets; the client's --json and text output
are checked against hand-written expectations.
"""

import json
from pathlib import Path
import re
import socket
import struct
import subprocess
import sys
import threading

HEADER_SIZE = 129
MAX_PACKET = 1500
MAX_PAYLOAD = MAX_PACKET - HEADER_SIZE
# The DPO header follows Ethernet (14), IPv4 (20) and UDP (8) headers in phit 0.
HEADER_START_BIT = (14 + 20 + 8) * 8

# (field, byte offset, byte size).  Two framework bytes precede the message
# fields of ratelimit.dpo; NET_ADDR is 12 bytes, an enum 1 byte, and N bits are
# N/8 bytes.  All multi-byte integers are little-endian.
LAYOUT = [
    ("msg_type", 0, 1), ("internal_msg_type", 1, 1), ("client_addr", 2, 12),
    ("client_xid", 14, 8), ("client_timestamp", 22, 8), ("target_stage", 30, 1),
    ("overall_code", 31, 1), ("request_fast_path_valid", 32, 1),
    ("request_domain_len", 33, 1), ("request_domain", 34, 16),
    ("request_entry_key_len", 50, 1), ("request_entry_key", 51, 16),
    ("request_entry_value_len", 67, 1), ("request_entry_value", 68, 16),
    ("request_override_unit", 84, 1), ("request_override_requests_per_unit", 85, 4),
    ("response_fast_path_valid", 89, 1), ("response_status_code", 90, 1),
    ("response_current_limit_name_len", 91, 1), ("response_current_limit_name", 92, 16),
    ("response_current_limit_requests_per_unit", 108, 4), ("response_current_limit_unit", 112, 1),
    ("response_limit_remaining", 113, 4), ("response_duration_until_reset_seconds", 117, 8),
    ("response_duration_until_reset_nanos", 125, 4),
]
OFF = {name: (offset, size) for name, offset, size in LAYOUT}
UNITS = {"UNKNOWN": 0, "SECOND": 1, "MINUTE": 2, "HOUR": 3, "DAY": 4, "MONTH": 5, "YEAR": 6}
CODES = {"UNKNOWN": 0, "OK": 1, "OVER_LIMIT": 2}
MSG_REQUEST, MSG_RESPONSE = 0, 1
XID, TIMESTAMP, TARGET_STAGE = 1, 0, 1  # Fixed by the client.

passed = 0


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def ok(name):
    global passed
    passed += 1
    print("PASS", name, flush=True)


# ---------------------------------------------------------------- layout ----

def check_layout_is_contiguous():
    position = 0
    for name, offset, size in LAYOUT:
        check(offset == position, "%s starts at %d, expected %d" % (name, offset, position))
        position += size
    check(position == HEADER_SIZE, "layout covers %d bytes" % position)
    ok("layout_table_contiguous_129_bytes")


def layout_from_dpo(path):
    text = path.read_text()
    enums = set(re.findall(r"^\s*enum\s+(\w+)", text, re.M))
    bodies = dict(re.findall(r"message\s+(\w+)\s*\{(.*?)\}", text, re.S))
    check(bodies["RateLimitRequest"] == bodies["RateLimitResponse"],
          "request and response messages must share one header")
    fields, offset = [("msg_type", 0, 1), ("internal_msg_type", 1, 1)], 2
    for name, kind in re.findall(r"(\w+)\s*:\s*(\w+)\s*;", bodies["RateLimitRequest"]):
        bits = 96 if kind == "NET_ADDR" else 8 if kind in enums else int(kind)
        check(bits % 8 == 0, "%s is not byte aligned" % name)
        fields.append((name, offset, bits // 8))
        offset += bits // 8
    return fields


def generated_name(name):
    # The DPO compiler suffixes enum-typed fields with the enum name.
    return re.sub(r"_RateLimitCode$", "", name)


def layout_from_generated_phits(text):
    body = text[text.index("void parse("):text.index("void populate(")]
    parts = {}
    for case, block in re.findall(r"case (\d+):(.*?)break;", body, re.S):
        for name, high, low in re.findall(r"(\w+)(?:\(\d+, \d+\))? = phit\.range\((\d+), (\d+)\);", block):
            parts.setdefault(generated_name(name), []).append((int(case) * 512 + int(low), int(high) - int(low) + 1))
    base = min(start for pieces in parts.values() for start, _ in pieces)
    check(base == HEADER_START_BIT, "generated header starts at bit %d, expected %d" % (base, HEADER_START_BIT))
    fields = []
    for name, pieces in parts.items():
        pieces.sort()
        for (start, bits), (next_start, _) in zip(pieces, pieces[1:]):
            check(start + bits == next_start, "%s is split non-contiguously" % name)
        fields.append((name, (pieces[0][0] - base) // 8, sum(bits for _, bits in pieces) // 8))
    return sorted(fields, key=lambda field: field[1])


def layout_from_generated_struct(text):
    body = text[text.index("struct ratelimitHeader {"):text.index("ratelimitHeader& operator=")]
    fields, offset = [], 0
    for kind, name in re.findall(r"^\s*(ap_uint<\d+>|NetAddrTuple)\s+(\w+);", body, re.M):
        size = 12 if kind == "NetAddrTuple" else int(kind[8:-1]) // 8
        fields.append((generated_name(name), offset, size))
        offset += size
    return fields


def check_against_dpo(root):
    dpo = root / "protocols/ratelimit/ratelimit.dpo"
    header = root / "software/src/protocols/ratelimit/ratelimit_header.h"
    packet_h = root / "software/src/protocols/ratelimit/ratelimit_packet.h"
    check(layout_from_dpo(dpo) == LAYOUT, "ratelimit.dpo layout differs from the client layout")
    ok("layout_matches_ratelimit_dpo")
    text = header.read_text()
    check(layout_from_generated_phits(text) == LAYOUT, "generated phit ranges differ from the client layout")
    ok("layout_matches_generated_phit_ranges")
    check(layout_from_generated_struct(text) == LAYOUT, "generated packed struct differs from the client layout")
    ok("layout_matches_generated_packed_struct")
    length = int(re.search(r"get_length\(\)\s*\{\s*return (\d+);", text).group(1))
    check(length == HEADER_SIZE, "generated get_length() is %d" % length)
    server_sizes = dict(re.findall(r"(kHeaderSize|kMaxPacketBytes)\s*=\s*(\d+);", packet_h.read_text()))
    check(server_sizes == {"kHeaderSize": str(HEADER_SIZE), "kMaxPacketBytes": str(MAX_PACKET)},
          "DPO ratelimit_packet.h sizes differ: %r" % server_sizes)
    ok("sizes_match_generated_length_and_dpo_ratelimit_packet_h")


# --------------------------------------------------- independent encoders ----

def string8(value):
    data = value.encode() if isinstance(value, str) else value
    check(len(data) <= 255, "string too long for the request format")
    return bytes([len(data)]) + data


def blob16(value):
    data = value.encode() if isinstance(value, str) else value
    return struct.pack("<H", len(data)) + data


def encode_request_payload(request):
    descriptors = request["descriptors"]
    out = string8(request["domain"]) + struct.pack("<IB", request.get("hits_addend", 0), len(descriptors))
    for descriptor in descriptors:
        limit, hits = descriptor.get("limit"), descriptor.get("hits_addend")
        flags = (1 if limit else 0) | (2 if hits is not None else 0) | (4 if descriptor.get("is_negative_hits") else 0)
        out += struct.pack("<BB", len(descriptor["entries"]), flags)
        if limit:
            out += struct.pack("<IB", limit["requests_per_unit"], UNITS[limit["unit"]])
        if hits is not None:
            out += struct.pack("<Q", hits)
        for entry in descriptor["entries"]:
            out += string8(entry["key"]) + string8(entry["value"])
    return out


def put(header, field, value):
    offset, size = OFF[field]
    if isinstance(value, (bytes, str)):
        data = value.encode() if isinstance(value, str) else value
        check(len(data) <= size, "%s overflows" % field)
        header[offset:offset + len(data)] = data
    else:
        header[offset:offset + size] = value.to_bytes(size, "little", signed=value < 0)


def fast_path_eligible(request):
    descriptors = request["descriptors"]
    if len(descriptors) != 1 or len(request["domain"].encode()) > 16:
        return False
    descriptor = descriptors[0]
    limit = descriptor.get("limit")
    if len(descriptor["entries"]) != 1 or not limit or descriptor.get("is_negative_hits"):
        return False
    entry = descriptor["entries"][0]
    return (1 <= UNITS[limit["unit"]] <= 6 and len(entry["key"].encode()) <= 16
            and len(entry["value"].encode()) <= 16)


def expected_request_packet(request):
    header = bytearray(HEADER_SIZE)
    put(header, "msg_type", MSG_REQUEST)
    put(header, "client_xid", XID)
    put(header, "client_timestamp", TIMESTAMP)
    put(header, "target_stage", TARGET_STAGE)
    if fast_path_eligible(request):
        descriptor = request["descriptors"][0]
        entry = descriptor["entries"][0]
        put(header, "request_fast_path_valid", 1)
        put(header, "request_domain_len", len(request["domain"].encode()))
        put(header, "request_domain", request["domain"])
        put(header, "request_entry_key_len", len(entry["key"].encode()))
        put(header, "request_entry_key", entry["key"])
        put(header, "request_entry_value_len", len(entry["value"].encode()))
        put(header, "request_entry_value", entry["value"])
        put(header, "request_override_unit", UNITS[descriptor["limit"]["unit"]])
        put(header, "request_override_requests_per_unit", descriptor["limit"]["requests_per_unit"])
    return bytes(header) + encode_request_payload(request)


def reply_header(request_packet, overall_code, fast=None):
    header = bytearray(HEADER_SIZE)
    put(header, "msg_type", MSG_RESPONSE)
    header[14:30] = request_packet[14:30]  # Echo client_xid and client_timestamp.
    put(header, "overall_code", overall_code)
    if fast:
        put(header, "response_fast_path_valid", 1)
        put(header, "response_status_code", fast["status_code"])
        put(header, "response_current_limit_name_len", len(fast["name"]))
        put(header, "response_current_limit_name", fast["name"])
        put(header, "response_current_limit_requests_per_unit", fast["requests_per_unit"])
        put(header, "response_current_limit_unit", fast["unit"])
        put(header, "response_limit_remaining", fast["remaining"])
        put(header, "response_duration_until_reset_seconds", fast["seconds"])
        put(header, "response_duration_until_reset_nanos", fast["nanos"])
    return bytes(header)


def encode_quota(quota):
    out = struct.pack("<I", quota["requests"]) + string8(quota["id"])
    if "valid_until" in quota:
        return out + b"\x01" + struct.pack("<qi", *quota["valid_until"])
    return out + b"\x00"


def encode_response_payload(statuses, response_headers, request_headers, body, metadata=None, quota=None):
    out = bytes([len(statuses)])
    for status in statuses:
        flags = (1 if "limit" in status else 0) | (2 if "duration" in status else 0) | (4 if "quota" in status else 0)
        out += struct.pack("<BIB", status["code"], status["remaining"], flags)
        if "limit" in status:
            name, requests, unit = status["limit"]
            out += string8(name) + struct.pack("<IB", requests, unit)
        if "duration" in status:
            out += struct.pack("<qi", *status["duration"])
        if "quota" in status:
            out += encode_quota(status["quota"])
    out += bytes([(1 if metadata is not None else 0) | (2 if quota is not None else 0)])
    for headers in (response_headers, request_headers):
        out += bytes([len(headers)]) + b"".join(blob16(k) + blob16(v) + blob16(r) for k, v, r in headers)
    out += blob16(body)
    if metadata is not None:
        out += blob16(json.dumps(metadata, separators=(",", ":")))
    if quota is not None:
        out += encode_quota(quota)
    return out


# ------------------------------------------------------- mock UDP server ----

def exchange(client, request, reply=None, extra_args=(), peer_timeout=5.0):
    """Run the client once; capture its datagram and optionally answer it."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as peer:
        peer.bind(("127.0.0.1", 0))
        peer.settimeout(peer_timeout)
        captured = {}

        def serve():
            try:
                packet, address = peer.recvfrom(65535)
            except socket.timeout:
                return
            captured["packet"] = packet
            if reply is not None:
                peer.sendto(reply(packet), address)

        thread = threading.Thread(target=serve)
        thread.start()
        result = subprocess.run(
            [client, "--port", str(peer.getsockname()[1]), *extra_args, "-"],
            input=json.dumps(request), capture_output=True, text=True, timeout=15)
        thread.join()
    return result, captured.get("packet")


def check_sent(name, client, request):
    expected = expected_request_packet(request)
    result, packet = exchange(client, request, peer_timeout=5.0)
    check(packet is not None, "%s: no packet captured; stderr=%s" % (name, result.stderr))
    check(len(packet) == len(expected), "%s: packet is %d bytes, expected %d" % (name, len(packet), len(expected)))
    if packet != expected:
        diff = [i for i in range(len(packet)) if packet[i] != expected[i]]
        raise AssertionError("%s: first differing byte %d (got %r, expected %r)" %
                             (name, diff[0], packet[diff[0]:diff[0] + 8], expected[diff[0]:diff[0] + 8]))
    check(result.returncode == 1 and "Resource temporarily unavailable" in result.stderr,
          "%s: unanswered client should time out; rc=%s stderr=%s" % (name, result.returncode, result.stderr))
    ok(name)
    return packet


def single(domain="rl", key="user", value="alice", limit=(5, "MINUTE"), **extra):
    descriptor = {"entries": [{"key": key, "value": value}], **extra}
    if limit:
        descriptor["limit"] = {"requests_per_unit": limit[0], "unit": limit[1]}
    return {"domain": domain, "descriptors": [descriptor]}


def boundary_request(payload_size):
    # domain 200 bytes, two maximal entries, one entry sized to hit payload_size.
    request = {"domain": "d" * 200, "descriptors": [{"entries": [
        {"key": "k" * 255, "value": "v" * 255}, {"key": "K" * 255, "value": "V" * 255},
        {"key": "x", "value": ""}]}]}
    fill = payload_size - len(encode_request_payload(request))
    request["descriptors"][0]["entries"][2]["value"] = "y" * fill
    check(len(encode_request_payload(request)) == payload_size, "boundary payload sizing")
    return request


def test_request_packets(client, dpo_root):
    check_sent("fast_path_request_exact_bytes", client, single())
    check_sent("fast_path_16_byte_strings_exact_bytes", client,
               single(domain="d" * 16, key="k" * 16, value="v" * 16, limit=(0xFFFFFFFF, "YEAR")))
    check_sent("domain_17_bytes_is_not_fast_path", client, single(domain="d" * 17))
    check_sent("negative_hits_is_not_fast_path", client, single(is_negative_hits=True, hits_addend=3))
    check_sent("no_limit_is_not_fast_path", client, single(limit=None))
    check_sent("multi_descriptor_optional_fields_exact_bytes", client, {
        "domain": "messaging", "hits_addend": 7, "descriptors": [
            {"entries": [{"key": "a", "value": "x\u0000y"}, {"key": "b", "value": "é"}],
             "limit": {"requests_per_unit": 1, "unit": "SECOND"}, "hits_addend": 0},
            {"entries": [], "is_negative_hits": True},
            {"entries": [{"key": "c", "value": ""}], "hits_addend": 2 ** 64 - 1}]})
    if dpo_root:
        demo = json.loads((dpo_root / "config/ratelimit/request.json").read_text())
        check_sent("dpo_demo_request_json_exact_bytes", client, demo)
    packet = check_sent("payload_exactly_fills_1500_byte_packet", client, boundary_request(MAX_PAYLOAD))
    check(len(packet) == MAX_PACKET, "boundary packet should be exactly 1500 bytes")
    result, packet = exchange(client, boundary_request(MAX_PAYLOAD + 1), peer_timeout=1.0)
    check(packet is None and result.returncode == 1 and "too large" in result.stderr,
          "one byte over the payload budget must fail before sending: %r" % (result,))
    ok("payload_one_byte_over_budget_is_rejected_before_send")


def test_responses(client, generated):
    request = single()
    fast = {"status_code": 2, "name": "cfg", "requests_per_unit": 5, "unit": 2, "remaining": 0,
            "seconds": 12, "nanos": 500}
    expected_fast = {"overall_code": "OVER_LIMIT", "statuses": [{
        "code": "OVER_LIMIT", "current_limit": {"name": "cfg", "requests_per_unit": 5, "unit": "MINUTE"},
        "limit_remaining": 0, "duration_until_reset": "12.000000500s"}],
        "response_headers_to_add": [], "request_headers_to_add": [], "raw_body": ""}
    outputs = []

    result, _ = exchange(client, request, lambda p: reply_header(p, 2, fast), ["--json"])
    check(result.returncode == 0, "fast-path reply: %r" % (result,))
    check(json.loads(result.stdout) == expected_fast, "fast-path JSON: %s" % result.stdout)
    outputs.append(result.stdout)
    ok("fast_path_response_json")

    result, _ = exchange(client, request, lambda p: reply_header(p, 2, fast))
    check(result.returncode == 0 and result.stdout == (
        "RateLimitResponse: xid=1, overall_code=OVER_LIMIT, statuses=1\n"
        "  status[0]: code=OVER_LIMIT, limit_remaining=0\n"
        "    current_limit: name=\"cfg\", requests_per_unit=5, unit=MINUTE\n"
        "    duration_until_reset: seconds=12, nanos=500\n"
        "    quota: absent\n"
        "  response_headers_to_add: count=0\n"
        "  request_headers_to_add: count=0\n"
        "  raw_body: \"\"\n"
        "  dynamic_metadata: absent\n"
        "  response_quota: absent\n"), "default text output changed: %r" % result.stdout)
    ok("fast_path_response_default_text_output_unchanged")

    two = {"domain": "rl", "descriptors": [{"entries": [{"key": "a", "value": "1"}]},
                                           {"entries": [{"key": "b", "value": "2"}]}]}
    payload = encode_response_payload(
        statuses=[{"code": 1, "remaining": 4, "limit": ("per_minute", 5, 2), "duration": (59, 250000000),
                   "quota": {"requests": 3, "id": "q1", "valid_until": (1767225600, 5)}},
                  {"code": 1, "remaining": 0, "duration": (-1, -500000000)}],
        response_headers=[("x-ratelimit-limit", "5", b"")],
        request_headers=[("x-req", "v", b"\x00\xff")],
        body=b"a\x00b", metadata={"route": {"n": 1}}, quota={"requests": 9, "id": "global"})
    expected_full = {"overall_code": "OK", "statuses": [
        {"code": "OK", "current_limit": {"name": "per_minute", "requests_per_unit": 5, "unit": "MINUTE"},
         "limit_remaining": 4, "duration_until_reset": "59.250000000s",
         "quota": {"requests": 3, "valid_until": "2026-01-01T00:00:00.000000005Z", "id": "q1"}},
        {"code": "OK", "limit_remaining": 0, "duration_until_reset": "-1.500000000s"}],
        "response_headers_to_add": [{"key": "x-ratelimit-limit", "value": "5", "raw_value": ""}],
        "request_headers_to_add": [{"key": "x-req", "value": "v", "raw_value": "AP8="}],
        "raw_body": "YQBi", "dynamic_metadata": {"route": {"n": 1}}, "quota": {"requests": 9, "id": "global"}}
    result, _ = exchange(client, two, lambda p: reply_header(p, 1) + payload, ["--json"])
    check(result.returncode == 0, "payload reply: %r" % (result,))
    check(json.loads(result.stdout) == expected_full, "payload JSON: %s" % result.stdout)
    outputs.append(result.stdout)
    ok("payload_response_all_fields_json")

    empty = encode_response_payload([], [], [], b"")
    check(len(empty) == 6, "an empty response payload is 6 bytes")
    result, _ = exchange(client, request, lambda p: reply_header(p, 0) + empty, ["--json"])
    check(result.returncode == 1 and json.loads(result.stdout) == {
        "overall_code": "UNKNOWN", "statuses": [], "response_headers_to_add": [],
        "request_headers_to_add": [], "raw_body": ""}, "UNKNOWN reply: %r" % (result,))
    outputs.append(result.stdout)
    ok("unknown_response_json_and_exit_code_1")

    def wrong_xid(packet):
        header = bytearray(reply_header(packet, 2, fast))
        header[14] ^= 0xFF
        return bytes(header)
    result, _ = exchange(client, request, wrong_xid, ["--json"])
    check(result.returncode == 1 and result.stdout == "" and "transaction ID" in result.stderr,
          "mismatched xid: %r" % (result,))
    ok("mismatched_transaction_id_rejected")

    result, _ = exchange(client, request, lambda p: reply_header(p, 1) + payload, ["--json"])
    check(result.returncode == 1 and result.stdout == "" and "status count" in result.stderr,
          "status/descriptor count mismatch: %r" % (result,))
    ok("status_count_mismatch_rejected")

    if generated:
        sys.path.insert(0, str(Path(generated).resolve()))
        from google.protobuf import json_format
        from envoy.service.ratelimit.v3 import rls_pb2
        for output in outputs:
            json_format.ParseDict(json.loads(output), rls_pb2.RateLimitResponse())
        ok("json_output_parses_as_envoy_RateLimitResponse")


def test_cli(client):
    result = subprocess.run([client, "--help"], capture_output=True, text=True)
    check(result.returncode == 0 and "usage:" in result.stdout, "--help: %r" % (result,))
    for args, message in [(["--port", "0", "-"], "invalid port"), (["--port", "x", "-"], "invalid port"),
                          (["--host", "localhost", "-"], "invalid IPv4"), (["a.json", "b.json"], "usage:"),
                          (["--bogus"], "usage:")]:
        result = subprocess.run([client, *args], input=json.dumps(single()), capture_output=True, text=True)
        check(result.returncode == 1 and message in result.stderr, "%s: %r" % (args, result))
    ok("cli_arguments_validated")
