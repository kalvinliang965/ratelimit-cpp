# Rate-limit wire codec

The codec converts between rate-limit C++ types and a bounded binary payload.
It does not include gRPC, Protobuf, or generated DPO headers.

- `include/xrpc_conversion.h` declares the interface and payload limits.
- `src/xrpc_conversion.cc` implements the encoder and decoder.
- `tests/xrpc_conversion_test.cc` tests known bytes, optional fields, malformed
  input, binary data, and payload boundaries.
- `tests/xrpc_conversion_json.cc` provides JSON-to-C++ and wire round trips.
- `tests/fixtures/ratelimit_wire/` contains example JSON inputs.

From the repository root:

```sh
make test-wire
./build/xrpc-conversion-json --request
./build/xrpc-conversion-json --response
```

The JSON exercise writes its results to `build/ratelimit_wire_output/` and
compares parsed JSON values with the inputs. Pass a fixture path after the mode
to test another input. The fixture uses JSON text strings for raw body and
header values; the codec itself supports arbitrary bytes.

The response's `overall_code` belongs in the DPO header rather than the
payload. The demo passes it separately to `decode_response`, just as a DPO
handler must do. The 1,500-byte budget is for the DPO header and payload, not
the network MTU.

Recommended size constants are not hard limits: fields must fit their wire
length/count types and the aggregate payload budget. The existing DPO codec
enforces stricter limits for some inputs. Check interoperability before using
this codec in place of the DPO codec.

For linking from another C++ program, `make build/libxrpc_conversion.a` builds
a standalone static library. Include `xrpc_conversion.h` and link that library.
