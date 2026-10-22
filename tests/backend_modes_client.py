#!/usr/bin/env python3
"""Make one paired RateLimit RPC with the pinned Docker gRPC client.

The backend-mode driver launches this program in the same grpcio 1.62.3 image
used by the Docker validation.  Keeping the complete gRPC result comparison in
this process avoids treating a client-library metadata presentation difference
as a service difference.  It deliberately compares all returned fields,
including metadata, without filtering or normalization.
"""

import argparse
import base64
import concurrent.futures
import json
import threading

import grpc


METHOD = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit"


def metadata(values):
    if values is None:
        return ()
    return tuple((key, value if isinstance(value, str) else base64.b64encode(value).decode())
                 for key, value in values)


def call(target, request, compression):
    channel = grpc.insecure_channel(target)
    try:
        rpc = channel.unary_unary(
            METHOD,
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )
        try:
            options = {}
            if compression == "gzip":
                options["compression"] = grpc.Compression.Gzip
            response, rpc_call = rpc.with_call(request, timeout=8, **options)
            return {
                "kind": "response",
                "payload": base64.b64encode(response).decode(),
                "initial_metadata": metadata(rpc_call.initial_metadata()),
                "trailing_metadata": metadata(rpc_call.trailing_metadata()),
            }
        except grpc.RpcError as error:
            return {
                "kind": "error",
                "code": error.code().name,
                "details": error.details(),
                "initial_metadata": metadata(error.initial_metadata()),
                "trailing_metadata": metadata(error.trailing_metadata()),
            }
    finally:
        channel.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp-target", required=True)
    parser.add_argument("--go-target", required=True)
    parser.add_argument("--request-base64", required=True)
    parser.add_argument("--compression", choices=("", "gzip"), default="")
    args = parser.parse_args()
    request = base64.b64decode(args.request_base64, validate=True)

    # Dispatch through one gate so time-window reset durations are sampled at
    # the same instant while keeping the two services and Redis fixtures apart.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        gate = threading.Event()
        futures = [
            executor.submit(lambda target=target: (gate.wait(), call(target, request, args.compression))[1])
            for target in (args.cpp_target, args.go_target)
        ]
        gate.set()
        cpp, go = [future.result() for future in futures]

    result = {
        "client_grpcio": grpc.__version__,
        "cpp": cpp,
        "go": go,
        "equal": cpp == go,
    }
    print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    if not result["equal"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
