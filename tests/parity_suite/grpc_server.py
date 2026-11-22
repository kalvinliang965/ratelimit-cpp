"""A C++ or Go rate-limit service reached over gRPC (shared parts: server.py)."""

import time

import grpc

from common.support import free_port
from . import scenarios
from .server import Server as _BaseServer

METHOD = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit"


class GrpcServer(_BaseServer):
    def __init__(self, binary, directory, config, settings):
        super().__init__(binary, directory, config, settings)
        self.channel = None
        self.port = free_port()

    def start(self):
        env = self.prepare({
            "GRPC_HOST": "127.0.0.1", "GRPC_PORT": str(self.port),
            "HOST": "127.0.0.1", "PORT": str(free_port()),
            "DEBUG_HOST": "127.0.0.1", "DEBUG_PORT": str(free_port()),
        })
        process = self.launch([self.binary], "server", env)
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % self.port)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("Server exited with code %s" % process.returncode)
            try:
                grpc.channel_ready_future(self.channel).result(timeout=0.2)
                self.rpc = self.channel.unary_unary(
                    METHOD,
                    request_serializer=scenarios.rls.RateLimitRequest.SerializeToString,
                    response_deserializer=scenarios.rls.RateLimitResponse.FromString)
                # Deliberately bypass protobuf parsing in the client so the
                # server, rather than Python's message constructor, receives
                # malformed gRPC payloads.
                self.raw_rpc = self.channel.unary_unary(
                    METHOD,
                    request_serializer=lambda body: body,
                    response_deserializer=lambda body: body)
                return self
            except grpc.FutureTimeoutError:
                pass
        raise AssertionError("gRPC server did not become ready")

    def call(self, request):
        try:
            return self.rpc(request, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())

    def call_raw(self, body):
        try:
            return self.raw_rpc(body, timeout=5)
        except grpc.RpcError as error:
            return (error.code().name, error.details())

    def close(self):
        if self.channel:
            self.channel.close()
        super().close()


# runner.py and load.py use this name for the gRPC server.
Server = GrpcServer
