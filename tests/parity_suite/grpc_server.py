"""A C++ or Go rate-limit gRPC server with private Redis instances."""

import json
import os
import subprocess
import time

import grpc

from common.support import free_port, redis_command
from . import scenarios


class Server:
    def __init__(self, binary, directory, config, settings):
        self.binary = binary
        self.directory = directory
        self.config_dir = directory / "runtime" / "ratelimit" / "config"
        self.config_dir.mkdir(parents=True)
        self.write_config(config)
        self.processes = []
        self.logs = []
        self.channel = None
        self.redis_ports = []
        self.port = free_port()
        self.settings = settings

    def write_config(self, config):
        # JSON is a YAML subset and preserves scalar types without extra packages.
        configs = config if isinstance(config, list) else [config]
        for index, item in enumerate(configs):
            temporary = self.config_dir / (".pending%d" % index)
            temporary.write_text(json.dumps(item))
            temporary.replace(self.config_dir / ("policy%d.yaml" % index))

    def launch(self, command, name, env=None):
        output = open(self.directory / (name + ".log"), "wb")
        self.logs.append(output)
        process = subprocess.Popen(command, env=env, stdout=output, stderr=output)
        self.processes.append(process)
        return process

    def start_redis(self):
        port = free_port()
        self.launch(["redis-server", "--bind", "127.0.0.1", "--port", str(port),
                     "--save", "", "--appendonly", "no", "--loglevel", "warning"],
                    "redis%d" % len(self.redis_ports))
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                if redis_command(port, "PING") == "PONG":
                    self.redis_ports.append(port)
                    return port
            except OSError:
                time.sleep(0.025)
        raise AssertionError("Redis did not become ready")

    def start(self):
        redis_port = self.start_redis()
        # Prevent developer-shell application settings from affecting fixtures.
        prefixes = ("REDIS_", "RUNTIME_", "LIMIT_", "GRPC_", "CONFIG_", "MEMCACHE_",
                    "STATSD_", "TRACING_", "PROMETHEUS_")
        env = {k: v for k, v in os.environ.items() if not k.startswith(prefixes)}
        env.update({
            "RUNTIME_ROOT": str(self.directory / "runtime"),
            "RUNTIME_SUBDIRECTORY": "ratelimit", "RUNTIME_APPDIRECTORY": "config",
            "RUNTIME_WATCH_ROOT": "false", "RUNTIME_IGNOREDOTFILES": "true",
            "GRPC_HOST": "127.0.0.1", "GRPC_PORT": str(self.port),
            "HOST": "127.0.0.1", "PORT": str(free_port()),
            "DEBUG_HOST": "127.0.0.1", "DEBUG_PORT": str(free_port()),
            "REDIS_SOCKET_TYPE": "tcp", "REDIS_URL": "127.0.0.1:%d" % redis_port,
            "REDIS_TYPE": "SINGLE", "REDIS_TIMEOUT": "2s",
            "REDIS_STARTUP_MAX_ELAPSED_TIME": "5s", "REDIS_PERSECOND": "false",
            "BACKEND_TYPE": "redis", "EXPIRATION_JITTER_MAX_SECONDS": "0",
            "LOCAL_CACHE_SIZE_IN_BYTES": "0", "CACHE_KEY_PREFIX": "",
            "NEAR_LIMIT_RATIO": "0.8", "STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT": "false",
            "USE_CALENDAR_MONTH_RATE_LIMIT": "false", "USE_STATSD": "false",
            "USE_DOG_STATSD": "false", "USE_PROMETHEUS": "false",
            "DISABLE_STATS": "true", "TRACING_ENABLED": "false", "LOG_LEVEL": "error",
            "SHADOW_MODE": "false", "QUOTA_MODE": "false", "RESPONSE_DYNAMIC_METADATA": "false",
            "MERGE_DOMAIN_CONFIG": "false", "CONFIG_TYPE": "FILE",
            "FORCE_START_WITHOUT_INITIAL_CONFIG": "false",
        })
        env.update(self.settings)
        if env.get("REDIS_PERSECOND") == "true":
            env.update(REDIS_PERSECOND_SOCKET_TYPE="tcp",
                       REDIS_PERSECOND_URL="127.0.0.1:%d" % self.start_redis())
        process = self.launch([self.binary], "server", env)
        self.channel = grpc.insecure_channel("127.0.0.1:%d" % self.port)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("Server exited with code %s" % process.returncode)
            try:
                grpc.channel_ready_future(self.channel).result(timeout=0.2)
                self.rpc = self.channel.unary_unary(
                    "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
                    request_serializer=scenarios.rls.RateLimitRequest.SerializeToString,
                    response_deserializer=scenarios.rls.RateLimitResponse.FromString)
                # Deliberately bypass protobuf parsing in the client so the
                # server, rather than Python's message constructor, receives
                # malformed gRPC payloads.
                self.raw_rpc = self.channel.unary_unary(
                    "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
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

    def snapshot(self):
        return [dict((key, redis_command(port, "GET", key))
                     for key in sorted(redis_command(port, "KEYS", "*")))
                for port in self.redis_ports]

    def close(self):
        if self.channel:
            self.channel.close()
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for output in self.logs:
            output.close()
