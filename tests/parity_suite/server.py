"""Shared infrastructure for one rate-limit implementation under test.

A Server owns a private directory with the policy files, its own Redis
instance(s), the service's environment, the processes it launches, and their
logs.  Subclasses add a transport: start() launches the service and waits until
it serves, and call(request) sends one RateLimitRequest (see grpc_server.py).
"""

import json
import os
import subprocess
import time

from common.support import free_port, redis_command


class Server:
    def __init__(self, binary, directory, config, settings):
        self.binary = binary
        self.directory = directory
        self.config_dir = directory / "runtime" / "ratelimit" / "config"
        self.config_dir.mkdir(parents=True)
        self.write_config(config)
        self.processes = []
        self.logs = []
        self.redis_ports = []
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

    def prepare(self, transport_env):
        """Start this server's Redis and return the service environment.

        transport_env holds the transport's own variables (ports, addresses).
        The scenario's settings are applied after it, as before, and a second
        Redis is started when they enable REDIS_PERSECOND.
        """
        redis_port = self.start_redis()
        # Prevent developer-shell application settings from affecting fixtures.
        prefixes = ("REDIS_", "RUNTIME_", "LIMIT_", "GRPC_", "CONFIG_", "MEMCACHE_",
                    "STATSD_", "TRACING_", "PROMETHEUS_")
        env = {k: v for k, v in os.environ.items() if not k.startswith(prefixes)}
        env.update({
            "RUNTIME_ROOT": str(self.directory / "runtime"),
            "RUNTIME_SUBDIRECTORY": "ratelimit", "RUNTIME_APPDIRECTORY": "config",
            "RUNTIME_WATCH_ROOT": "false", "RUNTIME_IGNOREDOTFILES": "true",
        })
        env.update(transport_env)
        env.update({
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
        return env

    def snapshot(self):
        return [dict((key, redis_command(port, "GET", key))
                     for key in sorted(redis_command(port, "KEYS", "*")))
                for port in self.redis_ports]

    def close(self):
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
