"""C++ Redis connection-pool tests: idle connections closed by Redis (Linux only).

These check ratelimit-cpp's own pool, not Go parity: Go's radix pool may fail a
request that draws a dead pooled connection, which C++ avoids by checking an
idle connection before reusing it.

Observable state is used instead of sleeps:
  * Redis CLIENT LIST ids show which connections the service holds, so reuse,
    replacement and the REDIS_POOL_SIZE cap are visible.
  * After Redis closes connections, each test waits until the service
    process's own sockets to Redis have left ESTABLISHED (/proc/<pid>/net/tcp),
    so the service has really received the close before the next request.
"""

import concurrent.futures
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time

TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS))
import load  # noqa: E402  (ManagedServer: application and Redis process control)
from common import support  # noqa: E402
from parity_suite import scenarios  # noqa: E402

POOL_SIZE = 3
LIMIT = 1000000


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def wait_until(predicate, description, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError("timed out waiting for " + description)
        time.sleep(0.02)


# ----------------------------------------------------------- Redis admin ----

def resp_read(stream):
    line = stream.readline()
    kind, body = line[:1], line[1:-2]
    if kind == b"-":
        raise RuntimeError(body.decode())
    if kind == b"+":
        return body.decode()
    if kind == b":":
        return int(body)
    if kind == b"$":
        size = int(body)
        if size < 0:
            return None
        value = stream.read(size)
        stream.read(2)
        return value.decode()
    if kind == b"*":
        return [resp_read(stream) for _ in range(int(body))]
    raise RuntimeError("unexpected Redis reply %r" % line)


class Admin:
    """Redis commands from the test, over TCP or TLS."""

    def __init__(self, port, cafile=None):
        self.port, self.cafile = port, cafile

    def __call__(self, *args):
        raw = socket.create_connection(("127.0.0.1", self.port), timeout=3)
        if self.cafile:
            context = ssl.create_default_context(cafile=self.cafile)
            raw = context.wrap_socket(raw, server_hostname="127.0.0.1")
        with raw:
            data = [str(a).encode() for a in args]
            raw.sendall(b"*%d\r\n" % len(data) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in data))
            return resp_read(raw.makefile("rb"))

    def service_client_ids(self):
        """Ids of the service's connections (every client except this CLIENT LIST)."""
        ids = set()
        for line in self("CLIENT", "LIST").splitlines():
            fields = dict(item.split("=", 1) for item in line.split() if "=" in item)
            if fields.get("cmd") != "client|list":
                ids.add(int(fields["id"]))
        return ids


def sockets_to_port(pid, port):
    """TCP states of the process's sockets whose remote port is `port`."""
    inodes = set()
    for fd in os.listdir("/proc/%d/fd" % pid):
        try:
            target = os.readlink("/proc/%d/fd/%s" % (pid, fd))
        except OSError:
            continue
        if target.startswith("socket:["):
            inodes.add(target[8:-1])
    states = []
    for table in ("tcp", "tcp6"):
        for line in Path("/proc/%d/net/%s" % (pid, table)).read_text().splitlines()[1:]:
            fields = line.split()
            if fields[9] in inodes and int(fields[2].rsplit(":", 1)[1], 16) == port:
                states.append(fields[3])
    return states


def wait_service_saw_close(server, port):
    # 01 is TCP_ESTABLISHED; a socket whose peer closed moves to CLOSE_WAIT (08).
    wait_until(lambda: "01" not in sockets_to_port(server.app_process.pid, port),
               "the service's Redis sockets to leave ESTABLISHED")


# -------------------------------------------------------------- fixtures ----

class TlsServer(load.ManagedServer):
    """The C++ service talking to a TLS-only Redis."""

    def __init__(self, *args, cert, key, **kwargs):
        super().__init__(*args, **kwargs)
        self.cert, self.key = cert, key

    def start_redis(self):
        port = support.free_port()
        self.launch(["redis-server", "--bind", "127.0.0.1", "--port", "0", "--tls-port", str(port),
                     "--tls-cert-file", self.cert, "--tls-key-file", self.key,
                     "--tls-ca-cert-file", self.cert, "--tls-auth-clients", "no",
                     "--save", "", "--appendonly", "no", "--loglevel", "warning"],
                    "redis%d" % len(self.redis_ports))
        admin = Admin(port, self.cert)
        wait_until(lambda: self._ping(admin), "TLS Redis to answer PING")
        self.redis_ports.append(port)
        return port

    @staticmethod
    def _ping(admin):
        try:
            return admin("PING") == "PONG"
        except OSError:
            return False


def make_tls_material(directory):
    cert, key = str(directory / "redis.crt"), str(directory / "redis.key")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                    "-out", cert, "-days", "1", "-subj", "/CN=127.0.0.1",
                    "-addext", "subjectAltName=IP:127.0.0.1"],
                   check=True, capture_output=True)
    return cert, key


class Fixture:
    def __init__(self, args, name, tls=False):
        self.directory = Path(tempfile.mkdtemp(prefix="ratelimit-cpp-pool-%s-" % name))
        config = scenarios.policy(scenarios.rule("pool", LIMIT, "hour"))
        settings = {"REDIS_POOL_SIZE": str(POOL_SIZE)}
        if tls:
            cert, key = make_tls_material(self.directory)
            settings.update(REDIS_TLS="true", REDIS_TLS_CACERT=cert)
            self.server = TlsServer(args.cpp, self.directory / "cpp", config, settings, cert=cert, key=key)
        else:
            self.server = load.ManagedServer(args.cpp, self.directory / "cpp", config, settings)
        self.args, self.cert = args, cert if tls else None
        self.request = scenarios.request(scenarios.descriptor(("pool", "x")), hits=1)

    def __enter__(self):
        self.server.start()
        self.port = self.server.redis_ports[0]
        self.admin = Admin(self.port, self.cert)
        return self

    def __exit__(self, kind, value, traceback):
        self.server.close()
        if kind is None and not self.args.keep_logs:
            shutil.rmtree(self.directory)
        else:
            print("  Logs:", self.directory, flush=True)

    def ok(self, label):
        result = self.server.call(self.request)
        check(not isinstance(result, tuple) and result.overall_code == scenarios.rls.RateLimitResponse.OK,
              "%s: expected OK, got %r" % (label, result))
        return result

    def sequential(self, count, label):
        return [self.ok("%s #%d" % (label, i + 1)) for i in range(count)]

    def burst(self, calls, label, workers=12):
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda _: self.server.call(self.request), range(calls)))
        failures = [r for r in results if isinstance(r, tuple)]
        check(not failures, "%s: %d of %d concurrent requests failed, first %r" %
              (label, len(failures), calls, failures[:1]))
        return results

    def ids_within_pool(self, label):
        ids = self.admin.service_client_ids()
        check(1 <= len(ids) <= POOL_SIZE, "%s: service holds %d Redis connections, pool size %d: %r" %
              (label, len(ids), POOL_SIZE, ids))
        return ids

    def kill_idle(self):
        killed = self.admin("CLIENT", "KILL", "TYPE", "normal")  # SKIPME yes: not this connection
        check(killed >= 1, "CLIENT KILL closed no service connection")
        wait_service_saw_close(self.server, self.port)
        return killed


# ----------------------------------------------------------------- tests ----

def test_healthy_idle_reuse(fixture):
    """Healthy idle connections are reused: no reconnects across many requests."""
    fixture.burst(24, "warm-up burst")
    before = fixture.ids_within_pool("after warm-up")
    fixture.sequential(20, "sequential reuse")
    fixture.burst(24, "concurrent reuse")
    after = fixture.ids_within_pool("after reuse")
    check(after == before, "healthy connections were replaced: %r -> %r" % (before, after))


def test_idle_connections_closed_by_redis(fixture):
    """Every idle connection is closed by Redis; the next requests still succeed."""
    fixture.burst(24, "warm-up burst")
    before = fixture.ids_within_pool("after warm-up")
    killed = fixture.kill_idle()
    check(killed == len(before), "killed %d, expected %d" % (killed, len(before)))
    fixture.sequential(5, "after CLIENT KILL")
    after = fixture.ids_within_pool("after replacement")
    check(not after & before, "a closed connection is still in use: %r" % (after & before))


def test_redis_restart_while_idle(fixture):
    """Redis restarts while connections are idle; counters start from zero."""
    fixture.burst(24, "warm-up burst")
    fixture.ids_within_pool("after warm-up")
    fixture.server.restart_redis(fixture.port)
    wait_service_saw_close(fixture.server, fixture.port)
    results = fixture.sequential(5, "after Redis restart")
    # The restarted Redis is empty and no request failed after the restart,
    # so no unacknowledged INCRBY can be in the counter: it is exact.
    remaining = [r.statuses[0].limit_remaining for r in results]
    check(remaining == [LIMIT - n for n in range(1, 6)], "unexpected remaining counts %r" % remaining)
    fixture.ids_within_pool("after restart recovery")


def test_concurrent_requests_after_close(fixture):
    """Concurrent requests draw closed idle connections at once."""
    for round_ in range(3):
        fixture.burst(24, "round %d warm-up" % round_)
        before = fixture.ids_within_pool("round %d before" % round_)
        fixture.kill_idle()
        fixture.burst(36, "round %d after CLIENT KILL" % round_)
        after = fixture.ids_within_pool("round %d after" % round_)
        check(not after & before, "round %d: a closed connection is still in use" % round_)


def test_outage_then_recovery_keeps_capacity(fixture):
    """Failed checks and failed dials while Redis is down leave every pool slot usable."""
    fixture.burst(24, "warm-up burst")
    fixture.server.stop_redis(fixture.port)
    wait_service_saw_close(fixture.server, fixture.port)
    first = fixture.server.call(fixture.request)
    # The pooled connection was closed by the server and cannot be replaced:
    # the same error Go's radix pool reports for that request.
    check(first == ("UNKNOWN", "response returned from Conn: EOF"), "first outage request: %r" % (first,))
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        outage = list(pool.map(lambda _: fixture.server.call(fixture.request), range(2 * POOL_SIZE + 2)))
    check(all(isinstance(r, tuple) and r[0] == "UNKNOWN" for r in outage), "outage requests: %r" % outage[:2])
    fixture.server.restart_redis(fixture.port)
    # If a slot had leaked, POOL_SIZE concurrent callers could not all proceed.
    fixture.burst(36, "after Redis returns")
    fixture.ids_within_pool("after recovery")


def test_tls_healthy_idle_reuse(fixture):
    """Over TLS, idle connections are reused (TLS records are not mistaken for data)."""
    test_healthy_idle_reuse(fixture)


def test_tls_idle_connections_closed_by_redis(fixture):
    test_idle_connections_closed_by_redis(fixture)


TESTS_TO_RUN = [
    ("healthy_idle_reuse", test_healthy_idle_reuse, False),
    ("idle_connections_closed_by_redis", test_idle_connections_closed_by_redis, False),
    ("redis_restart_while_idle", test_redis_restart_while_idle, False),
    ("concurrent_requests_after_close", test_concurrent_requests_after_close, False),
    ("outage_then_recovery_keeps_capacity", test_outage_then_recovery_keeps_capacity, False),
    ("tls_healthy_idle_reuse", test_tls_healthy_idle_reuse, True),
    ("tls_idle_connections_closed_by_redis", test_tls_idle_connections_closed_by_redis, True),
]
