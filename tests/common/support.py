"""Small helpers shared by the Python test suites."""

import socket


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def redis_command(port, *args):
    """Tiny RESP client, avoiding a Python redis dependency."""
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        data = [str(a).encode() for a in args]
        sock.sendall(b"*%d\r\n" % len(data) + b"".join(
            b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in data))
        stream = sock.makefile("rb")

        def read():
            line = stream.readline()
            if not line:
                raise RuntimeError("Redis disconnected")
            kind, value = line[:1], line[1:-2]
            if kind == b"-":
                raise RuntimeError(value.decode())
            if kind == b":":
                return int(value)
            if kind == b"$":
                length = int(value)
                if length < 0:
                    return None
                result = stream.read(length)
                stream.read(2)
                return result.decode()
            if kind == b"*":
                return [read() for _ in range(int(value))]
            return value.decode()

        return read()


def encoded(response):
    # Raw malformed-wire tests intentionally use a bytes deserializer so a
    # valid request with unknown fields can be compared at the exact gRPC
    # payload level too.
    if isinstance(response, bytes):
        return response
    return repr(response).encode() if isinstance(response, tuple) else response.SerializeToString(deterministic=True)
