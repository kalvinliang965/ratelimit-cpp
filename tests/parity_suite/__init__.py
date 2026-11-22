"""Rate-limit parity test suite: scenarios, the gRPC server fixture, and the runner.

scenarios.load_protos() must run before the fixtures are used; it owns the single
generated rls_pb2 module (scenarios.rls) that the other modules read.
"""
