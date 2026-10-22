# A self-contained build.  The runtime intentionally uses the same Debian
# release as the build stage so the gRPC and Abseil shared libraries exactly
# match the binary that was compiled here.
FROM debian:bookworm-slim AS build

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libboost-system-dev \
        libgrpc++-dev \
        libhiredis-dev \
        libprotobuf-dev \
        libssl-dev \
        libyaml-cpp-dev \
        make \
        pkg-config \
        protobuf-compiler \
        protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Makefile ./
COPY include ./include
COPY proto ./proto
COPY src ./src
RUN make -j"$(nproc)"

FROM build AS runtime

RUN useradd --system --uid 10001 --create-home ratelimit
COPY --from=build /src/build/ratelimit-cpp /usr/local/bin/ratelimit

USER 10001
EXPOSE 8080 8081 6070
ENTRYPOINT ["/usr/local/bin/ratelimit"]
