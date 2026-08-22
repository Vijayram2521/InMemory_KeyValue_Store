# Builds kv_benchmark, compute_node, and leader_node from one shared build
# stage, then ships each as its own minimal runtime image via named target
# stages -- `docker build --target <name> .` picks which one to produce.
#
# kv_benchmark: `docker run --memory=4g --memory-swap=4g <image> [args]`
#   measures write/read throughput under a fixed memory budget.
# compute_node: `docker run -e NODE_ID=... -e LISTEN_PORT=... -e DATA_DIR=... <image>`
#   runs one StorageEngine shard, served over TCP.
# leader_node:  `docker run -e LISTEN_PORT=... -e COMPUTE_NODES=id:host:port,... <image>`
#   routes client requests to compute nodes via consistent hashing.
# See docker-compose.yml for the full 4-container (1 leader + 3 compute)
# topology this project actually runs.

# ---- Build stage (shared by all three runtime images below) ----
FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target kv_benchmark compute_node leader_node -j "$(nproc)"

# ---- kv_benchmark runtime ----
FROM debian:bookworm-slim AS kv_benchmark

RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin bench

COPY --from=build /src/build/kv_benchmark /app/kv_benchmark

USER bench
WORKDIR /home/bench

ENTRYPOINT ["/app/kv_benchmark"]
# Only --data-dir is set here -- it must point somewhere writable inside the
# container. Every other knob (writes, reads, value size range, miss rate,
# memtable threshold, threads) is left to kv_benchmark's own compiled-in
# defaults (see the sizing comment at the top of src/benchmark.cpp) rather
# than being re-hardcoded here, so there's one source of truth instead of
# numbers that can drift out of sync between the binary and this image.
# `docker run <image> --writes=... ...` replaces this whole CMD list, not
# just appends to it.
CMD ["--data-dir=/home/bench/data"]

# ---- compute_node runtime ----
FROM debian:bookworm-slim AS compute_node

RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin cnode

COPY --from=build /src/build/compute_node /app/compute_node

USER cnode
WORKDIR /home/cnode

# Config comes from env vars (NODE_ID, LISTEN_PORT, DATA_DIR) -- see
# docker-compose.yml -- not CMD args, so no defaults are set here.
ENTRYPOINT ["/app/compute_node"]

# ---- leader_node runtime ----
FROM debian:bookworm-slim AS leader_node

RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin lnode

COPY --from=build /src/build/leader_node /app/leader_node

USER lnode
WORKDIR /home/lnode

# Config comes from env vars (LISTEN_PORT, COMPUTE_NODES) -- see
# docker-compose.yml -- not CMD args, so no defaults are set here.
ENTRYPOINT ["/app/leader_node"]
