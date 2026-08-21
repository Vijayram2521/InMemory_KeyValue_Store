# Builds kv_benchmark and runs it in a small runtime image.
# Intended usage: `docker run --memory=2g --memory-swap=2g <image> [kv_benchmark args]`
# to measure write/read throughput under a fixed memory budget.

# ---- Build stage ----
FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target kv_benchmark -j "$(nproc)"

# ---- Runtime stage ----
FROM debian:bookworm-slim

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
