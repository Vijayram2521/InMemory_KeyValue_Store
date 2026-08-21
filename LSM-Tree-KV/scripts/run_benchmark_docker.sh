#!/usr/bin/env bash
# Build the kv_benchmark image and run it in a container capped at 4GB of
# memory, reporting write/read throughput (WPS/RPS) with 15% guaranteed-miss
# reads by default.
#
# All benchmark knobs (writes, reads, value size range, miss rate, threads,
# memtable threshold) default to kv_benchmark's own compiled-in defaults --
# see the sizing comment at the top of src/benchmark.cpp for how those were
# derived. Set an env var below only if you want to override one; unset ones
# are simply not passed through, so this script never hardcodes a number
# that could drift out of sync with the binary's actual defaults.
#
# Optional env overrides: MEMORY_LIMIT (default 4g), WRITES, READS,
#   MIN_VALUE_SIZE, MAX_VALUE_SIZE, MISS_RATE, THREADS, MEMTABLE_THRESHOLD
#
# Usage: ./scripts/run_benchmark_docker.sh
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="lsm-kv-benchmark:latest"
MEMORY_LIMIT="${MEMORY_LIMIT:-4g}"

docker build -t "$IMAGE" .

args=(--data-dir=/home/bench/data)
[ -n "${WRITES:-}" ]             && args+=(--writes="$WRITES")
[ -n "${READS:-}" ]              && args+=(--reads="$READS")
[ -n "${MIN_VALUE_SIZE:-}" ]     && args+=(--min-value-size="$MIN_VALUE_SIZE")
[ -n "${MAX_VALUE_SIZE:-}" ]     && args+=(--max-value-size="$MAX_VALUE_SIZE")
[ -n "${MISS_RATE:-}" ]          && args+=(--miss-rate="$MISS_RATE")
[ -n "${THREADS:-}" ]            && args+=(--threads="$THREADS")
[ -n "${MEMTABLE_THRESHOLD:-}" ] && args+=(--memtable-threshold="$MEMTABLE_THRESHOLD")

docker run --rm \
    --memory="$MEMORY_LIMIT" --memory-swap="$MEMORY_LIMIT" \
    "$IMAGE" \
    "${args[@]}"
