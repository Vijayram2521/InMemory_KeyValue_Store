#!/usr/bin/env bash
# "Biggest" preset: 3,000,000 writes at 100,000 records/generation (30
# SSTable files), then delete 300,000 of them (10%, sampled without
# replacement, exercising the tombstone path), then 10,000,000 reads.
#
# Sizing (value range 512-4096B, mean 2304B, matching the "use large values"
# ask so I/O actually moves meaningful bytes per op, not just per-op fixed
# overhead): ~6.97GB of SSTable data, ~13.94GB total disk including
# retained WAL segments -- comfortably clears a 4GB RAM budget on disk while
# staying well under it in actual peak memory (memtable is bounded by
# memtable-threshold, not total writes; see the sizing comment in
# src/benchmark.cpp). Verify free disk before running somewhere other than
# the VM this was validated on -- `df -h`.
#
# This is a thin preset over run_benchmark_docker.sh, not a separate
# implementation -- it just pre-sets these env vars and delegates, so
# there's no duplicated Docker-build/run logic to drift out of sync.
#
# Any env var run_benchmark_docker.sh accepts still applies and overrides
# these presets if set beforehand (MEMORY_LIMIT, MISS_RATE, THREADS).
#
# Usage: ./scripts/run_benchmark_docker_biggest.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export WRITES="${WRITES:-3000000}"
export DELETES="${DELETES:-300000}"
export READS="${READS:-10000000}"
export MEMTABLE_THRESHOLD="${MEMTABLE_THRESHOLD:-100000}"
export MIN_VALUE_SIZE="${MIN_VALUE_SIZE:-512}"
export MAX_VALUE_SIZE="${MAX_VALUE_SIZE:-4096}"

exec ./scripts/run_benchmark_docker.sh
