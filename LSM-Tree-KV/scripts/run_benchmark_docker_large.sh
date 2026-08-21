#!/usr/bin/env bash
# "Large-scale" preset: 1,000,000 writes (the same dataset size as the
# default benchmark -- see the sizing comment in src/benchmark.cpp for why
# 1,000,000 keeps total disk usage well under 3GB, ~1.13GB measured
# including retained WAL segments) but 10,000,000 reads instead of the
# default 5,000, for a read-throughput sample two orders of magnitude
# larger and correspondingly more statistically stable.
#
# This is a thin preset over run_benchmark_docker.sh, not a separate
# implementation -- it just pre-sets WRITES/READS and delegates, so there's
# no duplicated Docker-build/run logic to drift out of sync.
#
# Any other env var run_benchmark_docker.sh accepts (MEMORY_LIMIT,
# MIN_VALUE_SIZE, MAX_VALUE_SIZE, MISS_RATE, THREADS, MEMTABLE_THRESHOLD)
# still applies here and overrides these presets if set beforehand.
#
# Usage: ./scripts/run_benchmark_docker_large.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export WRITES="${WRITES:-1000000}"
export READS="${READS:-10000000}"

exec ./scripts/run_benchmark_docker.sh
