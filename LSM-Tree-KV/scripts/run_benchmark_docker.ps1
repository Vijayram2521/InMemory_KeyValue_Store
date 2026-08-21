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
# Optional env overrides: MEMORY_LIMIT (default 4g), WRITES, DELETES, READS,
#   MIN_VALUE_SIZE, MAX_VALUE_SIZE, MISS_RATE, THREADS, MEMTABLE_THRESHOLD
#
# Usage: .\scripts\run_benchmark_docker.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$Image = "lsm-kv-benchmark:latest"
$MemoryLimit = if ($env:MEMORY_LIMIT) { $env:MEMORY_LIMIT } else { "4g" }

docker build -t $Image .

$benchArgs = @("--data-dir=/home/bench/data")
if ($env:WRITES)             { $benchArgs += "--writes=$($env:WRITES)" }
if ($env:DELETES)             { $benchArgs += "--deletes=$($env:DELETES)" }
if ($env:READS)               { $benchArgs += "--reads=$($env:READS)" }
if ($env:MIN_VALUE_SIZE)      { $benchArgs += "--min-value-size=$($env:MIN_VALUE_SIZE)" }
if ($env:MAX_VALUE_SIZE)      { $benchArgs += "--max-value-size=$($env:MAX_VALUE_SIZE)" }
if ($env:MISS_RATE)           { $benchArgs += "--miss-rate=$($env:MISS_RATE)" }
if ($env:THREADS)             { $benchArgs += "--threads=$($env:THREADS)" }
if ($env:MEMTABLE_THRESHOLD)  { $benchArgs += "--memtable-threshold=$($env:MEMTABLE_THRESHOLD)" }

docker run --rm `
    --memory=$MemoryLimit --memory-swap=$MemoryLimit `
    $Image `
    @benchArgs
