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
# the VM this was validated on -- `Get-PSDrive`.
#
# This is a thin preset over run_benchmark_docker.ps1, not a separate
# implementation -- it just pre-sets these env vars and delegates, so
# there's no duplicated Docker-build/run logic to drift out of sync.
#
# Any env var run_benchmark_docker.ps1 accepts still applies and overrides
# these presets if set beforehand (MEMORY_LIMIT, MISS_RATE, THREADS).
#
# Usage: .\scripts\run_benchmark_docker_biggest.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not $env:WRITES)             { $env:WRITES = "3000000" }
if (-not $env:DELETES)            { $env:DELETES = "300000" }
if (-not $env:READS)              { $env:READS = "10000000" }
if (-not $env:MEMTABLE_THRESHOLD) { $env:MEMTABLE_THRESHOLD = "100000" }
if (-not $env:MIN_VALUE_SIZE)     { $env:MIN_VALUE_SIZE = "512" }
if (-not $env:MAX_VALUE_SIZE)     { $env:MAX_VALUE_SIZE = "4096" }

& (Join-Path $PSScriptRoot "run_benchmark_docker.ps1")
