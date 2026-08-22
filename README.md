# 🚀 LSM-Tree Key-Value Store

A high-performance, persistent Key-Value storage engine built from scratch in C++. This project implements a **Log-Structured Merge-Tree (LSM-Tree)** architecture, modeled after industry standards like RocksDB and AlloyDB, optimized for high write throughput and durable storage.

## 📊 Project Status: Phase 4 Complete (Indexed Search)
We have successfully transitioned from a volatile in-memory store to a durable engine capable of surviving crashes, managing on-disk sorted files, and finding a key in any of them without scanning the file.

### Completed Features
* **Durable Write-Ahead Log (WAL):** Every operation is logged to an append-only binary file with sequence numbers before touching RAM, ensuring zero data loss on crashes.
* **SSTable Implementation:** MemTables are automatically flushed to disk as **Sorted String Tables** once they hit a defined threshold.
* **Tombstone System:** Implemented "Death Markers" (Type 2 records) to handle deletions across multiple files, preventing deleted data from "resurrecting" during reads.
* **Manifest Versioning:** A global `MANIFEST` file tracks the chronological order of SSTables, allowing the engine to rebuild its state correctly upon restart.
* **Multi-Layered Search:** * **First-Hit Logic:** Search order flows from `MemTable` → `Tombstone Set` → `SSTables` (Newest to Oldest).
    * **Indexed Point Lookups:** Every SSTable carries a full sorted index (`Key -> Byte Offset`) plus a fixed footer; a lookup binary-searches the index in memory, then does one `seekg` straight to the record. No SSTable is ever linearly scanned to answer a `Get`.
* **Thread-Safety:** Utilizes `std::shared_mutex` for a "Single Writer, Multiple Reader" concurrency model.
* **Bloom Filters:** One filter per SSTable, checked before opening the file at all. See Phase 5 in the Roadmap below for the full writeup (hashing scheme, sizing, measured false-positive rate).
* **Test Suite:** Real assertions (not print-and-eyeball output) via a small header-only framework, wired into CTest. 36 checks covering WAL crash recovery, SSTable flush/restart, tombstone resurrection, Manifest, StorageEngine edge cases (overwrite, delete-then-revive), the SSTable index/binary-search path, and the Bloom filter's no-false-negatives guarantee plus measured false-positive rate.
* **Throughput Benchmark:** `kv_benchmark` times writes and reads separately against a workload sized deliberately against a 2GB memory budget (see "Benchmarking" below), with every value an independently random size and 15% of reads guaranteed misses -- runnable natively or in a Docker container capped at `--memory=4g`.

---

## 🏗️ Architecture Overview

The engine utilizes a layered approach to balance speed and durability:

1.  **WAL (Write-Ahead Log):** The immediate on-disk recovery log.
2.  **MemTable:** Active in-memory `std::map` providing $O(\log N)$ writes and reads.
3.  **Tombstone Set:** In-memory tracking of deleted keys to short-circuit reads.
4.  **SSTables:** Immutable, sorted binary files on disk representing snapshots of history. Each file is `[data block][index block][footer]`; puts and tombstones are merged into one ascending-key sequence during the flush so the index (and binary search over it) stays valid regardless of record type.
5.  **Manifest:** The "Source of Truth" that manages the list of active SSTables and current sequence numbers.
6.  **Index Cache:** An in-memory `unordered_map<sstable path, index>` inside `StorageEngine`, populated once per file (at flush time for new generations, at startup for generations recovered via the Manifest) and never rebuilt on the read path.
7.  **Bloom Filter Cache:** A parallel `unordered_map<sstable path, BloomFilter>`, same population lifecycle as the Index Cache above. Consulted in `Get` *before* the index/file at all -- a "definitely not here" answer skips that SSTable without ever opening it.

### Write Path
```
    +-----------------------+
    |         Client        |
    | Put(k, v) / Delete(k) |
    +-----------------------+
                 |
                 v
     +----------------------+
     |    StorageEngine     |
     | unique_lock(rw_lock) |
     +----------------------+
                 |
                 v
   +-------------------------+
   |           WAL           |
   | append record + flush() |
   +-------------------------+
                 |
                 v
     +----------------------+
     |       MemTable       |
     | std::map<key, value> |
     +----------------------+
                 |
                 v   size >= THRESHOLD ?
           +---------+
           | flush() |
           +---------+
                 |
                 v
  +----------------------------+
  |  SSTable written to disk   |
  | [ data | index | footer ]  |
  | (puts + tombstones merged  |
  |  into one sorted sequence) |
  +----------------------------+
                 |
                 v
 +-----------------------------+
 |           Manifest          |
 | append new generation entry |
 +-----------------------------+
                 |
                 v
  +----------------------------+
  |        Index Cache         |
  | cache the index just built |
  |   (no re-read from disk)   |
  +----------------------------+
```

### Read Path
```
            +--------+
            | Client |
            | Get(k) |
            +--------+
                 |
                 v
     +----------------------+
     |    StorageEngine     |
     | shared_lock(rw_lock) |
     +----------------------+
                 |
                 v
       +------------------+
       | 1. Tombstone Set |   present? --> return nullopt (it's deleted)
       |   (in-memory)    |
       +------------------+
                 | not present
                 v
         +-------------+
         | 2. MemTable |   present? --> return value
         |   std::map  |
         +-------------+
                 | not present
                 v
 +-----------------------------+
 |         3. SSTables         |
 | newest -> oldest generation |
 +-----------------------------+
                 |
                 v   for each generation, newest first:
     +----------------------+
     |     Bloom Filter     |   definitely absent --> skip this file, no open()
     | maybe_contains(key)? |
     +----------------------+
                 | maybe present
                 v
      +--------------------+
      |    Index Cache     |
      | binary_search(key) |
      +--------------------+
                 |
                 v
              found in this file's index?
                |
                +-- yes --> seekg(offset) --> read exactly 1 record
                |             --> value, or nullopt if it's a tombstone
                |
                +-- no  --> try the next (older) generation
                             (or return nullopt if none left)
```

### SSTable On-Disk Layout
```
+--------------------------------------------------------------------------+
|                                                                          |
|                             One SSTable File                             |
|                                                                          |
|  +----------------------+  +----------------------+  +----------------+  |
|  |      Data Block      |  |     Index Block      |  |     Footer     |  |
|  | PUT/DELETE records,  |  | key -> byte offset,  |  |   20 bytes:    |  |
|  |   merged into ONE    |  |  same ascending-key  |  | index_offset,  |  |
|  | ascending-key order  |  |  order as the data   |  |  index_count,  |  |
|  |   (never scanned)    |  |                      |  |  magic number  |  |
|  +----------------------+  +----------------------+  +----------------+  |
|                                                                          |
|  byte offset 0           offset = end of data      offset = EOF-20       |
+--------------------------------------------------------------------------+

Lookup: seek to EOF-20, read footer --> binary_search(index) in memory
        --> seekg(matched byte offset) --> read exactly 1 record --> done
        (a key not found in the index is not in the file -- no scan fallback)
```

---

## 🗺️ Roadmap: What's Next?

### Phase 4: High-Performance Search (Indexing) -- ✅ Complete
* [x] **Full Indexing:** Every SSTable carries a complete `Key -> Byte Offset` index, not a sparse one. The roadmap originally called for indexing every 16th record; a full index was chosen instead because the overhead is small (~3% of file size at this project's typical value sizes) and it avoids the extra "scan forward from the nearest indexed neighbor" step a sparse index requires -- simpler and strictly O(log n) with no scan component at all. Revisit sparse indexing only if index memory becomes a real constraint at a much larger scale than currently benchmarked.
* [x] **Footer Implementation:** A fixed 20-byte footer (index offset, index entry count, magic number) at a known offset from EOF lets a lookup jump straight to the index without scanning for it.
* [x] **Point Lookups:** `SSTable::search_with_index` binary-searches the cached index and does exactly one `seekg` + read on a hit; there is no scan fallback. Measured on the benchmark's default 1,000,000-write / 4-generation workload: read throughput went from ~1.75 RPS (full linear scan per lookup) to **~150-340 RPS on Windows** (native x86_64) and **~190K-363K RPS on Linux** (Docker container, ARM64, warm page cache) -- see the "Benchmarking" section below for the full numbers, environment differences, and how each was measured, and `src/benchmark.cpp`'s sizing comment for the before/after methodology.

### Phase 5: Optimization & Efficiency
* [x] **Bloom Filters:** One filter per SSTable (`include/engine/bloom_filter.h`, `src/bloom_filter.cpp`), built at flush/load time alongside the existing index and consulted in `Get` *before* `std::ifstream::open()` -- currently the dominant per-lookup cost (see "Benchmarking" above) -- so a `Get` can skip opening a file entirely when the filter proves the key definitely isn't in it. 5 hash probes per key (Kirsch-Mitzenmacher double-hashing: two FNV-1a hashes combined as `h1 + i*h2 mod m`, standard practice in production Bloom filters -- provably as effective as 5 genuinely independent hash functions without paying for 5 separate hash computations), 10 bits/key, landing at the standard ~1% false-positive-rate ballpark (~0.94% analytically; **0.60% measured** on a 100,000-key sample -- see `tests/test_bloom_filter.cpp`). Correctness-checked two ways: 5 dedicated unit tests (no false negatives -- a hard guarantee, checked exactly, not sampled; false-positive rate stays well under a generous bound; sizing) plus the full existing suite (36/36 checks) passing unmodified through the public `StorageEngine` API, confirming this is purely a skip-work optimization with no observable behavior change. Perf validation (does it actually move the RPS needle, especially on the many-generation "biggest" workload) is the next step, pending VM access.
* [ ] **Compaction (L0 -> L1):** A background (or on-demand) worker that merges multiple SSTable generations into fewer, larger ones -- discarding obsolete versions of overwritten keys and dropping tombstones once nothing older they'd shadow remains. Two things to get right: (1) **locking** -- compaction mutates the same `sstable_files` list and `index_cache` that `Get` reads under `shared_lock`, so a merge needs to build the new merged file(s) *off to the side* first and only take the `unique_lock` for the brief pointer-swap that atomically replaces the old generation list with the new one, rather than holding a write lock for the whole (potentially slow) merge; (2) it directly fixes two things this session's benchmarking exposed as real, not theoretical, problems: generation count growing unboundedly (currently the only way to keep it bounded is choosing a big `memtable-threshold` up front) and stale WAL segments never being cleaned up after their generation flushes (roughly doubles on-disk footprint today -- see the sizing math in `src/benchmark.cpp`).
* [ ] **Cold-cache benchmarking:** The Linux/Docker RPS numbers in the "Benchmarking" section above all reflect a warm OS page cache (reads immediately follow the write phase that produced the same dataset). Get a genuine cold-cache number too -- e.g. drop caches between phases, or read data written by a prior, separate `docker run` -- for a fuller "worst case" picture alongside the current "best case" numbers.

### Phase 6: Advanced Features
* [ ] **Value Compression:** Compress each value (LZ4 is the natural choice -- fast enough that decompression cost on the read path stays negligible next to the I/O costs above, unlike heavier schemes optimized for ratio over speed) before writing it into an SSTable record, decompressing on read. Per-value rather than per-block: it fits this project's existing record framing (type + kLen + key + vLen + value) without restructuring the file format, at the cost of losing the better compression ratio a shared block-level dictionary would give across many small values. Matters most for the "large values" benchmark configuration (512-4096B, see below) -- that's exactly the value-size range where compression has real bytes to work with, unlike the earlier 64-1024B default where per-value overhead would dominate any savings.
* [ ] **Snapshots:** Implement point-in-time consistent views of the database.

---

## 🛠️ Getting Started

### Prerequisites
* **Windows 10/11**
* **MSYS2 UCRT64** Environment
* **CMake** (MSYS2 version)
* **C++23** compatible compiler (GCC 13+)

### Build & Run
```bash
# From LSM-Tree-KV/, configure, build, and run the unit test suite via CTest
./scripts/run_tests.sh      # bash / MSYS2
./scripts/run_tests.ps1     # PowerShell
```
This builds two binaries into `build/`: `kv_tests` (the unit test suite,
registered with CTest) and `kv_benchmark` (the throughput benchmark below).
There is currently no interactive CLI -- `StorageEngine` is used as an
in-process library (see `src/main.cpp` / `src/benchmark.cpp` for usage
examples).

### Benchmarking (Docker)
`kv_benchmark` times up to three phases separately against a single
`StorageEngine`: writes (WPS), an optional delete phase (DPS -- deletes
`--deletes` distinct, previously-written keys, sampled without replacement,
exercising the tombstone path), and reads (RPS). Every value gets its own
random size in `[min-value-size, max-value-size]` -- values are never a
fixed size, so nothing about the dataset is artificially uniform. By
default, 15% of reads target keys that were never written (a disjoint key
prefix guarantees a genuine miss rather than relying on chance), simulating
a realistic cache-miss workload; the observed miss rate is printed alongside
the requested one so you can confirm the split landed correctly (and, if
`--deletes` is nonzero, the observed rate will read a bit above the
requested one -- some "hit" targets will have since been deleted, which is
correct tombstone behavior, not a bug). Every phase also prints periodic
`[progress] phase: done/total (pct%) elapsed=Ws` status lines
(`--progress-interval`, default 10s) so a long phase never looks
indistinguishable from a hang.

```bash
# Build the image and run it in a container capped at 4GB of memory
./scripts/run_benchmark_docker.sh    # bash
./scripts/run_benchmark_docker.ps1   # PowerShell
```
Optional env overrides: `WRITES`, `DELETES`, `READS`, `MIN_VALUE_SIZE`,
`MAX_VALUE_SIZE`, `MISS_RATE`, `THREADS`, `MEMTABLE_THRESHOLD`,
`MEMORY_LIMIT` (default `4g`). Leave any of them unset and the container
falls back to `kv_benchmark`'s own compiled-in defaults (1,000,000 writes,
0 deletes, 5,000 reads, memtable-threshold 250,000, value sizes 64-1024
bytes) -- the Dockerfile and scripts deliberately don't re-hardcode those
numbers, so there's one source of truth. Run `kv_benchmark --help` (or read
the sizing comment at the top of `src/benchmark.cpp`) for the full
derivation and flag list.

```bash
# Large-scale preset: same 1,000,000-write dataset (well under 3GB of disk),
# but 10,000,000 reads instead of 5,000 -- two orders of magnitude more
# read samples for a far more statistically stable RPS number.
./scripts/run_benchmark_docker_large.sh    # bash
./scripts/run_benchmark_docker_large.ps1   # PowerShell
```
A thin preset over the same script above (just pre-sets `WRITES`/`READS`,
no duplicated logic) -- see "Large-scale results" below for the numbers
this produced.

```bash
# Biggest preset: 3,000,000 writes at 100,000 records/generation, delete
# 300,000 (10%), then 10,000,000 reads -- see "Large-scale,
# higher-generation-count results" below for what this found.
./scripts/run_benchmark_docker_biggest.sh    # bash
./scripts/run_benchmark_docker_biggest.ps1   # PowerShell
```
Also a thin preset (`WRITES`/`DELETES`/`READS`/`MEMTABLE_THRESHOLD`/
`MIN_VALUE_SIZE`/`MAX_VALUE_SIZE` pre-set) over the same script.

**How the default 1,000,000-write workload was sized:** at an average
on-disk record size of ~563 bytes (9 bytes of framing + ~10-byte key +
average 544-byte value), 1,000,000 writes lands at ~563MB of SSTable data on
disk -- about a quarter of the *original* 2GB reference budget the workload
was sized against, verified empirically (4 SSTables totaling 562.9MB, within
0.4% of the calculation). The `memtable-threshold` of 250,000 keeps the
in-flight memtable's peak RAM around ~150MB (~7% of that 2GB reference); the
index cache (see Phase 4 below) adds another ~40MB across all 1,000,000
indexed keys. The container itself now runs with a `--memory=4g` cap (up
from the original 2g) for extra headroom, so in practice this workload uses
under 15% of what the container actually allows -- the workload wasn't
rescaled to fill the larger cap, it just has more room to spare.

**Read performance:** every SSTable lookup binary-searches that file's
cached index (built once, not per lookup) and does a single `seekg` -- no
linear scan. Measured at this default scale (1,000,000 writes, 4
generations, 4 threads), in two very different environments:

| Environment | Sample | RPS | WPS |
|---|---|---|---|
| Windows 11, native (x86_64, NTFS) | 200,000 reads | ~226.8 | ~58K |
| Windows 11, native (x86_64, NTFS) | 5,000 reads, cache-warm | ~339.0 | ~63K |
| Linux, Docker container (ARM64, Oracle Ampere A1, `--memory=4g`) | 5,000 reads | ~190,404 | ~88K |
| Linux, Docker container (ARM64, Oracle Ampere A1, `--memory=4g`) | 2,000,000 reads | ~363,252 | ~90K |
| **Linux, Docker container (ARM64, Oracle Ampere A1, `--memory=4g`)** | **10,000,000 reads** | **~351,649** | **~89.5K** |

The Linux/Docker numbers are not a like-for-like recheck of the Windows
ones -- different CPU architecture (Ampere ARM64 vs. x86_64), different
filesystem (ext4/overlayfs vs. NTFS), and Windows file `open()` calls carry
overhead (real-time antivirus scanning, NTFS metadata) that Linux's VFS
largely doesn't. Both are honestly reported rather than picking the
flattering one. It reflects a warm-page-cache scenario (reads immediately
follow the write phase that produced the same dataset, which easily fits in
the VM's 32GB RAM), so treat it as "best case, data is hot," not a promise
that a cold read against a much larger, disk-resident dataset would be this
fast (see the cold-cache follow-up noted in the roadmap below).

**Large-scale results (`run_benchmark_docker_large`):** the numbers above
could be small-sample artifacts, so they were checked against progressively
bigger read samples on the same 1,000,000-key dataset (~1.13GB on disk,
comfortably under a 3GB budget) -- 5,000, then 2,000,000, then
**10,000,000** reads. RPS stayed in a tight band the whole way (190K -> 363K
-> 352K), including at 2,000x the original sample size, which is what makes
this a real sustained-throughput number rather than a lucky small sample:

* **1,000,000 writes:** 11.18s, **~89,470 WPS**, ~563MB of SSTable data (4
  generations) -- matches the sizing calculation to within 0.4%, as before.
* **10,000,000 reads:** 28.44s, **~351,649 RPS**, exactly the requested
  85%/15% hit/miss split (8,500,000 hits / 1,500,000 misses).
* Total run: ~40s, on hardware that cost nothing extra to provision (the
  same VM already used for the numbers above).

Reproduce with `./scripts/run_benchmark_docker_large.sh` (or `.ps1`) -- a
thin preset over the same Docker script above, just with `WRITES=1000000
READS=10000000` pre-set.

**Large-scale, higher-generation-count results (`run_benchmark_docker_biggest`):**
every number above was measured at 4 SSTable generations. This preset asks a
different question -- what happens well past that, closer to a keyspace
that genuinely doesn't fit in memory -- with 3,000,000 writes at 100,000
records/generation, then deleting 300,000 keys (10%, exercising the
tombstone path for real, not just via the small correctness tests), then
10,000,000 reads. Values are 512-4096B (mean ~2304B) instead of the default
64-1024B, so I/O actually moves meaningful bytes per op:

* **Generation count:** landed at **33**, not the naively-expected 30 --
  `Delete` can *also* trigger a flush once accumulated tombstones cross
  `memtable-threshold` (300,000 deletes / 100,000 threshold ~= 3 more),
  which is easy to miss if you only think about the write phase.
* **Disk usage:** ~6.9GB of SSTable data (`AVG_VALUE_BYTES` landed at
  2302.7, essentially exact against the 2304B target), ~18GB total on the
  VM including retained WAL segments and the Docker image layers -- verified
  directly with `df -h`, not just calculated.
* **Peak memory: ~1.2GB (30% of the 4GB cap), confirmed empirically** by
  sampling `docker stats` throughout the run, not just predicted --
  comfortably safe, and it stayed roughly flat through the write, delete,
  *and* read phases rather than spiking at any one of them. This is the
  concrete answer to "would a keyspace too big to fit in memory cause an
  OOM": no, because the memtable is bounded by `memtable-threshold`
  regardless of total keys, and the only component that scales with total
  writes (the index cache) is small enough (~40B/entry) that it would take
  on the order of 100,000,000 keys to threaten a 4GB budget on its own.
* **WPS: ~11,480** (261.3s for 3,000,000 writes) -- lower than the ~89,470
  WPS at the default value-size range, consistent with writing 4-6x more
  bytes per record.
* **DPS: ~149,930** (2.0s for 300,000 deletes) -- deletes are cheap: no
  value to write, just a WAL append plus an in-memory map/set update.
* **RPS: ~4,419** (2,262.9s, i.e. ~37.7 minutes, for 10,000,000 reads) --
  **an ~80x drop from the ~352K RPS measured at 4 generations**, and far
  worse than a simple "average files touched per lookup" model predicts
  (that model, based on a miss needing to open all N generations and a hit
  needing to open roughly N/2 on average, predicts only a ~7x slowdown
  from 4 to 33 generations, i.e. ~49K RPS). The gap between that prediction
  and the measured ~4,419 RPS is itself informative: it means real disk/
  page-cache I/O pressure -- not just the *count* of `open()` calls -- has
  become the dominant cost once the dataset (~18GB) is large enough that it
  no longer trivially fits alongside everything else in page cache the way
  the ~560MB-to-1.13GB 4-generation datasets did. This is exactly the
  scenario bloom filters and compaction (Phase 5 below) target: bloom
  filters would let most of that unnecessary `open()`-and-maybe-read
  traffic never happen at all for keys that provably aren't in a given
  file, and compaction would keep generation count (and therefore files
  touched per lookup) bounded as the store grows, instead of growing
  without limit.
* **Observed miss rate: 23.5%** against a 15% request (2,350,944 misses /
  10,000,000 reads) -- consistent with the delete-interaction documented
  above: 15% guaranteed misses plus ~85% * 10% of "hit" targets landing on
  a since-deleted key ~= 23.5% predicted, matching almost exactly.

This run also surfaced two real bugs, both fixed the same session rather
than left in the numbers above: (1) the very first attempt lost its
results entirely -- a local SSH connection capturing output silently died
during the ~38-minute, previously-silent read phase (no data flowing long
enough to trip an idle-connection timeout), and because the container used
`--rm` with no mounted volume, both the output and the on-disk dataset were
gone once it exited. Fixed by adding periodic progress logging to
`kv_benchmark` itself (see above) and, operationally, by launching detached
on the remote side with output redirected to a file there instead of
relying on one long-lived local connection. (2) That same progress-logging
addition introduced a formatting bug -- `std::fixed`/`setprecision` applied
directly to `std::cout` leaked into all later output, truncating the final
results block to 1 decimal place (e.g. the requested 0.15 miss rate
displayed as "0.1"). Caught by noticing the displayed value didn't match
what was actually requested, root-caused, and fixed by scoping the
formatting to a local stream instead -- the underlying computed values were
never wrong, only their display.

Reproduce with `./scripts/run_benchmark_docker_biggest.sh` (or `.ps1`).
Budget real time for it -- unlike the other presets, the read phase alone
takes tens of minutes at this generation count, not seconds.

Either way, this is **up from ~1.75 RPS** before the index existed --
roughly two to five orders of magnitude, depending on environment. The
current bottleneck is one `std::ifstream` `open()` per SSTable a lookup has
to touch (a guaranteed miss must open all of them), not scanning -- bloom
filters (Phase 5 below) would let a miss skip that open entirely, which
would matter more on Windows than it apparently does on Linux. Raise
`WRITES` or `READS` deliberately, not by accident -- they still cost time,
just far less than before.