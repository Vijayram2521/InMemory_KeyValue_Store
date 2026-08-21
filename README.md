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
* **Test Suite:** Real assertions (not print-and-eyeball output) via a small header-only framework, wired into CTest. 31 checks covering WAL crash recovery, SSTable flush/restart, tombstone resurrection, Manifest, StorageEngine edge cases (overwrite, delete-then-revive), and the SSTable index/binary-search path directly.
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
* [ ] **Bloom Filters:** Implement a bitmask (Probabilistic Data Structure) for each SSTable to skip disk I/O for keys that definitely do not exist in that file.
* [ ] **Compaction (L0 -> L1):** Develop a background worker to merge fragmented SSTables, discard obsolete versions, and clear out processed tombstones.
* [ ] **Cold-cache benchmarking:** The Linux/Docker RPS numbers in the "Benchmarking" section above all reflect a warm OS page cache (reads immediately follow the write phase that produced the same dataset). Get a genuine cold-cache number too -- e.g. drop caches between phases, or read data written by a prior, separate `docker run` -- for a fuller "worst case" picture alongside the current "best case" numbers.

### Phase 6: Advanced Features
* [ ] **Block Compression:** Snappy or LZ4 compression for SSTable blocks to reduce disk footprint.
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
`kv_benchmark` times a write phase and a read phase separately against a
single `StorageEngine`, reporting writes/sec (WPS) and reads/sec (RPS). Every
value gets its own random size in `[min-value-size, max-value-size]` --
values are never a fixed size, so nothing about the dataset is artificially
uniform. By default, 15% of reads target keys that were never written (a
disjoint key prefix guarantees a genuine miss rather than relying on
chance), simulating a realistic cache-miss workload; the observed miss rate
is printed alongside the requested one so you can confirm the split landed
correctly.

```bash
# Build the image and run it in a container capped at 4GB of memory
./scripts/run_benchmark_docker.sh    # bash
./scripts/run_benchmark_docker.ps1   # PowerShell
```
Optional env overrides: `WRITES`, `READS`, `MIN_VALUE_SIZE`,
`MAX_VALUE_SIZE`, `MISS_RATE`, `THREADS`, `MEMTABLE_THRESHOLD`,
`MEMORY_LIMIT` (default `4g`). Leave any of them unset and the container
falls back to `kv_benchmark`'s own compiled-in defaults (1,000,000 writes,
5,000 reads, memtable-threshold 250,000, value sizes 64-1024 bytes) -- the
Dockerfile and scripts deliberately don't re-hardcode those numbers, so
there's one source of truth. Run `kv_benchmark --help` (or read the sizing
comment at the top of `src/benchmark.cpp`) for the full derivation and flag
list.

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

Either way, this is **up from ~1.75 RPS** before the index existed --
roughly two to five orders of magnitude, depending on environment. The
current bottleneck is one `std::ifstream` `open()` per SSTable a lookup has
to touch (a guaranteed miss must open all of them), not scanning -- bloom
filters (Phase 5 below) would let a miss skip that open entirely, which
would matter more on Windows than it apparently does on Linux. Raise
`WRITES` or `READS` deliberately, not by accident -- they still cost time,
just far less than before.