# 🚀 LSM-Tree Key-Value Store

A high-performance, persistent Key-Value storage engine built from scratch in C++. This project implements a **Log-Structured Merge-Tree (LSM-Tree)** architecture, modeled after industry standards like RocksDB and AlloyDB, optimized for high write throughput and durable storage.

## 📊 Project Status: Phase 3 Complete (Persistence & Search)
We have successfully transitioned from a volatile in-memory store to a durable engine capable of surviving crashes and managing on-disk sorted files.

### Completed Features
* **Durable Write-Ahead Log (WAL):** Every operation is logged to an append-only binary file with sequence numbers before touching RAM, ensuring zero data loss on crashes.
* **SSTable Implementation:** MemTables are automatically flushed to disk as **Sorted String Tables** once they hit a defined threshold.
* **Tombstone System:** Implemented "Death Markers" (Type 2 records) to handle deletions across multiple files, preventing deleted data from "resurrecting" during reads.
* **Manifest Versioning:** A global `MANIFEST` file tracks the chronological order of SSTables, allowing the engine to rebuild its state correctly upon restart.
* **Multi-Layered Search:** * **First-Hit Logic:** Search order flows from `MemTable` → `Tombstone Set` → `SSTables` (Newest to Oldest).
    * **Binary Search Ready:** SSTables are written in sorted order, laying the groundwork for indexed lookups.
* **Thread-Safety:** Utilizes `std::shared_mutex` for a "Single Writer, Multiple Reader" concurrency model.

---

## 🏗️ Architecture Overview

The engine utilizes a layered approach to balance speed and durability:

1.  **WAL (Write-Ahead Log):** The immediate on-disk recovery log.
2.  **MemTable:** Active in-memory `std::map` providing $O(\log N)$ writes and reads.
3.  **Tombstone Set:** In-memory tracking of deleted keys to short-circuit reads.
4.  **SSTables:** Immutable, sorted binary files on disk representing snapshots of history.
5.  **Manifest:** The "Source of Truth" that manages the list of active SSTables and current sequence numbers.

---

## 🗺️ Roadmap: What's Next?

### Phase 4: High-Performance Search (Indexing)
* [ ] **Sparse Indexing:** Append an index block to the end of SSTables to store `Key -> Byte Offset` every 16th record.
* [ ] **Footer Implementation:** Add a fixed-size footer to SSTables for instant index location.
* [ ] **Point Lookups:** Replace linear file scans with `seekg` jumps based on binary-searched index offsets.

### Phase 5: Optimization & Efficiency
* [ ] **Bloom Filters:** Implement a bitmask (Probabilistic Data Structure) for each SSTable to skip disk I/O for keys that definitely do not exist in that file.
* [ ] **Compaction (L0 -> L1):** Develop a background worker to merge fragmented SSTables, discard obsolete versions, and clear out processed tombstones.

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
# Build the image and run it in a container capped at 2GB of memory
./scripts/run_benchmark_docker.sh    # bash
./scripts/run_benchmark_docker.ps1   # PowerShell
```
Optional env overrides: `WRITES`, `READS`, `MIN_VALUE_SIZE`,
`MAX_VALUE_SIZE`, `MISS_RATE`, `THREADS`, `MEMTABLE_THRESHOLD`,
`MEMORY_LIMIT` (default `2g`). Leave any of them unset and the container
falls back to `kv_benchmark`'s own compiled-in defaults (1,000,000 writes,
100 reads, memtable-threshold 250,000, value sizes 64-1024 bytes) -- the
Dockerfile and scripts deliberately don't re-hardcode those numbers, so
there's one source of truth. Run `kv_benchmark --help` (or read the sizing
comment at the top of `src/benchmark.cpp`) for the full derivation and flag
list.

**How the default 1,000,000-write workload was sized:** at an average
on-disk record size of ~563 bytes (9 bytes of framing + ~10-byte key +
average 544-byte value), 1,000,000 writes lands at ~563MB of SSTable data on
disk -- about a quarter of the container's 2GB budget, verified empirically
(4 SSTables totaling 562.9MB, within 0.4% of the calculation). The
`memtable-threshold` of 250,000 keeps the in-flight memtable's peak RAM
around ~150MB (~7% of the budget), comfortably inside the 2GB limit.

**Note on scale:** every SSTable lookup today opens the file and does a full
linear scan (no sparse index or bloom filter yet -- see the Phase 4/5
roadmap below), so read cost is roughly proportional to *total records
written*, not just to SSTable count. Calibrated locally at ~1.165us of scan
time per record in a file, the default workload (1,000,000 writes) averages
~570ms per Get -- verified empirically at ~57s for the default 100 reads
(~1.75 RPS), with the full default run (write + read) taking ~75s total.
Raise `WRITES` or `READS` deliberately, not by accident -- they trade off
directly against how long the read phase takes.