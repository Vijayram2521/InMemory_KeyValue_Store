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
# Create build directory
mkdir build && cd build

# Configure and compile
cmake .. -G "MinGW Makefiles"
cmake --build .

# Run the automated test suite
./kv_engine_test.exe

# Launch the interactive CLI
./lsm_cli.exe