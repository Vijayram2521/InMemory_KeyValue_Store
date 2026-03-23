# 🚀LSM-Tree Key-Value Store

A high-performance, persistent Key-Value storage engine built from scratch in C++. This project implements the **Log-Structured Merge-Tree (LSM-Tree)** architecture, optimizing for high-write throughput and efficient disk utilization.

## Project Status: Phase 1 Complete
We have successfully established the core engine interface and a thread-safe in-memory storage layer.

### Completed Features
* **Modern C++23 Environment:** Fully migrated to the `MSYS2 UCRT64` toolchain, ensuring support for `std::optional`, `std::shared_mutex`, and advanced memory management.
* **Unified Build System:** Configured CMake to work seamlessly with the MSYS2 environment inside VS Code.
* **Storage Interface:** Defined a clean, abstract API for `Put`, `Get`, and `Delete` operations.
* **MemTable (In-Memory Store):** * Implemented using `std::map` for ordered key storage.
    * Thread-safety guaranteed via `std::shared_mutex` (allowing multiple readers or one exclusive writer).
* **PIMPL Design Pattern:** Decoupled the engine implementation from the public interface to reduce compilation times and hide internal complexity.

---

##Architecture Overview

The engine follows a layered approach to balance speed and durability:



1.  **Interface:** The entry point for the user (`lsm_cli`).
2.  **MemTable (Current):** The active, in-memory buffer where all writes initially live.
3.  **WAL (Next):** The "Write-Ahead Log" to ensure data isn't lost during a crash.
4.  **SSTables (Planned):** On-disk, sorted files where data is eventually flushed for permanent storage.

---

## Roadmap: What's Next?

### Phase 2: Durability (The WAL) 
* [ ] **WAL Implementation:** Create a binary append-only log to record operations.
* [ ] **Persistence First:** Update `Put` logic to write to disk *before* updating the MemTable.
* [ ] **Crash Recovery:** Implement a replay mechanism to rebuild the MemTable from the WAL on startup.

### Phase 3: Persistence & Flushing (SSTables)
* [ ] **Threshold Management:** Detect when the MemTable exceeds a size limit (e.g., 4MB).
* [ ] **Immutable MemTables:** Freeze the current table and flush it to disk as a **Sorted String Table (SSTable)**.
* [ ] **Multi-Level Search:** Implement logic to search Memory → Active SSTables → Level 0 Files.

### Phase 4: Optimization
* [ ] **Bloom Filters:** Implement probabilistic data structures to skip unnecessary disk I/O.
* [ ] **Compaction:** Merge small SSTables into larger ones to reclaim space and handle key deletions.

---

## Getting Started

### Prerequisites
* Windows 10/11
* **MSYS2 UCRT64** Environment
* CMake (MSYS2 version)

### Build & Run
```bash
# Clean and build using the UCRT64 terminal
mkdir build && cd build
cmake .. -G "MinGW Makefiles"
cmake --build .

# Run the CLI
./lsm_cli.exe