# 🚀 LSM-Tree Key-Value Store

A high-performance, persistent Key-Value storage engine built from scratch in C++. This project implements a **Log-Structured Merge-Tree (LSM-Tree)** architecture, modeled after industry standards like RocksDB and AlloyDB, optimized for high write throughput and durable storage.

## 📊 Project Status: Phase 4 Complete (Indexed Search) + Distributed Cluster (implemented, correctness-verified, and perf-validated against the single-node baseline)
We have successfully transitioned from a volatile in-memory store to a durable engine capable of surviving crashes, managing on-disk sorted files, and finding a key in any of them without scanning the file. On top of that, a distributed leader + compute-node architecture now exists, is fully implemented, verified correct end to end (real `docker compose` deployment, real cross-node routing), and has been perf-tested at the same scale as the single-node "biggest" stress benchmark -- see "📈 Performance Results" below for the real numbers.

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
* **Distributed Cluster:** A leader node routes PUT/GET/DELETE to one of several compute nodes via consistent hashing; each compute node is just an unmodified `StorageEngine` serving its own keyspace shard over a hand-rolled binary TCP protocol. See "Distributed Architecture" below for the design and `cluster_benchmark`'s real, measured numbers against the live deployed cluster.

---

## 📈 Performance Results

This is the one place in this README that states the current headline throughput numbers -- other
sections below cover methodology, bug fixes, and per-scenario detail, but always point back here
rather than restating figures. Updated as an evolving record each time a change might move the
needle; older rows stay for reference rather than being overwritten.

All rows below are the *same* workload -- the "biggest" stress scenario: 3,000,000 writes, 300,000
deletes (10%), then 10,000,000 reads, values 512-4096B (mean ~2304B), `memtable-threshold=100,000`,
4 client threads, real hardware (Oracle Cloud Ampere A1, ARM64) -- not the smaller "large" scenario,
which comfortably fits in page cache and doesn't stress anything. Latency = `threads / throughput`
(4 concurrent threads on both sides), not `1 / throughput`.

| Stage | WPS | DPS | RPS | Write latency | Delete latency | Read latency |
|---|---|---|---|---|---|---|
| **Single-node** (Phase 5, with Bloom filters) | 11,494.5 | 181,539.8 | 4,412.24 | 348.0us | 22.0us | 906.6us |
| **Multi-node** (Phase 7, 3-node sharded cluster) | 11,072.4 | 23,596.7 | **4,754.23** | 361.3us | 169.5us | **841.4us** |

**What this shows:** sharding helps the workload it was actually built for. The single-node run puts
all 3,000,000 keys through one `StorageEngine` (33 SSTable generations, ~18GB); the multi-node run
splits the same 3,000,000 keys across 3 compute nodes via consistent hashing (~1,000,000 keys/node,
so each node holds a third of the data and proportionally fewer generations). Read throughput went
up ~7.8% (4,412 -> 4,754 RPS) *despite* paying for two Docker-bridge-network hops per request
(client -> leader -> compute-node) plus wire-protocol encode/decode on top -- confirming the Bloom
filter finding further below (Phase 5 in the Roadmap) that per-node disk I/O pressure, not
`open()`-call count, is the real bottleneck: reducing the data one node has to serve helps, even
after paying a real network tax. It's a genuine but modest win, not a dramatic one -- 3 nodes doesn't
mean 3x, because the underlying per-node I/O bottleneck doesn't vanish, there's just less of it per
node.

Writes came out roughly flat (11,494 -> 11,072 WPS, -3.7%): each write still serializes through
exactly one shard's own exclusive lock, so per-shard write cost is unchanged, and throughput here is
still gated by the benchmark's 4 client threads rather than server capacity. Deletes got measurably
worse (181,540 -> 23,597 DPS, ~7.7x slower): a delete is nearly free on a single node (WAL append +
in-memory map update, no value I/O at all), so the ~150-170us network round trip that's a rounding
error against a ~900us read becomes the dominant cost against an operation that used to take ~22us --
the network tax is roughly constant per op, so it hurts cheap ops far more than expensive ones.

Reproduce: `docker compose up --build -d` (3 compute nodes, `MEMTABLE_THRESHOLD=100000` set in
`docker-compose.yml` to match the single-node scenario's own threshold), then
`./build/cluster_benchmark --writes=3000000 --deletes=300000 --reads=10000000 --min-value-size=512
--max-value-size=4096 --threads=4` against the leader's published port. Single-node reproduction:
`./scripts/run_benchmark_docker_biggest.sh` (see "Benchmarking (Docker)" below for full methodology
and prior scenarios).

_Next row goes here once compaction (Phase 5, next up) lands -- re-run both single-node and
multi-node identically and add a new pair of rows, not a new section._

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

## 🌐 Distributed Architecture

Motivated directly by the Bloom filter finding above: at real scale (33
generations, ~18GB dataset) the read bottleneck turned out to be disk I/O
bandwidth for genuinely necessary reads, not `open()` call count -- a
problem no single-node optimization can fix, only reducing the amount of
data any one node has to serve can. So the engine is now shardable: a thin
leader routes requests to one of several compute nodes via consistent
hashing, and each compute node is just an ordinary, unmodified
`StorageEngine` -- the sharding layer doesn't touch the storage engine at
all, it sits entirely on top of it.

```
             +--------------------------------+             
             |             Client             |             
             | Put(k,v) / Get(k) / Delete(k)  |             
             +--------------------------------+             
                              |
                              v
             +--------------------------------+             
             |      Leader (leader_node)      |             
             | accept, thread-per-connection  |             
             |     HashRing.get_node(key)     |             
             +--------------------------------+             
                              |
                              v  relay raw request/response bytes,
                                 no decode/re-encode except the routing key
+------------------------+   +------------------------+   +------------------------+
|       Compute-1        |   |       Compute-2        |   |       Compute-3        |
|  StorageEngine shard   |   |  StorageEngine shard   |   |  StorageEngine shard   |
| own WAL/SSTables/Bloom |   | own WAL/SSTables/Bloom |   | own WAL/SSTables/Bloom |
+------------------------+   +------------------------+   +------------------------+
```

### Design decisions (this phase)
* **Wire protocol:** hand-rolled binary framing (`[4B length][1B type][fields]`), mirroring the same length-prefixed record framing already used on disk in `wal.cpp`/`sstable.cpp`, rather than pulling in JSON/HTTP/gRPC -- consistent with this project's zero-external-dependency, built-from-scratch approach throughout. Same protocol both client&harr;leader and leader&harr;compute-node; the leader relays raw frame bytes in both directions and only decodes far enough to extract the routing key from a request.
* **Consistent hashing, deliberately minimal:** one ring point per node (no virtual nodes), fixed membership set once at leader startup (no live add/remove). Reuses the same FNV-1a hash `BloomFilter` already uses (extracted to `include/engine/hash_utils.h`), passed through a MurmurHash3 finalizer (`avalanche()`) -- needed for real: raw FNV-1a output isn't uniformly spread across the 64-bit space for structurally similar short strings, which routed 100% of a 50,000-key test sample to a single node before this fix (confirmed via a standalone diagnostic, not guessed at).
* **No new locking anywhere in the cluster layer.** Each compute node wraps exactly one `StorageEngine`, which already owns a `std::shared_mutex` governing all access to it; connection-handler threads call straight into its already-thread-safe `Put`/`Get`/`Delete`. One lock per physical node, exactly as before.
* **Manifest/WAL: zero sharing, by construction.** Each compute node holds a disjoint keyspace shard, so there's never a scenario where two nodes' Manifests or WALs need to coordinate -- each just runs the unmodified `StorageEngine` against its own data directory.
* **Fail loud, not silently:** if the leader can't reach the compute node a key routes to, the client gets an `ERROR_RESPONSE`, not a retry loop or a hung connection.
* **Explicitly deferred to a future phase:** virtual nodes (needed for real load balancing across few physical nodes), live add/remove of compute nodes with data rebalancing, failover, and replication. The `HashRing`/wire-protocol shapes were chosen so these are additive later, not a rewrite -- but none of them exist yet, not even as unwired dead code.

### Verified, not just implemented
* **43 new automated checks** (`kv_cluster_tests`, a separate CTest target so the core 36-check `kv_tests` binary stays exactly as fast and platform-neutral as before): `HashRing` determinism/coverage, wire-protocol round-trips for every message type plus malformed-input handling, and a full integration test that spins up 3 `ComputeNodeServer`s + 1 `LeaderServer` in-process on ephemeral ports and drives real PUT/GET/DELETE through actual TCP sockets -- including verifying, against an independently-computed `HashRing` prediction, that every one of 60 test keys landed in *exactly* the compute node consistent hashing predicts, not just that *some* node answered correctly.
* **Real `docker compose up` deployment**, not just a build that compiles: 4 containers (1 leader + 3 compute, each `mem_limit: 1.2g`), verified healthy and staying up (not crash-looping). A standalone smoke-test client -- a separate process, not part of the test suite, built and run on the VM against the actually-running leader on its published port -- drove real PUT/GET/DELETE over the real Docker network. Cross-node routing was confirmed at the deployment level too: `docker compose exec <node> ls -la /data` showed different keys landing in different compute nodes' data directories.
* **Four real bugs found and fixed during this**, each with actual evidence rather than assumed away: the FNV-1a clustering issue above; `ComputeNodeServer::stop()` hanging indefinitely because closing a listening socket from another thread doesn't reliably unblock a thread already parked in `accept()` (confirmed hung for real via `timeout 15`, fixed with `poll()`-based polling instead of a blocking `accept()` call); compute nodes crashing on startup in the real deployment (`Failed to open WAL file`) because a fresh Docker named volume is root-owned by default while the container runs as a non-root user (fixed the standard way -- bake the data directory into the image with correct ownership before the volume ever mounts there, so Docker's volume copy-up carries it over); and `compute_node` silently using `StorageEngine`'s tiny unit-test-sized default `memtable_threshold` (5) instead of a real operational one, which would have produced hundreds of thousands of SSTable generations per shard at benchmark scale -- caught by inspection before running the real benchmark, not after getting a misleading number.

### Perf: real numbers, now at the comparable scale

**See "📈 Performance Results" above for the canonical single-node vs. multi-node comparison** --
the same 3,000,000-write/300,000-delete/10,000,000-read "biggest" scenario run against both, which
is the only comparison that actually tests whether sharding helps (splitting the exact same
keyspace the single-node "biggest" benchmark stressed across the 3 nodes here, rather than
comparing against an easy, comfortably-cached dataset like the "large" scenario's ~89,470 WPS /
~351,649 RPS, which isn't a meaningful baseline for anything).

Before that run, `cluster_benchmark` (new: drives PUT/GET through a real `leader_node` over TCP,
since `kv_benchmark` only ever talks to an in-process `StorageEngine`) was also exercised at an
easier 1,000,000-key/4-generation scale as a network-overhead sanity check -- not a verdict on
sharding, since a single node has no real problem to solve at that easy scale either: 22,663.7 WPS,
21,521.1 RPS, ~176.5us/write and ~185.9us/read average latency for two Docker-bridge-network hops
(client&rarr;leader, leader&rarr;compute-node) plus wire-protocol encode/decode. Useful for
isolating roughly what the network layer alone costs, separate from any I/O-pressure difference
between the two setups.

Reproduce the easy-scale sanity check: `docker compose up --build -d`, then
`./build/cluster_benchmark --leader-port=6000 --writes=1000000 --reads=10000000`. Reproduce the
real comparable-scale result: see "📈 Performance Results" above.

---

## 🗺️ Roadmap: What's Next?

### Phase 4: High-Performance Search (Indexing) -- ✅ Complete
* [x] **Full Indexing:** Every SSTable carries a complete `Key -> Byte Offset` index, not a sparse one. The roadmap originally called for indexing every 16th record; a full index was chosen instead because the overhead is small (~3% of file size at this project's typical value sizes) and it avoids the extra "scan forward from the nearest indexed neighbor" step a sparse index requires -- simpler and strictly O(log n) with no scan component at all. Revisit sparse indexing only if index memory becomes a real constraint at a much larger scale than currently benchmarked.
* [x] **Footer Implementation:** A fixed 20-byte footer (index offset, index entry count, magic number) at a known offset from EOF lets a lookup jump straight to the index without scanning for it.
* [x] **Point Lookups:** `SSTable::search_with_index` binary-searches the cached index and does exactly one `seekg` + read on a hit; there is no scan fallback. Measured on the benchmark's default 1,000,000-write / 4-generation workload: read throughput went from ~1.75 RPS (full linear scan per lookup) to **~150-340 RPS on Windows** (native x86_64) and **~190K-363K RPS on Linux** (Docker container, ARM64, warm page cache) -- see the "Benchmarking" section below for the full numbers, environment differences, and how each was measured, and `src/benchmark.cpp`'s sizing comment for the before/after methodology.

### Phase 5: Optimization & Efficiency
* [x] **Bloom Filters:** One filter per SSTable (`include/engine/bloom_filter.h`, `src/bloom_filter.cpp`), built at flush/load time alongside the existing index and consulted in `Get` before `std::ifstream::open()`, so a `Get` can skip opening a file entirely when the filter proves the key definitely isn't in it. 5 hash probes per key (Kirsch-Mitzenmacher double-hashing: two FNV-1a hashes combined as `h1 + i*h2 mod m`, standard practice in production Bloom filters -- provably as effective as 5 genuinely independent hash functions without paying for 5 separate hash computations), 10 bits/key, landing at the standard ~1% false-positive-rate ballpark (~0.94% analytically; **0.60% measured** on a 100,000-key sample -- see `tests/test_bloom_filter.cpp`). Correctness-checked two ways: 5 dedicated unit tests (no false negatives -- a hard guarantee, checked exactly, not sampled; false-positive rate stays well under a generous bound; sizing) plus the full existing suite (36/36 checks) passing unmodified through the public `StorageEngine` API. **Perf-validated at scale, and the result is a genuinely useful negative one:** re-running the 33-generation "biggest" benchmark identically showed RPS/WPS essentially unchanged (~4,412 vs ~4,419 RPS, well within noise) -- see the "Bloom filter re-run" writeup in the Benchmarking section below for the full numbers and why. It disproves this project's original (inferred, never directly measured) assumption that `open()` call count was the dominant per-lookup cost at scale; the real cost is disk I/O for the reads that remain necessary, which a Bloom filter can reduce the *count* of but not the *volume* of. Kept in the codebase because it's still theoretically correct and cheap (and would matter more at smaller scale, or with a colder file-open path than this Linux/Docker environment has), but it's not what actually moves the needle here -- that's the evidence behind moving toward sharding across multiple nodes instead of further single-node read-path optimization.
* [ ] **Compaction (L0 -> L1):** A background (or on-demand) worker that merges multiple SSTable generations into fewer, larger ones -- discarding obsolete versions of overwritten keys and dropping tombstones once nothing older they'd shadow remains. Two things to get right: (1) **locking** -- compaction mutates the same `sstable_files` list and `index_cache` that `Get` reads under `shared_lock`, so a merge needs to build the new merged file(s) *off to the side* first and only take the `unique_lock` for the brief pointer-swap that atomically replaces the old generation list with the new one, rather than holding a write lock for the whole (potentially slow) merge; (2) it directly fixes two things this session's benchmarking exposed as real, not theoretical, problems: generation count growing unboundedly (currently the only way to keep it bounded is choosing a big `memtable-threshold` up front) and stale WAL segments never being cleaned up after their generation flushes (roughly doubles on-disk footprint today -- see the sizing math in `src/benchmark.cpp`).
* [ ] **Cold-cache benchmarking:** The Linux/Docker RPS numbers in the "Benchmarking" section above all reflect a warm OS page cache (reads immediately follow the write phase that produced the same dataset). Get a genuine cold-cache number too -- e.g. drop caches between phases, or read data written by a prior, separate `docker run` -- for a fuller "worst case" picture alongside the current "best case" numbers.

### Phase 6: Advanced Features
* [ ] **Value Compression:** Compress each value (LZ4 is the natural choice -- fast enough that decompression cost on the read path stays negligible next to the I/O costs above, unlike heavier schemes optimized for ratio over speed) before writing it into an SSTable record, decompressing on read. Per-value rather than per-block: it fits this project's existing record framing (type + kLen + key + vLen + value) without restructuring the file format, at the cost of losing the better compression ratio a shared block-level dictionary would give across many small values. Matters most for the "large values" benchmark configuration (512-4096B, see below) -- that's exactly the value-size range where compression has real bytes to work with, unlike the earlier 64-1024B default where per-value overhead would dominate any savings.
* [ ] **Snapshots:** Implement point-in-time consistent views of the database.

### Phase 7: Distributed Architecture -- ✅ Implemented, correctness-verified, and perf-validated at the real comparable scale
* [x] **Leader + compute-node sharding:** see "Distributed Architecture" above for the full design, verification, and benchmark writeup. Consistent-hash routing, hand-rolled binary wire protocol, real `docker compose` deployment (4 containers, 1.2GB each), 43 new automated checks, 4 real bugs found and fixed with evidence.
* [x] **Network-overhead sanity check run** (1M writes/10M reads against the live deployment): 22,663.7 WPS / 21,521.1 RPS, ~176.5us/~185.9us average write/read latency. Useful for knowing roughly what two network hops cost in isolation.
* [x] **The comparison that actually answers whether sharding helps:** the same 3,000,000-key total keyspace the "biggest" single-node benchmark used, sharded ~1,000,000 keys/node across the 3 compute nodes, run through the identical write/delete/read workload. **Result: RPS up ~7.8% (4,412 -> 4,754), WPS roughly flat (-3.7%), DPS down ~7.7x** (network tax hits cheap ops hardest) -- see "📈 Performance Results" above for the full table and reasoning.
* [ ] **Resize workflow:** dynamic add/remove of compute nodes with data rebalancing. `HashRing` was deliberately kept minimal (fixed membership, no virtual nodes) specifically so this phase can extend it rather than rewrite it.
* [ ] **Virtual nodes:** needed for real load balance across a small number of physical nodes -- deferred alongside resize since they're most useful once membership can actually change.
* [ ] **Failover:** detect and route around an unreachable compute node instead of just failing the request loud (current behavior).
* [ ] **Replication:** currently each key lives on exactly one compute node -- losing a node's volume loses that shard's data.

---

## 🛠️ Getting Started

### Prerequisites
This project targets **Unix (Linux) exclusively** -- the distributed layer
(`compute_node`/`leader_node`) uses POSIX sockets directly, and perf
testing/deployment have always been Linux-only in practice, so there's no
Windows compatibility gate to maintain.
* **CMake** 3.15+
* **C++17** compatible compiler (GCC 9+ recommended)
* **Docker** + **Docker Compose v2** (for the distributed cluster and containerized benchmarking)

### Build & Run
```bash
# From LSM-Tree-KV/, configure, build, and run the unit test suite via CTest
./scripts/run_tests.sh
```
This builds `kv_tests` (the core engine test suite, 36 checks),
`kv_cluster_tests` (the distributed-layer test suite, 43 checks -- see
"Distributed Architecture" above), `kv_benchmark` (the single-node
throughput benchmark below), and `cluster_benchmark` (the distributed
throughput benchmark, see above), all registered with CTest. There is no
interactive single-node CLI -- `StorageEngine` is used as an in-process
library directly (see `src/main.cpp` / `src/benchmark.cpp`) or served over
the network via `compute_node`/`leader_node` (see "Distributed
Architecture" above).

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

**Bloom filter re-run -- a negative result, and what it actually proves:**
after implementing Bloom filters (Phase 5 below), this exact scenario was
re-run identically (same 3,000,000 writes / 300,000 deletes / 10,000,000
reads / 33 generations / values 512-4096B) to see whether skipping
unnecessary `open()` calls would move the needle. It didn't, meaningfully:

| Metric | Before Bloom filters | After Bloom filters | Delta |
|---|---|---|---|
| WPS | 11,479.9 | 11,494.5 | +0.13% (noise) |
| Write latency | 87.10us | 86.998us | ~0 |
| DPS | 149,930.5 | 181,539.8 | +21% (deletes don't touch the read path at all -- run-to-run system noise, not a Bloom filter effect) |
| RPS | 4,419.1 | 4,412.24 | -0.16% (noise) |
| Read latency | 226.29us | 226.642us | ~0 |
| Block I/O (read) | ~95GB | ~89GB | ~6% less |

The small I/O reduction shows the filter *is* doing its job (skipping some
opens), but not enough to matter, which means the original hypothesis this
project shipped Bloom filters to fix -- that `open()` call count was the
dominant per-lookup cost -- was an *inference*, never directly measured, and
turns out to be wrong at this scale. If it had been right, skipping ~93% of
unnecessary generation touches (the Bloom filter's own unit-tested ~0.6%
false-positive rate implies almost every non-matching generation gets
correctly skipped) should have produced a large speedup, not a rounding
error. The real cost is the disk I/O for the touches that remain
*necessary* -- pulling actual record bytes from an ~18GB working set that
doesn't comfortably fit in page cache, plausibly amplified by OS readahead
pulling more than the ~2.3KB a single record needs. A Bloom filter can only
eliminate *unnecessary* touches; it can't shrink the *necessary* data volume
or make the disk faster. That's a real, useful negative result, not a
wasted implementation -- it rules out one hypothesis with actual evidence
and points at the right one, which is why this project's next direction is
sharding the keyspace across multiple nodes rather than further
single-node read-path micro-optimization (design in progress -- not yet a
numbered roadmap phase below, pending alignment on the node topology).

Either way, the index itself (see Phase 4 above) remains a **~2-5 order of
magnitude** improvement over the pre-index ~1.75 RPS baseline this project
started from. Raise `WRITES` or `READS` deliberately, not by accident --
they still cost time, just far less than before the index existed.