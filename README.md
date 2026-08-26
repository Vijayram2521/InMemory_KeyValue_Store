# 🚀 LSM-Tree Key-Value Store

A persistent key-value storage engine built from scratch in C++, implementing a **Log-Structured
Merge-Tree (LSM-Tree)** — the design behind RocksDB, Cassandra, and similar systems. Optimized for
high write throughput and crash-safe durability, with an optional distributed cluster mode.

## Status

**Single-node engine: feature-complete.** WAL, MemTable, SSTables with full indexing, tombstones,
Bloom filters, and background compaction are all implemented, unit-tested, and perf-benchmarked.

**Distributed cluster: core implemented and benchmarked.** A leader + compute-node architecture
with consistent-hash routing is deployed and correctness-verified via Docker Compose. Resize,
virtual nodes, failover, and replication are not yet built (see Roadmap).

Not started: value compression, snapshots, size-tiered compaction.

## Features

- **Write-Ahead Log** — every write is appended and fsync'd before touching memory; full crash recovery via WAL replay.
- **MemTable → SSTable flush** — an in-memory `std::map` flushes to an immutable, sorted on-disk file once it crosses `memtable-threshold`.
- **Tombstones** — deletes are markers, not erasures, so they correctly shadow older values across files.
- **Manifest** — tracks which SSTable generations exist and in what order; rebuilds engine state on restart.
- **Indexed point lookups** — every SSTable carries a full `key → byte offset` index; `Get` binary-searches it and does exactly one seek, never a linear scan.
- **Bloom filters** — one per SSTable, checked before the file is even opened, to skip files that provably don't contain a key.
- **Background compaction** — a lock-free merge of the two oldest SSTable generations at a time, bounding disk growth and generation count over time. Opt-in via `--enable-compaction` / `ENABLE_COMPACTION=1`.
- **Distributed cluster** — a leader node routes `Put`/`Get`/`Delete` to one of several compute nodes via consistent hashing; each compute node is an unmodified `StorageEngine` instance serving its own shard over a hand-rolled binary TCP protocol.
- **Thread safety** — `std::shared_mutex` per engine instance (single-writer, multi-reader); compaction never blocks readers.
- **Test suite** — 36 engine checks + 43 cluster checks, all wired into CTest.

## Architecture

```
Put(k,v) / Delete(k)                       Get(k)
        |                                     |
        v                                     v
   WAL append                        Tombstone set? -> absent
        |                                     |
   MemTable (std::map)                 MemTable? -> return value
        |                                     |
   size >= threshold?             SSTables, newest -> oldest:
        |                            Bloom filter says "no"? -> skip file
        v                            Index binary search -> seek -> read
   Flush to SSTable
   [data | index | footer]
        |
   Manifest += new generation
```

Each SSTable file is `[data block][index block][20-byte footer]`. Puts and tombstones are merged
into one ascending-key sequence at flush time, so the index — and binary search over it — stays
valid regardless of record type. A lookup seeks straight to the footer, binary-searches the index
in memory, then does exactly one more seek to read the record. No file is ever scanned linearly.

**Compaction** runs as a background thread that repeatedly merges the two globally-oldest surviving
generations (never the newest), dropping tombstones and superseded values. The merge itself holds
no lock — SSTables are immutable once written, so reading two of them to build a third needs no
coordination with concurrent `Get`s — only the final in-memory/Manifest swap takes a brief exclusive
lock. A size-ratio gate skips merging a pair once one file is more than 4x the other's size, to
bound the cost of any single merge pass; this means a given pair can stop progressing once that
ratio is crossed (a real limitation — size-tiered compaction would do better, see Roadmap).

### Distributed cluster

```
Client -> Leader (consistent-hash routing) -> Compute-1 / Compute-2 / Compute-3
                                                (each an independent StorageEngine shard,
                                                 own WAL/SSTables/Bloom filters, own lock)
```

The leader relays raw request bytes to whichever compute node a key's hash lands on, decoding
just enough to extract the routing key. Compute nodes require no changes to `StorageEngine`
itself — the sharding layer sits entirely on top of it, with zero new locking anywhere in the
cluster layer (each node still has exactly the one lock it always had). No replication yet: each
key lives on exactly one node.

## Performance

Measured on Oracle Cloud (Ampere A1, ARM64) with the OS page cache explicitly dropped before each
read/mixed phase (`--drop-caches-before-read`), so results are cold-started and comparable across
rows — an important control: without it, a read phase immediately following its own write phase
can benefit enormously from an already-warm cache, which earlier in this project's history produced
a misleadingly large "compaction win" that turned out to be a cache artifact, not an architectural
one. All rows: 3,000,000 writes, 300,000 deletes, 10,000,000 reads, values 512-4096B, 4 threads.

| Configuration | WPS | RPS |
|---|---|---|
| Single-node | 17,515 | 55,240 |
| Single-node, with compaction | 17,600 | 55,001 |
| Distributed, 3 nodes | 11,065 | 4,660 |
| Distributed, 3 nodes, with compaction | 6,190 | 4,702 |
| Distributed, 2 nodes, with compaction | 7,342 | 4,560 |

**Compaction doesn't measurably improve read throughput on this hardware.** With 31GB of RAM
against a ~7GB dataset, the read phase's own page cache fills up regardless of how many SSTable
files exist, making file count irrelevant. Compaction's real, demonstrated value here is bounding
disk growth and generation count over time — not raw throughput, on hardware with this much spare
RAM.

**Distributed write throughput drops under compaction (11,065 → 6,190 WPS) because of resource
contention, not the architecture.** All 4 containers share one VM's CPU and disk with no isolation.
A 2-node control test confirms this: fewer compute nodes (and one fewer compaction thread) partly
recovers write throughput (+18.6%), while read throughput doesn't move the same way, since each
surviving node now also holds more data. On genuinely separate machines — no sibling containers
competing for the same cores or disk queue — expect distributed WPS to climb well past the current
throttled range (plausibly toward the ~20K single-node figures scaled across 3 independent shards)
and RPS to recover much closer to single-node's ~55K, since the contention suppressing both today
has nothing to do with sharding itself.

**Mixed workload** (interleaved reads/writes/deletes at higher scale — 50M reads, 5M writes, 1M
deletes, all genuinely concurrent, not sequential phases — with compaction on throughout):

| Configuration | Ops/sec (56M total) | Wall time |
|---|---|---|
| Single-node | 62,173 | 15.0 min |
| Distributed, 3 nodes | 4,952 | 3h 8min |

Same story as above: single-node is contention-free and cache-assisted; distributed is bottlenecked
by 4 containers sharing one VM. Both runs completed cleanly with correct, internally-consistent
results — no data lost, no crashes, purely a throughput gap.

Reproduce: `./build/kv_benchmark --writes=3000000 --deletes=300000 --reads=10000000
--min-value-size=512 --max-value-size=4096 --memtable-threshold=100000 --threads=4
--drop-caches-before-read [--enable-compaction]` for single-node;
`ENABLE_COMPACTION=1 docker compose up --build -d` then the equivalent `cluster_benchmark` command
for distributed (`docker-compose.2node.yml` for the 2-node variant). Add
`--mixed-reads=50000000 --mixed-writes=5000000 --mixed-deletes=1000000` to either to run the mixed
workload instead of a plain read phase.

## Getting Started

**Prerequisites:** CMake 3.15+, a C++17 compiler (GCC 9+), Docker + Compose v2. Targets Linux —
the distributed layer uses POSIX sockets directly.

```bash
# Build everything and run the test suite
cmake -S . -B build && cmake --build build -j
ctest --test-dir build --output-on-failure
```

This builds `kv_tests` (36 checks), `kv_cluster_tests` (43 checks), `kv_benchmark` (single-node
throughput), and `cluster_benchmark` (distributed throughput).

```bash
# Single-node benchmark
./build/kv_benchmark --help

# Distributed cluster
docker compose up --build -d
./build/cluster_benchmark --writes=1000000 --reads=1000000
```

There's no interactive CLI — `StorageEngine` is used as an in-process library (see `src/main.cpp`)
or served over the network via `compute_node`/`leader_node`.

## Roadmap

- [x] WAL, MemTable, SSTable flush, tombstones, Manifest
- [x] Full sorted index + binary-search point lookups
- [x] Bloom filters
- [x] Background compaction (size-ratio-gated, lock-free merge)
- [x] Leader + compute-node distributed cluster, consistent hashing
- [x] Cold-cache-controlled benchmarking methodology
- [ ] Size-tiered compaction (current gate can permanently stall a pair; tiering would let smaller generations merge among themselves first)
- [ ] Value compression (LZ4, per-value)
- [ ] Snapshots
- [ ] Cluster resize (dynamic add/remove of compute nodes)
- [ ] Virtual nodes (real load balance across a small node count)
- [ ] Failover (route around an unreachable compute node instead of failing loud)
- [ ] Replication (currently one copy per key — losing a node's volume loses that shard)
- [ ] Re-run distributed benchmarks across genuinely separate machines, not one shared VM
