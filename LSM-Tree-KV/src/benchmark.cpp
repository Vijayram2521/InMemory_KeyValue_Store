// Standalone throughput benchmark for kv_engine::StorageEngine.
//
// Runs two timed phases against a single engine instance:
//   1. Write phase: `writes` unique Puts, split across `threads`.
//   2. Read phase:  `reads` Gets, split across `threads`, where a
//      `miss-rate` fraction target keys that were never written (a disjoint
//      key prefix guarantees a true miss rather than relying on chance).
//
// Prints machine-parseable KEY=VALUE lines so run scripts can pull WPS/RPS
// straight out of stdout.
//
// ---------------------------------------------------------------------------
// Sizing the default workload for a 2GB container (comprehensive calculation)
// ---------------------------------------------------------------------------
// Every value is a uniformly random size in [kDefaultMinValueSize,
// kDefaultMaxValueSize] -- deliberately wide (16x) so no single record size
// dominates the dataset ("no monotonicity": a fixed --value-size would make
// every on-disk record identical, which is not how real workloads look and
// makes per-record cost trivially predictable).
//
// Step 1 -- calibrate the real bottleneck. SSTable::search_file (sstable.cpp)
// reopens the file and does a full linear scan on *every* Get call; there is
// no sparse index or bloom filter yet (Phase 4/5 on the README roadmap).
// Measured locally: a single-generation, 100,000-record SSTable (~11.8MB, at
// the OLD fixed 100B value size) averaged 116.5ms per Get (40 reads / 4.66s,
// single-threaded) => ~1.165us of scan cost per record in the file. That
// number is what actually limits how large a workload this benchmark can run
// in a reasonable amount of time -- see kCalibratedScanCostPerRecordUs below.
//
// Step 2 -- pick a target dataset size relative to the 2GB container. We
// target ~512MiB of SSTable data on disk: a quarter of the container's
// budget, "sizeable" enough to be a genuine multi-hundred-MB stress test (vs.
// the old 2000-key default, ~250KB), while leaving 75% of the 2GB headroom
// for the OS baseline, thread stacks, the read-phase's shuffled op-plan
// vector, and (if the OS caches read SSTable pages) page-cache pressure.
//
//   avg on-disk record size = 9B framing (1B type + 4B keyLen + 4B valLen)
//                            + ~10B average key ("key_" + digits)
//                            + avg(min,max) value bytes
//                            = 9 + 10 + avg(64,1024) = 9 + 10 + 544 = 563B
//   target writes = 512MiB / 563B = 536,870,912 / 563 ~= 953,590
//   => rounds to a clean 1,000,000 writes.
//
// Verified empirically after implementing this: 1,000,000 writes produced
// 4 SSTables totaling 562.9MB (~26% of 2GB) -- matches the calculation to
// within 0.4%. Worth knowing: the engine currently leaves each generation's
// WAL segment on disk after flushing it (nothing deletes/compacts stale
// logs -- that's essentially part of the Phase 5 "Compaction" roadmap item),
// so *total* disk usage for this run is closer to 1.13GB, not 563MB. That
// doesn't affect the RAM/OOM analysis in Step 3 below (WAL files aren't
// memory-resident), but matters if you're also watching container disk
// quota, not just memory.
//
// Step 3 -- bound peak RAM (the actual thing that could OOM the container).
// The memtable (std::map<string,string> + a tombstone set) holds at most
// `memtable-threshold` records before it's flushed and cleared, so *that*,
// not total writes, is what determines peak in-flight memory. Budgeting
// generously for map-node + std::string heap overhead (~1.5-2x raw bytes):
// 250,000 records x ~600B/record (avg) ~= 150MB peak memtable, ~7% of the
// 2GB budget -- comfortable headroom. That threshold also splits the 1M
// writes into 4 SSTable generations, which calibration showed barely
// affects read cost at this scale (record-scan time dominates over the
// per-file open() cost once files are non-trivially sized).
//
// Step 4 -- keep the DEFAULT run finishing in a reasonable time. Read cost
// is intrinsically ~O(total records already written) per Get under the
// current linear-scan design (miss => scan every generation fully; hit =>
// scan ~half the dataset on average): the model predicts average Get latency
// ~= 0.575 * writes * kCalibratedScanCostPerRecordUs, i.e. ~670ms/read
// single-threaded at 1,000,000 writes. So the DEFAULT --reads is
// intentionally modest (100) to keep a default invocation bounded.
//
// Verified empirically: a full default run (1,000,000 writes, 100 reads, 4
// threads) took ~17.7s to write (~56K WPS) and ~57s to read (~1.75 RPS,
// i.e. ~570ms/read) -- matching the single-threaded prediction almost
// exactly, because 4-way read parallelism barely helped here (~1.2x, not
// 4x): each Get is dominated by sequential file-scan CPU work under one
// shared_lock-held file handle per thread, which this environment's
// disk/scheduler didn't parallelize well. Total default run: ~75 seconds.
// Raise --reads yourself if you want a longer, more statistically stable
// run (and are willing to wait roughly writes * 0.575us per extra read).
#include "engine/storage_engine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace kv_engine;
namespace fs = std::filesystem;

namespace {

// See the file-level comment above for how these were derived.
constexpr size_t kDefaultWrites = 1'000'000;
constexpr size_t kDefaultReads = 100;
constexpr size_t kDefaultMemtableThreshold = 250'000;
constexpr size_t kDefaultMinValueSize = 64;
constexpr size_t kDefaultMaxValueSize = 1024;
constexpr double kDefaultMissRate = 0.15;
constexpr unsigned kDefaultThreads = 4;
constexpr double kCalibratedScanCostPerRecordUs = 1.165;

struct BenchConfig {
    std::string data_dir = "./bench_data";
    size_t writes = kDefaultWrites;
    size_t reads = kDefaultReads;
    size_t min_value_size = kDefaultMinValueSize;
    size_t max_value_size = kDefaultMaxValueSize;
    double miss_rate = kDefaultMissRate;
    size_t memtable_threshold = kDefaultMemtableThreshold;
    unsigned threads = kDefaultThreads;
    bool keep_data = false;
};

void print_usage() {
    std::cout <<
        "kv_benchmark - measures write/read throughput (ops/sec) of the LSM-Tree KV store\n\n"
        "Usage: kv_benchmark [options]\n"
        "  --writes=N              number of Put operations to time (default "
              << kDefaultWrites << ")\n"
        "  --reads=N               number of Get operations to time (default "
              << kDefaultReads << ")\n"
        "  --min-value-size=N      lower bound, in bytes, of each written value (default "
              << kDefaultMinValueSize << ")\n"
        "  --max-value-size=N      upper bound, in bytes, of each written value (default "
              << kDefaultMaxValueSize << ")\n"
        "                          each value's size is drawn uniformly from\n"
        "                          [min-value-size, max-value-size] -- values are never a\n"
        "                          single fixed size, so there's no artificial monotonicity\n"
        "  --miss-rate=F           fraction of reads (0.0-1.0) that target keys never\n"
        "                          written, i.e. guaranteed misses (default "
              << kDefaultMissRate << ")\n"
        "  --memtable-threshold=N  entries buffered before a flush to a new SSTable\n"
        "                          (default " << kDefaultMemtableThreshold << "; the engine's\n"
        "                          own default is 5, which is fine for unit tests but would\n"
        "                          produce one SSTable per few writes at benchmark scale)\n"
        "  --threads=N             worker threads for each phase (default "
              << kDefaultThreads << ")\n"
        "  --data-dir=PATH         engine data directory (default ./bench_data)\n"
        "  --keep-data             do not delete data-dir when done\n"
        "  --help                  show this message\n\n"
        "Note: every SSTable lookup opens the file and does a full linear scan (sparse\n"
        "indexing / bloom filters are future roadmap items), so read cost is roughly\n"
        "proportional to total records written so far, not just to the number of SSTable\n"
        "files. Calibrated locally at ~" << kCalibratedScanCostPerRecordUs
              << "us of scan time per record in a file; expected average Get latency is\n"
        "roughly 0.575 * writes * " << kCalibratedScanCostPerRecordUs
              << "us (see the file-level comment in benchmark.cpp for the full derivation).\n"
        "Raise --writes or --reads deliberately, not by accident -- they directly trade off\n"
        "against how long the read phase takes.\n";
}

bool parse_flag(const std::string& arg, const std::string& name, std::string& out) {
    std::string prefix = "--" + name + "=";
    if (arg.rfind(prefix, 0) == 0) {
        out = arg.substr(prefix.size());
        return true;
    }
    return false;
}

BenchConfig parse_args(int argc, char** argv) {
    BenchConfig cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string val;
        if (arg == "--help" || arg == "-h") {
            print_usage();
            std::exit(0);
        } else if (parse_flag(arg, "writes", val)) {
            cfg.writes = std::stoull(val);
        } else if (parse_flag(arg, "reads", val)) {
            cfg.reads = std::stoull(val);
        } else if (parse_flag(arg, "min-value-size", val)) {
            cfg.min_value_size = std::stoull(val);
        } else if (parse_flag(arg, "max-value-size", val)) {
            cfg.max_value_size = std::stoull(val);
        } else if (parse_flag(arg, "miss-rate", val)) {
            cfg.miss_rate = std::stod(val);
        } else if (parse_flag(arg, "memtable-threshold", val)) {
            cfg.memtable_threshold = std::stoull(val);
        } else if (parse_flag(arg, "threads", val)) {
            cfg.threads = static_cast<unsigned>(std::stoul(val));
        } else if (parse_flag(arg, "data-dir", val)) {
            cfg.data_dir = val;
        } else if (arg == "--keep-data") {
            cfg.keep_data = true;
        } else {
            std::cerr << "Unknown argument: " << arg << " (--help for usage)\n";
            std::exit(1);
        }
    }
    if (cfg.threads == 0) cfg.threads = 1;
    if (cfg.miss_rate < 0.0 || cfg.miss_rate > 1.0) {
        std::cerr << "--miss-rate must be between 0.0 and 1.0\n";
        std::exit(1);
    }
    if (cfg.memtable_threshold == 0) cfg.memtable_threshold = 1;
    if (cfg.min_value_size == 0) cfg.min_value_size = 1;
    if (cfg.max_value_size < cfg.min_value_size) {
        std::cerr << "--max-value-size must be >= --min-value-size\n";
        std::exit(1);
    }
    return cfg;
}

// Every value gets its own random size in [min_size, max_size] AND random
// content -- no two writes are alike, so nothing about the dataset is
// monotonic/uniform the way a single fixed --value-size would be.
std::string make_value(size_t min_size, size_t max_size, std::mt19937_64& rng) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<size_t> size_dist(min_size, max_size);
    std::uniform_int_distribution<size_t> char_dist(0, sizeof(charset) - 2);
    std::string s(size_dist(rng), '0');
    for (auto& c : s) c = charset[char_dist(rng)];
    return s;
}

// "key_" and "miss_" are disjoint prefixes: hit_key(i) is always something the
// write phase put, miss_key(i) is never written by any phase, so miss reads
// are guaranteed real misses rather than "probably not there".
std::string hit_key(size_t i)  { return "key_"  + std::to_string(i); }
std::string miss_key(size_t i) { return "miss_" + std::to_string(i); }

// Splits [0, total) into `parts` contiguous, near-equal ranges.
std::vector<std::pair<size_t, size_t>> split_range(size_t total, unsigned parts) {
    std::vector<std::pair<size_t, size_t>> ranges;
    size_t base = total / parts;
    size_t rem = total % parts;
    size_t start = 0;
    for (unsigned p = 0; p < parts; ++p) {
        size_t count = base + (p < rem ? 1 : 0);
        ranges.emplace_back(start, start + count);
        start += count;
    }
    return ranges;
}

struct WriteResult {
    double seconds = 0.0;
    size_t total_value_bytes = 0;
};

WriteResult run_write_phase(StorageEngine& engine, const BenchConfig& cfg) {
    auto ranges = split_range(cfg.writes, cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> total_value_bytes{0};

    auto t0 = std::chrono::steady_clock::now();
    for (unsigned t = 0; t < cfg.threads; ++t) {
        auto [lo, hi] = ranges[t];
        workers.emplace_back([&engine, &cfg, &total_value_bytes, lo, hi, t] {
            std::mt19937_64 rng(1000 + t);
            size_t local_bytes = 0;
            for (size_t i = lo; i < hi; ++i) {
                std::string value = make_value(cfg.min_value_size, cfg.max_value_size, rng);
                local_bytes += value.size();
                engine.Put(hit_key(i), value);
            }
            total_value_bytes += local_bytes;
        });
    }
    for (auto& w : workers) w.join();
    auto t1 = std::chrono::steady_clock::now();

    WriteResult r;
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.total_value_bytes = total_value_bytes.load();
    return r;
}

struct ReadResult {
    double seconds = 0.0;
    size_t observed_hits = 0;
    size_t observed_misses = 0;
};

ReadResult run_read_phase(StorageEngine& engine, const BenchConfig& cfg) {
    size_t miss_count = static_cast<size_t>(std::llround(
        static_cast<double>(cfg.reads) * cfg.miss_rate));
    if (miss_count > cfg.reads) miss_count = cfg.reads;
    size_t hit_count = cfg.reads - miss_count;

    // Build the op plan up front (not timed) so the timed section is pure Get() calls.
    std::vector<std::string> plan;
    plan.reserve(cfg.reads);

    std::mt19937_64 plan_rng(42);
    std::uniform_int_distribution<size_t> key_dist(0, cfg.writes == 0 ? 0 : cfg.writes - 1);
    for (size_t i = 0; i < hit_count; ++i) {
        plan.push_back(hit_key(cfg.writes == 0 ? 0 : key_dist(plan_rng)));
    }
    for (size_t i = 0; i < miss_count; ++i) {
        plan.push_back(miss_key(i));
    }
    std::shuffle(plan.begin(), plan.end(), plan_rng);

    auto ranges = split_range(plan.size(), cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> hits{0}, misses{0};

    auto t0 = std::chrono::steady_clock::now();
    for (unsigned t = 0; t < cfg.threads; ++t) {
        auto [lo, hi] = ranges[t];
        workers.emplace_back([&engine, &plan, &hits, &misses, lo, hi] {
            size_t local_hits = 0, local_misses = 0;
            for (size_t i = lo; i < hi; ++i) {
                auto v = engine.Get(plan[i]);
                if (v.has_value()) ++local_hits; else ++local_misses;
            }
            hits += local_hits;
            misses += local_misses;
        });
    }
    for (auto& w : workers) w.join();
    auto t1 = std::chrono::steady_clock::now();

    ReadResult r;
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.observed_hits = hits.load();
    r.observed_misses = misses.load();
    return r;
}

} // namespace

int main(int argc, char** argv) {
    BenchConfig cfg = parse_args(argc, argv);

    if (fs::exists(cfg.data_dir)) fs::remove_all(cfg.data_dir);
    fs::create_directories(cfg.data_dir);

    std::cout << "=== kv_benchmark ===\n"
              << "writes=" << cfg.writes
              << " reads=" << cfg.reads
              << " value_size=[" << cfg.min_value_size << "-" << cfg.max_value_size << "]B"
              << " miss_rate=" << cfg.miss_rate
              << " memtable_threshold=" << cfg.memtable_threshold
              << " threads=" << cfg.threads
              << " data_dir=" << cfg.data_dir << "\n\n";

    WriteResult wr;
    ReadResult rr;
    {
        // Scoped so the engine (and its open WAL file handle) is destroyed
        // before we try to remove_all(data_dir) below -- Windows refuses to
        // delete a directory containing a file another handle still has open.
        StorageEngine engine(cfg.data_dir, cfg.memtable_threshold);
        wr = run_write_phase(engine, cfg);
        rr = run_read_phase(engine, cfg);
    }
    double wps = wr.seconds > 0.0 ? static_cast<double>(cfg.writes) / wr.seconds : 0.0;
    double rps = rr.seconds > 0.0 ? static_cast<double>(cfg.reads) / rr.seconds : 0.0;
    double observed_miss_rate = cfg.reads > 0
        ? static_cast<double>(rr.observed_misses) / static_cast<double>(cfg.reads)
        : 0.0;
    double avg_value_bytes = cfg.writes > 0
        ? static_cast<double>(wr.total_value_bytes) / static_cast<double>(cfg.writes)
        : 0.0;

    std::cout << "--- Results ---\n";
    std::cout << "WRITE_OPS=" << cfg.writes << "\n";
    std::cout << "WRITE_SECONDS=" << wr.seconds << "\n";
    std::cout << "WPS=" << wps << "\n";
    std::cout << "TOTAL_VALUE_BYTES=" << wr.total_value_bytes << "\n";
    std::cout << "AVG_VALUE_BYTES=" << avg_value_bytes << "\n";
    std::cout << "READ_OPS=" << cfg.reads << "\n";
    std::cout << "READ_SECONDS=" << rr.seconds << "\n";
    std::cout << "RPS=" << rps << "\n";
    std::cout << "READ_MISS_RATE_REQUESTED=" << cfg.miss_rate << "\n";
    std::cout << "READ_MISS_RATE_OBSERVED=" << observed_miss_rate << "\n";
    std::cout << "READ_HITS=" << rr.observed_hits << "\n";
    std::cout << "READ_MISSES=" << rr.observed_misses << "\n";

    if (!cfg.keep_data) {
        fs::remove_all(cfg.data_dir);
    }
    return 0;
}
