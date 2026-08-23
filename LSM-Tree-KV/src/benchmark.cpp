// Standalone throughput benchmark for kv_engine::StorageEngine.
//
// Runs up to three timed phases against a single engine instance:
//   1. Write phase:  `writes` unique Puts, split across `threads`.
//   2. Delete phase: `deletes` previously-written keys (sampled without
//      replacement), split across `threads`. Opt-in -- skipped entirely
//      when `deletes` is 0 (the default), so existing invocations are
//      unaffected. Exercises the tombstone path that Get and read-phase
//      throughput otherwise never touch.
//   3. Read phase:   `reads` Gets, split across `threads`, where a
//      `miss-rate` fraction target keys that were never written (a disjoint
//      key prefix guarantees a true miss rather than relying on chance).
//      If a deleted key is randomly selected as a "hit" target, Get
//      correctly returns nullopt -- that's real tombstone behavior, not a
//      bug, so the observed hit rate reads a bit below --miss-rate's
//      complement once --deletes is nonzero.
//
// Prints machine-parseable KEY=VALUE lines so run scripts can pull
// WPS/DPS/RPS straight out of stdout.
//
// ---------------------------------------------------------------------------
// Sizing the default workload for a 2GB container (comprehensive calculation)
// ---------------------------------------------------------------------------
// NOTE: the container's own --memory cap was later raised to 4GB (see
// Dockerfile / scripts/run_benchmark_docker.*) for more headroom on the
// verification VM. The workload's sizing math below still targets the
// original 2GB reference budget deliberately -- it doesn't need to grow
// just because the container got roomier, so treat every "~X% of 2GB"
// figure below as "~half that, of the actual 4GB cap" if you want the
// up-to-date percentage. The absolute numbers (writes, memtable-threshold,
// value sizes) are unchanged.
// ---------------------------------------------------------------------------
// Every value is a uniformly random size in [kDefaultMinValueSize,
// kDefaultMaxValueSize] -- deliberately wide (16x) so no single record size
// dominates the dataset ("no monotonicity": a fixed --value-size would make
// every on-disk record identical, which is not how real workloads look and
// makes per-record cost trivially predictable).
//
// Step 1 -- calibrate the real bottleneck. This used to be a full linear
// scan per Get (SSTable::search_file reopened the file and scanned every
// record on every call) -- calibrated at the time at ~1.165us of scan cost
// per record in the file, i.e. read cost scaled with total records written.
// That's fixed now: every SSTable carries a full sorted index (Key -> Byte
// Offset) plus a footer, built once at flush()/startup time and cached in
// StorageEngine, not rebuilt per lookup. SSTable::search_with_index binary
// searches the cached index in memory, then does exactly one seekg + read.
// Measured locally (1,000,000 writes, 4 generations, 4 threads): read
// throughput went from ~1.75 RPS (linear scan) to ~226.8 RPS sustained
// (200,000 reads / 882s) -- a ~129x improvement, and read latency is no
// longer proportional to how much data has been written. The remaining
// ~4.4ms average per Get is now dominated by std::ifstream's open() cost
// (one open per SSTable a lookup has to touch -- up to all 4 generations
// for a guaranteed miss, which must check every generation to conclude
// "not found"), not by scanning. That's the next bottleneck to attack --
// bloom filters (Phase 5 on the README roadmap) would let a miss skip
// opening a file at all instead of just skipping the scan inside it.
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
// not total writes, is what determines peak in-flight memtable memory.
// Budgeting generously for map-node + std::string heap overhead (~1.5-2x
// raw bytes): 250,000 records x ~600B/record (avg) ~= 150MB peak memtable,
// ~7% of the 2GB budget. On top of that, StorageEngine now keeps every
// SSTable's index resident in memory for the engine's whole lifetime (see
// index_cache in storage_engine.cpp) -- unlike the memtable, this is never
// evicted, so it grows with *total* writes, not just the current
// generation: ~1,000,000 index entries x ~40B/entry (a short SSO string +
// an 8-byte offset) ~= 40MB, ~2% of the budget. Both comfortably fit; the
// index cache is the one to watch if --writes is raised far beyond the
// default, since it has no eviction policy yet.
//
// Step 4 -- pick a DEFAULT --reads that's actually statistically meaningful
// now that reads are cheap. Verified empirically (1,000,000 writes, 4
// generations, 4 threads): sustained read throughput is ~226.8 RPS
// (200,000 reads / 882s), a ~129x improvement over the ~1.75 RPS the old
// linear-scan design measured at this same scale. A too-small --reads
// (the old default of 100, chosen when every read cost ~570ms) now finishes
// in well under a second and is dominated by one-time setup noise (thread
// spawn, op-plan shuffle) rather than steady-state throughput -- not a
// useful RPS sample any more. Verified: 5,000 reads (the new default) took
// ~14.8s (~339 RPS this run -- some run-to-run variance vs. the 200,000-read
// calibration above, plausibly page-cache warmth right after the write
// phase), for a full default run (write + read) of ~31s total -- actually
// *faster* than the old default's ~75s despite sampling 50x more reads.
// Raise --reads yourself for an even more stable sample; it's cheap now.
#include "engine/storage_engine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace kv_engine;
namespace fs = std::filesystem;

namespace {

// See the file-level comment above for how these were derived.
constexpr size_t kDefaultWrites = 1'000'000;
constexpr size_t kDefaultReads = 5'000;
constexpr size_t kDefaultMemtableThreshold = 250'000;
constexpr size_t kDefaultMinValueSize = 64;
constexpr size_t kDefaultMaxValueSize = 1024;
constexpr double kDefaultMissRate = 0.15;
constexpr unsigned kDefaultThreads = 4;
constexpr double kCalibratedSustainedRps = 226.8;

struct BenchConfig {
    std::string data_dir = "./bench_data";
    size_t writes = kDefaultWrites;
    size_t reads = kDefaultReads;
    size_t deletes = 0; // opt-in: 0 means the delete phase is skipped entirely
    size_t min_value_size = kDefaultMinValueSize;
    size_t max_value_size = kDefaultMaxValueSize;
    double miss_rate = kDefaultMissRate;
    size_t memtable_threshold = kDefaultMemtableThreshold;
    unsigned threads = kDefaultThreads;
    bool keep_data = false;
    bool enable_compaction = false;
    std::chrono::seconds progress_interval{10}; // 0 disables progress lines entirely
};

void print_usage() {
    std::cout <<
        "kv_benchmark - measures write/read throughput (ops/sec) of the LSM-Tree KV store\n\n"
        "Usage: kv_benchmark [options]\n"
        "  --writes=N              number of Put operations to time (default "
              << kDefaultWrites << ")\n"
        "  --reads=N               number of Get operations to time (default "
              << kDefaultReads << ")\n"
        "  --deletes=N             number of distinct, previously-written keys to delete\n"
        "                          between the write and read phases, sampled without\n"
        "                          replacement (default 0, i.e. the delete phase is skipped\n"
        "                          entirely). Deleted keys naturally show up as misses in the\n"
        "                          read phase if randomly selected as a \"hit\" target -- that's\n"
        "                          correct behavior (the key really is gone), not a bug, so\n"
        "                          the observed hit rate will read a bit below --miss-rate's\n"
        "                          complement once --deletes is nonzero.\n"
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
        "  --progress-interval=N   seconds between \"[progress] ...\" status lines during\n"
        "                          each phase (default 10; 0 disables them). Long phases\n"
        "                          otherwise produce zero stdout output until they finish\n"
        "                          entirely, which looks identical to a hang and can also\n"
        "                          trip idle-connection timeouts on whatever's capturing\n"
        "                          this process's output over a long-lived pipe (e.g. SSH).\n"
        "  --keep-data             do not delete data-dir when done\n"
        "  --enable-compaction     run background compaction (merges the two oldest SSTable\n"
        "                          generations at a time, never the newest) throughout the\n"
        "                          write/delete/read phases, instead of leaving every flushed\n"
        "                          generation on disk forever (default: off, matching prior\n"
        "                          benchmark behavior, for an A/B-comparable baseline)\n"
        "  --help                  show this message\n\n"
        "Note: every SSTable lookup binary-searches that file's cached index and does a\n"
        "single seekg -- there's no linear scan, so read cost no longer grows with total\n"
        "records written. Calibrated locally at ~" << kCalibratedSustainedRps
              << " sustained RPS (1,000,000 writes,\n"
        "4 generations, 4 threads); the current bottleneck is one std::ifstream open() per\n"
        "SSTable a lookup has to touch (up to all of them, for a guaranteed miss), not\n"
        "scanning -- bloom filters would let a miss skip that file-open entirely.\n"
        "Raise --writes or --reads deliberately, not by accident -- they still cost time,\n"
        "just far less than before.\n";
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
        } else if (parse_flag(arg, "deletes", val)) {
            cfg.deletes = std::stoull(val);
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
        } else if (parse_flag(arg, "progress-interval", val)) {
            cfg.progress_interval = std::chrono::seconds(std::stoll(val));
        } else if (arg == "--keep-data") {
            cfg.keep_data = true;
        } else if (arg == "--enable-compaction") {
            cfg.enable_compaction = true;
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
    if (cfg.deletes > cfg.writes) cfg.deletes = cfg.writes;
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

// Prints "[progress] <phase>: done/total (pct%) elapsed=Ws" every `period`
// on a background thread, so a long phase (the read phase especially,
// which otherwise produces zero stdout output until it's entirely done)
// stays visibly alive instead of looking hung -- both to a human watching
// and to anything capturing this process's output over a connection that
// might time out during a long silent stretch. RAII: starts on
// construction, stops and joins on destruction (wrap it in a scope around
// the phase's worker loop).
class ProgressReporter {
public:
    ProgressReporter(std::string phase_name, const std::atomic<size_t>& counter, size_t total,
                      std::chrono::seconds period)
        : phase_name_(std::move(phase_name)), counter_(counter), total_(total), period_(period) {
        if (period_.count() <= 0) return; // disabled
        start_ = std::chrono::steady_clock::now();
        thread_ = std::thread([this] { run(); });
    }

    ~ProgressReporter() {
        if (!thread_.joinable()) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    ProgressReporter(const ProgressReporter&) = delete;
    ProgressReporter& operator=(const ProgressReporter&) = delete;

private:
    void run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!cv_.wait_for(lock, period_, [this] { return stop_; })) {
            size_t done = counter_.load();
            double elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - start_).count();
            double pct = total_ > 0
                ? 100.0 * static_cast<double>(done) / static_cast<double>(total_) : 0.0;
            // Build the line in a local stream, not directly on std::cout --
            // std::fixed/setprecision are persistent stream-state changes,
            // not scoped to one operator<< chain, so applying them straight
            // to std::cout here would silently truncate every subsequent
            // cout write in the whole program to 1 decimal place (this
            // genuinely happened: the final --- Results --- block came out
            // misformatted, e.g. "0.15" printed as "0.1", after this ran).
            std::ostringstream line;
            line << "[progress] " << phase_name_ << ": " << done << "/" << total_
                 << " (" << std::fixed << std::setprecision(1) << pct << "%) "
                 << "elapsed=" << std::fixed << std::setprecision(1) << elapsed << "s\n";
            std::cout << line.str() << std::flush;
        }
    }

    std::string phase_name_;
    const std::atomic<size_t>& counter_;
    size_t total_;
    std::chrono::seconds period_;
    std::chrono::steady_clock::time_point start_;
    bool stop_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
};

struct WriteResult {
    double seconds = 0.0;
    size_t total_value_bytes = 0;
};

WriteResult run_write_phase(StorageEngine& engine, const BenchConfig& cfg) {
    auto ranges = split_range(cfg.writes, cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> total_value_bytes{0};
    std::atomic<size_t> done{0};

    auto t0 = std::chrono::steady_clock::now();
    {
        ProgressReporter progress("write", done, cfg.writes, cfg.progress_interval);
        for (unsigned t = 0; t < cfg.threads; ++t) {
            auto [lo, hi] = ranges[t];
            workers.emplace_back([&engine, &cfg, &total_value_bytes, &done, lo, hi, t] {
                std::mt19937_64 rng(1000 + t);
                size_t local_bytes = 0;
                size_t local_done = 0;
                for (size_t i = lo; i < hi; ++i) {
                    std::string value = make_value(cfg.min_value_size, cfg.max_value_size, rng);
                    local_bytes += value.size();
                    engine.Put(hit_key(i), value);
                    if (++local_done % 1000 == 0) done += 1000;
                }
                done += local_done % 1000;
                total_value_bytes += local_bytes;
            });
        }
        for (auto& w : workers) w.join();
    }
    auto t1 = std::chrono::steady_clock::now();

    WriteResult r;
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.total_value_bytes = total_value_bytes.load();
    return r;
}

struct DeleteResult {
    double seconds = 0.0;
    size_t ops = 0;
};

// Deletes `cfg.deletes` distinct, previously-written keys, sampled without
// replacement. Materializes the full [0, writes) index range once and
// shuffles it rather than rejection-sampling for distinctness -- simpler
// and robust, and the memory cost (writes * 8 bytes, ~24MB at 3,000,000
// writes) is trivial next to the memtable/index cache.
DeleteResult run_delete_phase(StorageEngine& engine, const BenchConfig& cfg) {
    std::vector<size_t> indices(cfg.writes);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937_64 plan_rng(7);
    std::shuffle(indices.begin(), indices.end(), plan_rng);
    indices.resize(cfg.deletes); // cfg.deletes already clamped to <= cfg.writes

    auto ranges = split_range(indices.size(), cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> done{0};

    auto t0 = std::chrono::steady_clock::now();
    {
        ProgressReporter progress("delete", done, indices.size(), cfg.progress_interval);
        for (unsigned t = 0; t < cfg.threads; ++t) {
            auto [lo, hi] = ranges[t];
            workers.emplace_back([&engine, &indices, &done, lo, hi] {
                size_t local_done = 0;
                for (size_t i = lo; i < hi; ++i) {
                    engine.Delete(hit_key(indices[i]));
                    if (++local_done % 1000 == 0) done += 1000;
                }
                done += local_done % 1000;
            });
        }
        for (auto& w : workers) w.join();
    }
    auto t1 = std::chrono::steady_clock::now();

    DeleteResult r;
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.ops = indices.size();
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
    std::atomic<size_t> done{0};

    auto t0 = std::chrono::steady_clock::now();
    ProgressReporter progress("read", done, plan.size(), cfg.progress_interval);
    for (unsigned t = 0; t < cfg.threads; ++t) {
        auto [lo, hi] = ranges[t];
        workers.emplace_back([&engine, &plan, &hits, &misses, &done, lo, hi] {
            size_t local_hits = 0, local_misses = 0, local_done = 0;
            for (size_t i = lo; i < hi; ++i) {
                auto v = engine.Get(plan[i]);
                if (v.has_value()) ++local_hits; else ++local_misses;
                if (++local_done % 1000 == 0) done += 1000;
            }
            done += local_done % 1000;
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
              << " deletes=" << cfg.deletes
              << " reads=" << cfg.reads
              << " value_size=[" << cfg.min_value_size << "-" << cfg.max_value_size << "]B"
              << " miss_rate=" << cfg.miss_rate
              << " memtable_threshold=" << cfg.memtable_threshold
              << " threads=" << cfg.threads
              << " data_dir=" << cfg.data_dir
              << " compaction=" << (cfg.enable_compaction ? "on" : "off") << "\n\n";

    WriteResult wr;
    DeleteResult dr;
    ReadResult rr;
    {
        // Scoped so the engine (and its open WAL file handle) is destroyed
        // before we try to remove_all(data_dir) below -- Windows refuses to
        // delete a directory containing a file another handle still has open.
        StorageEngine engine(cfg.data_dir, cfg.memtable_threshold);
        if (cfg.enable_compaction) {
            engine.StartBackgroundCompaction();
        }
        wr = run_write_phase(engine, cfg);
        if (cfg.deletes > 0) {
            dr = run_delete_phase(engine, cfg);
        }
        rr = run_read_phase(engine, cfg);
    }
    double wps = wr.seconds > 0.0 ? static_cast<double>(cfg.writes) / wr.seconds : 0.0;
    double dps = dr.seconds > 0.0 ? static_cast<double>(dr.ops) / dr.seconds : 0.0;
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
    if (cfg.deletes > 0) {
        std::cout << "DELETE_OPS=" << dr.ops << "\n";
        std::cout << "DELETE_SECONDS=" << dr.seconds << "\n";
        std::cout << "DPS=" << dps << "\n";
    }
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
