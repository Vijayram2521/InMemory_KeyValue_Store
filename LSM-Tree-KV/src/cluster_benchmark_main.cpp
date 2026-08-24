// Throughput benchmark for the DISTRIBUTED topology: drives PUT/GET
// through a real leader_node over the wire protocol (not an in-process
// StorageEngine like kv_benchmark), so results are directly comparable to
// the single-node kv_benchmark numbers already measured, but reflect the
// actual network+routing path a real client would see.
//
// Methodology mirrors kv_benchmark.cpp deliberately: same hit_key/miss_key
// disjoint-prefix scheme, same variable-value-size approach, same
// build-the-op-plan-then-time-pure-Gets read phase, same delete-phase
// sampling algorithm (same seed, same "shuffle then take the first N"
// approach), same KEY=VALUE-per-line output -- so a side-by-side
// comparison against the single-node numbers is comparing the same
// workload shape, not a methodologically different one.
#include "cluster/tcp_socket.h"
#include "cluster/wire_protocol.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace kv_cluster;

namespace {

struct BenchConfig {
    std::string leader_host = "127.0.0.1";
    uint16_t leader_port = 6000;
    size_t writes = 1'000'000;
    size_t deletes = 0; // opt-in: 0 skips the delete phase entirely, matching kv_benchmark
    size_t reads = 10'000'000;
    size_t min_value_size = 64;
    size_t max_value_size = 1024;
    double miss_rate = 0.15;
    unsigned threads = 4;
    std::chrono::seconds progress_interval{10};

    // Mixed-phase: opt-in (all 0 by default -> old behavior). See
    // kv_benchmark.cpp's run_mixed_phase for the full design rationale --
    // this mirrors it exactly, just sending wire-protocol requests to the
    // leader instead of calling a local StorageEngine directly.
    size_t mixed_reads = 0;
    size_t mixed_writes = 0;
    size_t mixed_deletes = 0;

    // See kv_benchmark.cpp's BenchConfig::drop_caches_before_read for the
    // full rationale -- identical mechanism here, dropped on the VM host
    // (compute node containers share the host kernel's page cache for
    // their volume-backed files, so this evicts their cached data too).
    bool drop_caches_before_read = false;
};

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
        std::string arg = argv[i], val;
        if (parse_flag(arg, "leader-host", val)) cfg.leader_host = val;
        else if (parse_flag(arg, "leader-port", val)) cfg.leader_port = static_cast<uint16_t>(std::stoul(val));
        else if (parse_flag(arg, "writes", val)) cfg.writes = std::stoull(val);
        else if (parse_flag(arg, "deletes", val)) cfg.deletes = std::stoull(val);
        else if (parse_flag(arg, "reads", val)) cfg.reads = std::stoull(val);
        else if (parse_flag(arg, "min-value-size", val)) cfg.min_value_size = std::stoull(val);
        else if (parse_flag(arg, "max-value-size", val)) cfg.max_value_size = std::stoull(val);
        else if (parse_flag(arg, "miss-rate", val)) cfg.miss_rate = std::stod(val);
        else if (parse_flag(arg, "threads", val)) cfg.threads = static_cast<unsigned>(std::stoul(val));
        else if (parse_flag(arg, "progress-interval", val)) cfg.progress_interval = std::chrono::seconds(std::stoll(val));
        else if (parse_flag(arg, "mixed-reads", val)) cfg.mixed_reads = std::stoull(val);
        else if (parse_flag(arg, "mixed-writes", val)) cfg.mixed_writes = std::stoull(val);
        else if (parse_flag(arg, "mixed-deletes", val)) cfg.mixed_deletes = std::stoull(val);
        else if (arg == "--drop-caches-before-read") cfg.drop_caches_before_read = true;
        else if (arg == "--help" || arg == "-h") {
            std::cout << "cluster_benchmark - drives PUT/GET through a real leader_node over TCP\n"
                         "  --leader-host=HOST      (default 127.0.0.1)\n"
                         "  --leader-port=N         (default 6000)\n"
                         "  --writes=N              (default 1000000)\n"
                         "  --deletes=N             distinct previously-written keys to delete\n"
                         "                          (default 0, phase skipped entirely)\n"
                         "  --reads=N               (default 10000000)\n"
                         "  --min-value-size=N / --max-value-size=N  (default 64/1024)\n"
                         "  --miss-rate=F           (default 0.15)\n"
                         "  --threads=N             (default 4)\n"
                         "  --progress-interval=N   seconds (default 10, 0 disables)\n"
                         "  --mixed-reads=N / --mixed-writes=N / --mixed-deletes=N\n"
                         "                          if any nonzero, replaces the standalone --reads\n"
                         "                          phase with one combined phase of genuinely\n"
                         "                          interleaved GET/PUT/DELETE across all worker\n"
                         "                          threads (see kv_benchmark --help for the full\n"
                         "                          rationale -- identical design here). Default 0/0/0.\n"
                         "  --drop-caches-before-read  drop the VM host's OS page cache before the\n"
                         "                          read/mixed phase, so it measures cold reads (see\n"
                         "                          kv_benchmark --help for why this matters far more\n"
                         "                          than generation count/compaction). Default: off.\n";
            std::exit(0);
        }
    }
    if (cfg.threads == 0) cfg.threads = 1;
    if (cfg.deletes > cfg.writes) cfg.deletes = cfg.writes;
    return cfg;
}

std::string make_value(size_t min_size, size_t max_size, std::mt19937_64& rng) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<size_t> size_dist(min_size, max_size);
    std::uniform_int_distribution<size_t> char_dist(0, sizeof(charset) - 2);
    std::string s(size_dist(rng), '0');
    for (auto& c : s) c = charset[char_dist(rng)];
    return s;
}

std::string hit_key(size_t i)  { return "key_"  + std::to_string(i); }
std::string miss_key(size_t i) { return "miss_" + std::to_string(i); }

std::vector<std::pair<size_t, size_t>> split_range(size_t total, unsigned parts) {
    std::vector<std::pair<size_t, size_t>> ranges;
    size_t base = total / parts, rem = total % parts, start = 0;
    for (unsigned p = 0; p < parts; ++p) {
        size_t count = base + (p < rem ? 1 : 0);
        ranges.emplace_back(start, start + count);
        start += count;
    }
    return ranges;
}

// Same shape as kv_benchmark.cpp's ProgressReporter: periodic status on a
// background thread so a long phase never looks indistinguishable from a
// hang (and doesn't trip idle-connection timeouts on whatever's watching
// this process's output, same lesson learned there).
class ProgressReporter {
public:
    ProgressReporter(std::string phase, const std::atomic<size_t>& counter, size_t total,
                      std::chrono::seconds period)
        : phase_(std::move(phase)), counter_(counter), total_(total), period_(period) {
        if (period_.count() <= 0) return;
        start_ = std::chrono::steady_clock::now();
        thread_ = std::thread([this] { run(); });
    }
    ~ProgressReporter() {
        if (!thread_.joinable()) return;
        { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
        cv_.notify_all();
        thread_.join();
    }
private:
    void run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!cv_.wait_for(lock, period_, [this] { return stop_; })) {
            size_t done = counter_.load();
            double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start_).count();
            double pct = total_ > 0 ? 100.0 * static_cast<double>(done) / static_cast<double>(total_) : 0.0;
            std::ostringstream line;
            line << "[progress] " << phase_ << ": " << done << "/" << total_
                 << " (" << std::fixed << std::setprecision(1) << pct << "%) "
                 << "elapsed=" << std::fixed << std::setprecision(1) << elapsed << "s\n";
            std::cout << line.str() << std::flush;
        }
    }
    std::string phase_;
    const std::atomic<size_t>& counter_;
    size_t total_;
    std::chrono::seconds period_;
    std::chrono::steady_clock::time_point start_;
    bool stop_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
};

TcpSocket connect_or_die(const BenchConfig& cfg) {
    TcpSocket sock;
    if (!sock.connect(cfg.leader_host, cfg.leader_port)) {
        std::cerr << "cluster_benchmark: failed to connect to leader at "
                  << cfg.leader_host << ":" << cfg.leader_port << std::endl;
        std::exit(1);
    }
    return sock;
}

double run_write_phase(const BenchConfig& cfg) {
    auto ranges = split_range(cfg.writes, cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> done{0};

    auto t0 = std::chrono::steady_clock::now();
    {
        ProgressReporter progress("write", done, cfg.writes, cfg.progress_interval);
        for (unsigned t = 0; t < cfg.threads; ++t) {
            auto [lo, hi] = ranges[t];
            workers.emplace_back([&cfg, &done, lo, hi, t] {
                TcpSocket sock = connect_or_die(cfg);
                std::mt19937_64 rng(1000 + t);
                size_t local_done = 0;
                for (size_t i = lo; i < hi; ++i) {
                    std::string value = make_value(cfg.min_value_size, cfg.max_value_size, rng);
                    sock.send_all(encode_put_request(hit_key(i), value));
                    receive_message(sock); // discard; write correctness is proven by the integration tests
                    if (++local_done % 1000 == 0) done += 1000;
                }
                done += local_done % 1000;
            });
        }
        for (auto& w : workers) w.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(t1 - t0).count();
}

struct DeleteResult { double seconds = 0.0; size_t ops = 0; };

// Mirrors kv_benchmark.cpp's run_delete_phase exactly: same sampling
// algorithm (materialize [0,writes), shuffle with the same seed=7, take the
// first `deletes`), so the two benchmarks delete the same *shape* of key
// set relative to their own write phase, not just the same count.
DeleteResult run_delete_phase(const BenchConfig& cfg) {
    std::vector<size_t> indices(cfg.writes);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937_64 plan_rng(7);
    std::shuffle(indices.begin(), indices.end(), plan_rng);
    indices.resize(cfg.deletes);

    auto ranges = split_range(indices.size(), cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> done{0};

    auto t0 = std::chrono::steady_clock::now();
    {
        ProgressReporter progress("delete", done, indices.size(), cfg.progress_interval);
        for (unsigned t = 0; t < cfg.threads; ++t) {
            auto [lo, hi] = ranges[t];
            workers.emplace_back([&cfg, &indices, &done, lo, hi] {
                TcpSocket sock = connect_or_die(cfg);
                size_t local_done = 0;
                for (size_t i = lo; i < hi; ++i) {
                    sock.send_all(encode_delete_request(hit_key(indices[i])));
                    receive_message(sock); // discard; correctness proven by the integration tests
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

struct ReadResult { double seconds = 0.0; size_t hits = 0; size_t misses = 0; };

ReadResult run_read_phase(const BenchConfig& cfg) {
    size_t miss_count = static_cast<size_t>(std::llround(static_cast<double>(cfg.reads) * cfg.miss_rate));
    if (miss_count > cfg.reads) miss_count = cfg.reads;
    size_t hit_count = cfg.reads - miss_count;

    std::vector<std::string> plan;
    plan.reserve(cfg.reads);
    std::mt19937_64 plan_rng(42);
    std::uniform_int_distribution<size_t> key_dist(0, cfg.writes == 0 ? 0 : cfg.writes - 1);
    for (size_t i = 0; i < hit_count; ++i) plan.push_back(hit_key(cfg.writes == 0 ? 0 : key_dist(plan_rng)));
    for (size_t i = 0; i < miss_count; ++i) plan.push_back(miss_key(i));
    std::shuffle(plan.begin(), plan.end(), plan_rng);

    auto ranges = split_range(plan.size(), cfg.threads);
    std::vector<std::thread> workers;
    std::atomic<size_t> done{0}, hits{0}, misses{0};

    auto t0 = std::chrono::steady_clock::now();
    {
        ProgressReporter progress("read", done, cfg.reads, cfg.progress_interval);
        for (unsigned t = 0; t < cfg.threads; ++t) {
            auto [lo, hi] = ranges[t];
            workers.emplace_back([&cfg, &plan, &done, &hits, &misses, lo, hi] {
                TcpSocket sock = connect_or_die(cfg);
                size_t local_done = 0, local_hits = 0, local_misses = 0;
                for (size_t i = lo; i < hi; ++i) {
                    sock.send_all(encode_get_request(plan[i]));
                    auto msg = receive_message(sock);
                    auto* r = msg ? std::get_if<GetResponse>(&*msg) : nullptr;
                    if (r && r->found) ++local_hits; else ++local_misses;
                    if (++local_done % 1000 == 0) done += 1000;
                }
                done += local_done % 1000;
                hits += local_hits;
                misses += local_misses;
            });
        }
        for (auto& w : workers) w.join();
    }
    auto t1 = std::chrono::steady_clock::now();

    ReadResult r;
    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.hits = hits.load();
    r.misses = misses.load();
    return r;
}

struct MixedResult {
    double seconds = 0.0;
    size_t get_ops = 0, get_hits = 0, get_misses = 0;
    size_t put_ops = 0, put_updates = 0, put_new_keys = 0;
    size_t delete_ops = 0;
};

// Mirrors kv_benchmark.cpp's run_mixed_phase exactly (same budget-based
// interleaving, same ~50/50 update/new-key split, same key distributions)
// but sends real wire-protocol requests to the leader instead of calling a
// local StorageEngine -- see that function's comment for the full design
// rationale, including why there's no pre-built op plan at this scale and
// why hit/miss counts aren't bit-for-bit reproducible run to run here.
MixedResult run_mixed_phase(const BenchConfig& cfg) {
    std::atomic<int64_t> reads_remaining{static_cast<int64_t>(cfg.mixed_reads)};
    std::atomic<int64_t> writes_remaining{static_cast<int64_t>(cfg.mixed_writes)};
    std::atomic<int64_t> deletes_remaining{static_cast<int64_t>(cfg.mixed_deletes)};
    std::atomic<uint64_t> next_new_key{cfg.writes};

    std::atomic<size_t> get_ops{0}, get_hits{0}, get_misses{0};
    std::atomic<size_t> put_ops{0}, put_updates{0}, put_new_keys{0};
    std::atomic<size_t> delete_ops{0};
    std::atomic<size_t> done{0};
    size_t total_ops = cfg.mixed_reads + cfg.mixed_writes + cfg.mixed_deletes;

    std::vector<std::thread> workers;
    auto t0 = std::chrono::steady_clock::now();
    {
        ProgressReporter progress("mixed", done, total_ops, cfg.progress_interval);
        for (unsigned t = 0; t < cfg.threads; ++t) {
            workers.emplace_back([&, t] {
                TcpSocket sock = connect_or_die(cfg);
                std::mt19937_64 rng(5000 + t);
                std::uniform_real_distribution<double> unit(0.0, 1.0);
                std::uniform_int_distribution<size_t> hit_dist(0, cfg.writes == 0 ? 0 : cfg.writes - 1);
                std::uniform_int_distribution<size_t> miss_dist(
                    0, cfg.mixed_reads == 0 ? 0 : cfg.mixed_reads - 1);

                size_t local_get = 0, local_hit = 0, local_miss = 0;
                size_t local_put = 0, local_update = 0, local_new = 0;
                size_t local_delete = 0, local_done = 0;

                while (true) {
                    int64_t r = std::max<int64_t>(reads_remaining.load(std::memory_order_relaxed), 0);
                    int64_t w = std::max<int64_t>(writes_remaining.load(std::memory_order_relaxed), 0);
                    int64_t d = std::max<int64_t>(deletes_remaining.load(std::memory_order_relaxed), 0);
                    int64_t total = r + w + d;
                    if (total <= 0) break;

                    uint64_t roll = std::uniform_int_distribution<uint64_t>(
                        0, static_cast<uint64_t>(total) - 1)(rng);

                    if (roll < static_cast<uint64_t>(r)) {
                        if (reads_remaining.fetch_sub(1, std::memory_order_relaxed) <= 0) {
                            reads_remaining.fetch_add(1, std::memory_order_relaxed);
                            continue;
                        }
                        bool miss = unit(rng) < cfg.miss_rate;
                        std::string key = miss ? miss_key(miss_dist(rng)) : hit_key(hit_dist(rng));
                        sock.send_all(encode_get_request(key));
                        auto msg = receive_message(sock);
                        auto* resp = msg ? std::get_if<GetResponse>(&*msg) : nullptr;
                        if (resp && resp->found) ++local_hit; else ++local_miss;
                        ++local_get;
                    } else if (roll < static_cast<uint64_t>(r) + static_cast<uint64_t>(w)) {
                        if (writes_remaining.fetch_sub(1, std::memory_order_relaxed) <= 0) {
                            writes_remaining.fetch_add(1, std::memory_order_relaxed);
                            continue;
                        }
                        bool is_new = unit(rng) < 0.5;
                        size_t idx = is_new ? next_new_key.fetch_add(1, std::memory_order_relaxed)
                                             : hit_dist(rng);
                        std::string value = make_value(cfg.min_value_size, cfg.max_value_size, rng);
                        sock.send_all(encode_put_request(hit_key(idx), value));
                        receive_message(sock);
                        if (is_new) ++local_new; else ++local_update;
                        ++local_put;
                    } else {
                        if (deletes_remaining.fetch_sub(1, std::memory_order_relaxed) <= 0) {
                            deletes_remaining.fetch_add(1, std::memory_order_relaxed);
                            continue;
                        }
                        sock.send_all(encode_delete_request(hit_key(hit_dist(rng))));
                        receive_message(sock);
                        ++local_delete;
                    }
                    if (++local_done % 1000 == 0) done += 1000;
                }

                done += local_done % 1000;
                get_ops += local_get; get_hits += local_hit; get_misses += local_miss;
                put_ops += local_put; put_updates += local_update; put_new_keys += local_new;
                delete_ops += local_delete;
            });
        }
        for (auto& w : workers) w.join();
    }
    auto t1 = std::chrono::steady_clock::now();

    MixedResult res;
    res.seconds = std::chrono::duration<double>(t1 - t0).count();
    res.get_ops = get_ops.load(); res.get_hits = get_hits.load(); res.get_misses = get_misses.load();
    res.put_ops = put_ops.load(); res.put_updates = put_updates.load(); res.put_new_keys = put_new_keys.load();
    res.delete_ops = delete_ops.load();
    return res;
}

} // namespace

int main(int argc, char** argv) {
    BenchConfig cfg = parse_args(argc, argv);

    bool mixed_mode = cfg.mixed_reads > 0 || cfg.mixed_writes > 0 || cfg.mixed_deletes > 0;

    std::cout << "=== cluster_benchmark ===\n"
              << "leader=" << cfg.leader_host << ":" << cfg.leader_port
              << " writes=" << cfg.writes << " deletes=" << cfg.deletes << " reads=" << cfg.reads
              << " value_size=[" << cfg.min_value_size << "-" << cfg.max_value_size << "]B"
              << " miss_rate=" << cfg.miss_rate << " threads=" << cfg.threads;
    if (mixed_mode) {
        std::cout << " mixed_reads=" << cfg.mixed_reads
                   << " mixed_writes=" << cfg.mixed_writes
                   << " mixed_deletes=" << cfg.mixed_deletes;
    }
    std::cout << "\n\n";

    double write_seconds = run_write_phase(cfg);
    double wps = write_seconds > 0.0 ? static_cast<double>(cfg.writes) / write_seconds : 0.0;

    DeleteResult dr;
    if (cfg.deletes > 0) {
        dr = run_delete_phase(cfg);
    }
    double dps = dr.seconds > 0.0 ? static_cast<double>(dr.ops) / dr.seconds : 0.0;

    std::cout << "--- Results ---\n";
    std::cout << "WRITE_OPS=" << cfg.writes << "\n";
    std::cout << "WRITE_SECONDS=" << write_seconds << "\n";
    std::cout << "WPS=" << wps << "\n";
    std::cout << "DELETE_OPS=" << dr.ops << "\n";
    std::cout << "DELETE_SECONDS=" << dr.seconds << "\n";
    std::cout << "DPS=" << dps << "\n";

    if (cfg.drop_caches_before_read) {
        std::cout << "--- Dropping OS page cache before read/mixed phase ---" << std::endl;
        int rc = std::system("sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null");
        std::cout << "--- drop_caches exit code: " << rc << " ---" << std::endl;
    }

    if (mixed_mode) {
        MixedResult mr = run_mixed_phase(cfg);
        size_t mixed_total_ops = mr.get_ops + mr.put_ops + mr.delete_ops;
        double mixed_ops_per_sec = mr.seconds > 0.0
            ? static_cast<double>(mixed_total_ops) / mr.seconds : 0.0;
        double mixed_get_miss_rate = mr.get_ops > 0
            ? static_cast<double>(mr.get_misses) / static_cast<double>(mr.get_ops) : 0.0;
        std::cout << "MIXED_SECONDS=" << mr.seconds << "\n";
        std::cout << "MIXED_TOTAL_OPS=" << mixed_total_ops << "\n";
        std::cout << "MIXED_OPS_PER_SEC=" << mixed_ops_per_sec << "\n";
        std::cout << "MIXED_GET_OPS=" << mr.get_ops << "\n";
        std::cout << "MIXED_GET_HITS=" << mr.get_hits << "\n";
        std::cout << "MIXED_GET_MISSES=" << mr.get_misses << "\n";
        std::cout << "MIXED_GET_MISS_RATE_OBSERVED=" << mixed_get_miss_rate << "\n";
        std::cout << "MIXED_PUT_OPS=" << mr.put_ops << "\n";
        std::cout << "MIXED_PUT_UPDATES=" << mr.put_updates << "\n";
        std::cout << "MIXED_PUT_NEW_KEYS=" << mr.put_new_keys << "\n";
        std::cout << "MIXED_DELETE_OPS=" << mr.delete_ops << "\n";
    } else {
        ReadResult rr = run_read_phase(cfg);
        double rps = rr.seconds > 0.0 ? static_cast<double>(cfg.reads) / rr.seconds : 0.0;
        double observed_miss_rate = cfg.reads > 0
            ? static_cast<double>(rr.misses) / static_cast<double>(cfg.reads) : 0.0;
        std::cout << "READ_OPS=" << cfg.reads << "\n";
        std::cout << "READ_SECONDS=" << rr.seconds << "\n";
        std::cout << "RPS=" << rps << "\n";
        std::cout << "READ_MISS_RATE_REQUESTED=" << cfg.miss_rate << "\n";
        std::cout << "READ_MISS_RATE_OBSERVED=" << observed_miss_rate << "\n";
        std::cout << "READ_HITS=" << rr.hits << "\n";
        std::cout << "READ_MISSES=" << rr.misses << "\n";
    }
    return 0;
}
