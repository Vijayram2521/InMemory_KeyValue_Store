// Throughput benchmark for the DISTRIBUTED topology: drives PUT/GET
// through a real leader_node over the wire protocol (not an in-process
// StorageEngine like kv_benchmark), so results are directly comparable to
// the single-node kv_benchmark numbers already measured, but reflect the
// actual network+routing path a real client would see.
//
// Methodology mirrors kv_benchmark.cpp deliberately: same hit_key/miss_key
// disjoint-prefix scheme, same variable-value-size approach, same
// build-the-op-plan-then-time-pure-Gets read phase, same
// KEY=VALUE-per-line output -- so a side-by-side comparison against the
// existing single-node numbers isn't comparing methodologically different
// things.
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
    size_t reads = 10'000'000;
    size_t min_value_size = 64;
    size_t max_value_size = 1024;
    double miss_rate = 0.15;
    unsigned threads = 4;
    std::chrono::seconds progress_interval{10};
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
        else if (parse_flag(arg, "reads", val)) cfg.reads = std::stoull(val);
        else if (parse_flag(arg, "min-value-size", val)) cfg.min_value_size = std::stoull(val);
        else if (parse_flag(arg, "max-value-size", val)) cfg.max_value_size = std::stoull(val);
        else if (parse_flag(arg, "miss-rate", val)) cfg.miss_rate = std::stod(val);
        else if (parse_flag(arg, "threads", val)) cfg.threads = static_cast<unsigned>(std::stoul(val));
        else if (parse_flag(arg, "progress-interval", val)) cfg.progress_interval = std::chrono::seconds(std::stoll(val));
        else if (arg == "--help" || arg == "-h") {
            std::cout << "cluster_benchmark - drives PUT/GET through a real leader_node over TCP\n"
                         "  --leader-host=HOST      (default 127.0.0.1)\n"
                         "  --leader-port=N         (default 6000)\n"
                         "  --writes=N              (default 1000000)\n"
                         "  --reads=N               (default 10000000)\n"
                         "  --min-value-size=N / --max-value-size=N  (default 64/1024)\n"
                         "  --miss-rate=F           (default 0.15)\n"
                         "  --threads=N             (default 4)\n"
                         "  --progress-interval=N   seconds (default 10, 0 disables)\n";
            std::exit(0);
        }
    }
    if (cfg.threads == 0) cfg.threads = 1;
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

} // namespace

int main(int argc, char** argv) {
    BenchConfig cfg = parse_args(argc, argv);

    std::cout << "=== cluster_benchmark ===\n"
              << "leader=" << cfg.leader_host << ":" << cfg.leader_port
              << " writes=" << cfg.writes << " reads=" << cfg.reads
              << " value_size=[" << cfg.min_value_size << "-" << cfg.max_value_size << "]B"
              << " miss_rate=" << cfg.miss_rate << " threads=" << cfg.threads << "\n\n";

    double write_seconds = run_write_phase(cfg);
    double wps = write_seconds > 0.0 ? static_cast<double>(cfg.writes) / write_seconds : 0.0;

    ReadResult rr = run_read_phase(cfg);
    double rps = rr.seconds > 0.0 ? static_cast<double>(cfg.reads) / rr.seconds : 0.0;
    double observed_miss_rate = cfg.reads > 0 ? static_cast<double>(rr.misses) / static_cast<double>(cfg.reads) : 0.0;

    std::cout << "--- Results ---\n";
    std::cout << "WRITE_OPS=" << cfg.writes << "\n";
    std::cout << "WRITE_SECONDS=" << write_seconds << "\n";
    std::cout << "WPS=" << wps << "\n";
    std::cout << "READ_OPS=" << cfg.reads << "\n";
    std::cout << "READ_SECONDS=" << rr.seconds << "\n";
    std::cout << "RPS=" << rps << "\n";
    std::cout << "READ_MISS_RATE_REQUESTED=" << cfg.miss_rate << "\n";
    std::cout << "READ_MISS_RATE_OBSERVED=" << observed_miss_rate << "\n";
    std::cout << "READ_HITS=" << rr.hits << "\n";
    std::cout << "READ_MISSES=" << rr.misses << "\n";
    return 0;
}
