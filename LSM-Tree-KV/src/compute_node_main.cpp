// Entry point for the compute_node binary: wraps one StorageEngine shard
// and serves it over the wire protocol. Config comes from environment
// variables (matching how docker-compose configures services) with CLI
// flag overrides for convenience when running/testing manually.
#include "cluster/compute_node_server.h"
#include "engine/storage_engine.h"

#include <chrono>
#include <cstdlib>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

namespace {

std::string env_or(const char* var, const std::string& fallback) {
    const char* v = std::getenv(var);
    return v ? std::string(v) : fallback;
}

bool parse_flag(const std::string& arg, const std::string& name, std::string& out) {
    std::string prefix = "--" + name + "=";
    if (arg.rfind(prefix, 0) == 0) {
        out = arg.substr(prefix.size());
        return true;
    }
    return false;
}

volatile std::sig_atomic_t g_shutdown_requested = 0;
void handle_shutdown_signal(int) { g_shutdown_requested = 1; }

} // namespace

int main(int argc, char** argv) {
    std::string node_id = env_or("NODE_ID", "compute-node");
    std::string port_str = env_or("LISTEN_PORT", "7001");
    std::string data_dir = env_or("DATA_DIR", "./data");
    // StorageEngine's own default (5) is deliberately tiny, sized for fast
    // unit tests -- fine there, but at any real dataset size it produces
    // one SSTable generation every 5 writes (hundreds of thousands of
    // generations for a benchmark-scale shard), which is exactly the
    // pathological many-generations scenario this project's own
    // benchmarking already measured as devastating for read throughput.
    // 50,000 is a much more reasonable operational default; still
    // overridable per-deployment via MEMTABLE_THRESHOLD/--memtable-threshold.
    std::string memtable_threshold_str = env_or("MEMTABLE_THRESHOLD", "50000");

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string val;
        if (parse_flag(arg, "node-id", val)) node_id = val;
        else if (parse_flag(arg, "port", val)) port_str = val;
        else if (parse_flag(arg, "data-dir", val)) data_dir = val;
        else if (parse_flag(arg, "memtable-threshold", val)) memtable_threshold_str = val;
        else if (arg == "--help" || arg == "-h") {
            std::cout << "compute_node -- runs one StorageEngine shard, served over TCP.\n"
                         "Config via env vars (NODE_ID, LISTEN_PORT, DATA_DIR, MEMTABLE_THRESHOLD)\n"
                         "or flags:\n"
                         "  --node-id=ID              identifies this node in logs (default: compute-node)\n"
                         "  --port=N                  TCP port to listen on (default: 7001)\n"
                         "  --data-dir=PATH           StorageEngine data directory (default: ./data)\n"
                         "  --memtable-threshold=N    entries buffered before a flush (default: 50000;\n"
                         "                            StorageEngine's own default of 5 is sized for unit\n"
                         "                            tests, not real data volumes)\n";
            return 0;
        }
    }

    uint16_t port = static_cast<uint16_t>(std::stoul(port_str));
    size_t memtable_threshold = std::stoull(memtable_threshold_str);

    kv_engine::StorageEngine engine(data_dir, memtable_threshold);
    kv_cluster::ComputeNodeServer server(engine);
    if (!server.start(port)) {
        std::cerr << "compute_node[" << node_id << "]: failed to listen on port " << port << std::endl;
        return 1;
    }
    std::cout << "compute_node[" << node_id << "]: listening on port " << server.port()
              << ", data_dir=" << data_dir << std::endl;

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);
    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "compute_node[" << node_id << "]: shutting down" << std::endl;
    server.stop();
    return 0;
}
