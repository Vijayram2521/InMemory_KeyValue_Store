// Entry point for the leader_node binary: accepts client connections and
// routes each request to the correct compute node via consistent hashing.
// Config comes from environment variables (matching docker-compose) with
// CLI flag overrides for manual runs.
#include "cluster/leader_server.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

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

// Parses "id:host:port,id:host:port,..." into endpoints. Returns an empty
// vector (caller treats as a fatal config error) if any entry is malformed.
std::vector<kv_cluster::ComputeNodeEndpoint> parse_compute_nodes(const std::string& spec) {
    std::vector<kv_cluster::ComputeNodeEndpoint> endpoints;
    std::istringstream stream(spec);
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        if (entry.empty()) continue;
        size_t first_colon = entry.find(':');
        size_t second_colon = entry.find(':', first_colon == std::string::npos ? 0 : first_colon + 1);
        if (first_colon == std::string::npos || second_colon == std::string::npos) return {};

        kv_cluster::ComputeNodeEndpoint ep;
        ep.node_id = entry.substr(0, first_colon);
        ep.host = entry.substr(first_colon + 1, second_colon - first_colon - 1);
        try {
            ep.port = static_cast<uint16_t>(std::stoul(entry.substr(second_colon + 1)));
        } catch (...) {
            return {};
        }
        if (ep.node_id.empty() || ep.host.empty()) return {};
        endpoints.push_back(std::move(ep));
    }
    return endpoints;
}

volatile std::sig_atomic_t g_shutdown_requested = 0;
void handle_shutdown_signal(int) { g_shutdown_requested = 1; }

} // namespace

int main(int argc, char** argv) {
    std::string port_str = env_or("LISTEN_PORT", "6000");
    std::string compute_nodes_spec = env_or("COMPUTE_NODES", "");

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string val;
        if (parse_flag(arg, "port", val)) port_str = val;
        else if (parse_flag(arg, "compute-nodes", val)) compute_nodes_spec = val;
        else if (arg == "--help" || arg == "-h") {
            std::cout << "leader_node -- routes client requests to compute nodes via consistent hashing.\n"
                         "Config via env vars (LISTEN_PORT, COMPUTE_NODES) or flags:\n"
                         "  --port=N              TCP port to listen on (default: 6000)\n"
                         "  --compute-nodes=SPEC   \"id:host:port,id:host:port,...\" (required)\n";
            return 0;
        }
    }

    auto endpoints = parse_compute_nodes(compute_nodes_spec);
    if (endpoints.empty()) {
        std::cerr << "leader_node: no valid compute nodes configured "
                     "(set COMPUTE_NODES=id:host:port,... or --compute-nodes=...)" << std::endl;
        return 1;
    }

    std::cout << "leader_node: routing to " << endpoints.size() << " compute node(s):";
    for (const auto& ep : endpoints) {
        std::cout << " " << ep.node_id << "(" << ep.host << ":" << ep.port << ")";
    }
    std::cout << std::endl;

    uint16_t port = static_cast<uint16_t>(std::stoul(port_str));
    kv_cluster::LeaderServer server(std::move(endpoints));
    if (!server.start(port)) {
        std::cerr << "leader_node: failed to listen on port " << port << std::endl;
        return 1;
    }
    std::cout << "leader_node: listening on port " << server.port() << std::endl;

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);
    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "leader_node: shutting down" << std::endl;
    server.stop();
    return 0;
}
