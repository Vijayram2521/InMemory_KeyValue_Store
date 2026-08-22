#pragma once
#include "cluster/hash_ring.h"
#include "cluster/tcp_socket.h"
#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace kv_cluster {

struct ComputeNodeEndpoint {
    std::string node_id;
    std::string host;
    uint16_t port;
};

// Accepts client connections and routes each request to the correct
// compute node via consistent hashing (HashRing), proxying the request and
// response as raw bytes -- no decode/re-encode round trip needed except to
// read the routing key out of the request.
//
// Fixed node set for this phase: `endpoints` is set once at construction
// (matches HashRing's own fixed-membership design) -- no live add/remove.
// If a compute node can't be reached, the client gets an ERROR_RESPONSE
// (fail loud, no silent retry/masking) rather than the leader hanging or
// pretending nothing happened.
class LeaderServer {
public:
    explicit LeaderServer(std::vector<ComputeNodeEndpoint> endpoints);
    ~LeaderServer();

    LeaderServer(const LeaderServer&) = delete;
    LeaderServer& operator=(const LeaderServer&) = delete;

    bool start(uint16_t port);
    uint16_t port() const;
    void stop();

private:
    void accept_loop();

    std::vector<ComputeNodeEndpoint> endpoints_;
    std::map<std::string, ComputeNodeEndpoint> endpoint_by_node_id_;
    HashRing ring_;
    TcpSocket listen_socket_;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};
};

} // namespace kv_cluster
