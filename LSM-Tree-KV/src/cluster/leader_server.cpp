#include "cluster/leader_server.h"
#include "cluster/wire_protocol.h"

namespace kv_cluster {

namespace {

// Runs on its own detached thread for the lifetime of one client
// connection. Keeps one persistent socket per compute node it actually
// needs to talk to (lazily opened on first use, kept for the connection's
// whole lifetime) -- avoids a reconnect per request, and since each
// connection has its own thread, there's no cross-thread sharing of these
// sockets to worry about.
void handle_client(const std::map<std::string, ComputeNodeEndpoint>& endpoint_by_node_id,
                    const HashRing& ring, TcpSocket client) {
    std::map<std::string, TcpSocket> node_sockets;

    while (true) {
        auto raw_request = receive_raw_frame(client);
        if (!raw_request) break; // client closed or sent something malformed

        // Payload starts after the 4-byte length prefix.
        std::vector<uint8_t> payload(raw_request->begin() + 4, raw_request->end());
        auto decoded = decode_payload(payload);
        auto key = decoded ? extract_request_key(*decoded) : std::nullopt;
        if (!key) {
            if (!client.send_all(encode_error_response("malformed or non-request message"))) break;
            continue;
        }

        auto node_id = ring.get_node(*key);
        auto endpoint_it = node_id ? endpoint_by_node_id.find(*node_id) : endpoint_by_node_id.end();
        if (!node_id || endpoint_it == endpoint_by_node_id.end()) {
            if (!client.send_all(encode_error_response("no compute node available for this key"))) break;
            continue;
        }

        // Get or lazily open this client connection's socket to the target node.
        auto sock_it = node_sockets.find(*node_id);
        if (sock_it == node_sockets.end() || !sock_it->second.valid()) {
            TcpSocket sock;
            if (!sock.connect(endpoint_it->second.host, endpoint_it->second.port)) {
                if (!client.send_all(encode_error_response("compute node " + *node_id + " unreachable"))) break;
                continue;
            }
            sock_it = node_sockets.insert_or_assign(*node_id, std::move(sock)).first;
        }

        if (!sock_it->second.send_all(*raw_request)) {
            if (!client.send_all(encode_error_response("compute node " + *node_id + " unreachable"))) break;
            node_sockets.erase(sock_it); // force a fresh connect attempt next time
            continue;
        }

        auto raw_response = receive_raw_frame(sock_it->second);
        if (!raw_response) {
            if (!client.send_all(encode_error_response("compute node " + *node_id + " unreachable"))) break;
            node_sockets.erase(sock_it);
            continue;
        }

        if (!client.send_all(*raw_response)) break; // client gone
    }
}

} // namespace

LeaderServer::LeaderServer(std::vector<ComputeNodeEndpoint> endpoints) : endpoints_(std::move(endpoints)) {
    std::vector<std::string> node_ids;
    node_ids.reserve(endpoints_.size());
    for (const auto& ep : endpoints_) {
        node_ids.push_back(ep.node_id);
        endpoint_by_node_id_[ep.node_id] = ep;
    }
    ring_.set_nodes(node_ids);
}

LeaderServer::~LeaderServer() { stop(); }

bool LeaderServer::start(uint16_t port) {
    if (!listen_socket_.listen(port)) return false;
    running_ = true;
    accept_thread_ = std::thread(&LeaderServer::accept_loop, this);
    return true;
}

uint16_t LeaderServer::port() const { return listen_socket_.bound_port(); }

void LeaderServer::stop() {
    if (!running_.exchange(false)) return;
    if (accept_thread_.joinable()) accept_thread_.join();
    listen_socket_.close();
}

void LeaderServer::accept_loop() {
    while (running_) {
        if (!listen_socket_.wait_readable(200)) continue;
        TcpSocket client = listen_socket_.accept();
        if (!client.valid()) continue;
        std::thread(handle_client, std::cref(endpoint_by_node_id_), std::cref(ring_), std::move(client)).detach();
    }
}

} // namespace kv_cluster
