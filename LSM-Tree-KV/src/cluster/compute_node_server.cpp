#include "cluster/compute_node_server.h"
#include "cluster/wire_protocol.h"

namespace kv_cluster {

namespace {

// Runs on its own detached thread for the lifetime of one client
// connection. Not a member function (static-like, takes engine by
// reference and sock by value/move) so it can be launched without the
// ComputeNodeServer object needing to track or join it -- the thread owns
// its own TcpSocket and exits (closing it) when the client disconnects or
// sends something malformed.
void handle_connection(kv_engine::StorageEngine& engine, TcpSocket sock) {
    while (true) {
        auto msg = receive_message(sock);
        if (!msg) break; // connection closed, errored, or sent something malformed

        std::vector<uint8_t> response;
        if (auto* req = std::get_if<PutRequest>(&*msg)) {
            bool ok = engine.Put(req->key, req->value);
            response = encode_put_response(ok);
        } else if (auto* req = std::get_if<GetRequest>(&*msg)) {
            auto value = engine.Get(req->key);
            response = encode_get_response(value.has_value(), value.value_or(std::string()));
        } else if (auto* req = std::get_if<DeleteRequest>(&*msg)) {
            bool ok = engine.Delete(req->key);
            response = encode_delete_response(ok);
        } else {
            // A request-shaped connection sent us a response/error message
            // type -- not a valid request, nothing meaningful to execute.
            response = encode_error_response("unexpected message type for a request");
        }

        if (!sock.send_all(response)) break; // client gone; nothing more to do
    }
}

} // namespace

ComputeNodeServer::ComputeNodeServer(kv_engine::StorageEngine& engine) : engine_(engine) {}

ComputeNodeServer::~ComputeNodeServer() { stop(); }

bool ComputeNodeServer::start(uint16_t port) {
    if (!listen_socket_.listen(port)) return false;
    running_ = true;
    accept_thread_ = std::thread(&ComputeNodeServer::accept_loop, this);
    return true;
}

uint16_t ComputeNodeServer::port() const { return listen_socket_.bound_port(); }

void ComputeNodeServer::stop() {
    if (!running_.exchange(false)) return; // already stopped
    listen_socket_.close(); // unblocks the accept() call in accept_loop
    if (accept_thread_.joinable()) accept_thread_.join();
}

void ComputeNodeServer::accept_loop() {
    while (running_) {
        TcpSocket client = listen_socket_.accept();
        if (!client.valid()) break; // listen socket closed (stop() was called) or a real error
        std::thread(handle_connection, std::ref(engine_), std::move(client)).detach();
    }
}

} // namespace kv_cluster
