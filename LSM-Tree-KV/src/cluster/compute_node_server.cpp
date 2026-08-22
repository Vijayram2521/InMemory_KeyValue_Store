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
    // Join BEFORE closing: accept_loop now exits on its own within ~200ms
    // once running_ is false (via the poll timeout in wait_readable), so
    // there's no need for close() to interrupt anything -- and closing
    // first would race the accept-loop thread's concurrent access to the
    // same TcpSocket's underlying fd.
    if (accept_thread_.joinable()) accept_thread_.join();
    listen_socket_.close();
}

void ComputeNodeServer::accept_loop() {
    while (running_) {
        // Poll with a bounded timeout rather than blocking in accept()
        // directly, so this loop re-checks running_ regularly instead of
        // potentially blocking forever -- closing listen_socket_ from
        // stop() is not a reliable way to unblock a thread already parked
        // in accept() (confirmed: it hung indefinitely on Linux/ARM64).
        if (!listen_socket_.wait_readable(200)) continue; // timed out, loop back to recheck running_
        TcpSocket client = listen_socket_.accept();
        if (!client.valid()) continue; // spurious/racy failure (e.g. stop() closed it between poll and accept)
        std::thread(handle_connection, std::ref(engine_), std::move(client)).detach();
    }
}

} // namespace kv_cluster
