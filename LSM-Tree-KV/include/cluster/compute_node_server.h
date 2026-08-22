#pragma once
#include "cluster/tcp_socket.h"
#include "engine/storage_engine.h"
#include <atomic>
#include <cstdint>
#include <thread>

namespace kv_cluster {

// Wraps a StorageEngine and serves PUT/GET/DELETE requests over the wire
// protocol, one thread per connection (detached -- matches this codebase's
// existing plain-std::thread style, e.g. benchmark.cpp; not warranted to
// reach for epoll at the node counts this project targets).
//
// Adds NO locking of its own: every connection thread calls straight into
// the same StorageEngine's already-thread-safe Put/Get/Delete, relying
// entirely on its existing std::shared_mutex for correctness. One lock per
// physical node, exactly as StorageEngine already provides on its own.
class ComputeNodeServer {
public:
    explicit ComputeNodeServer(kv_engine::StorageEngine& engine);
    ~ComputeNodeServer();

    ComputeNodeServer(const ComputeNodeServer&) = delete;
    ComputeNodeServer& operator=(const ComputeNodeServer&) = delete;

    // Starts listening on `port` (0 for an OS-assigned ephemeral port, used
    // by tests so runs never collide on a fixed port) and spawns the
    // accept-loop thread. Returns false if binding fails.
    bool start(uint16_t port);

    // The actual bound port -- meaningful after start() succeeds, resolves
    // what an ephemeral port (0) actually got assigned.
    uint16_t port() const;

    // Stops accepting new connections and joins the accept-loop thread.
    // Already-open connections finish naturally when their clients
    // disconnect (their handler threads are detached, not tracked/joined
    // here -- see the .cpp for why). Safe to call from any thread; also
    // called from the destructor if not called explicitly.
    void stop();

private:
    void accept_loop();

    kv_engine::StorageEngine& engine_;
    TcpSocket listen_socket_;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};
};

} // namespace kv_cluster
