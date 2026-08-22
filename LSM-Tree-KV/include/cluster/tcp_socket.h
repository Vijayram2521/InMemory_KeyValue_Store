#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace kv_cluster {

// RAII, move-only wrapper around a POSIX socket file descriptor (matches
// StorageEngine's no-copy discipline elsewhere in this codebase). Closes on
// destruction.
//
// send_all/recv_exact loop internally until the full byte count transfers
// or the socket errors/closes -- unlike the file I/O everywhere else in
// this codebase, a single send()/recv() call on a TCP byte stream can
// transfer fewer bytes than requested, so callers can't just call it once
// and assume success.
class TcpSocket {
public:
    TcpSocket() = default;
    explicit TcpSocket(int fd) : fd_(fd) {}
    ~TcpSocket();

    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;
    TcpSocket(TcpSocket&& other) noexcept;
    TcpSocket& operator=(TcpSocket&& other) noexcept;

    bool valid() const { return fd_ >= 0; }
    void close();

    // Client side: connect to host:port. Returns false on failure.
    bool connect(const std::string& host, uint16_t port);

    // Server side: bind + listen on `port` (0.0.0.0), pass 0 for an
    // OS-assigned ephemeral port (see bound_port() -- used by tests so
    // multiple test runs never collide on a fixed port). Returns false on
    // failure.
    bool listen(uint16_t port, int backlog = 16);

    // Blocks until a client connects. The returned socket is invalid
    // (valid() == false) on error.
    TcpSocket accept();

    // Waits up to timeout_ms for the socket to become readable (for a
    // listening socket: a connection is pending) or writable-would-succeed.
    // Returns false on timeout or error. Used instead of calling accept()
    // directly so an accept loop can periodically re-check a shutdown flag
    // rather than blocking indefinitely -- closing a socket from another
    // thread does NOT reliably unblock a thread already parked in a
    // blocking accept() call (confirmed the hard way: it hung indefinitely
    // on Linux/ARM64 here, not just theoretical POSIX-portability caution).
    bool wait_readable(int timeout_ms) const;

    // The actual bound port -- only meaningful after listen() succeeds;
    // resolves what port 0 (ephemeral) actually got assigned.
    uint16_t bound_port() const;

    // Send/receive exactly data.size()/n bytes, looping internally as
    // needed. Returns false on any socket error or an unexpected close
    // partway through.
    bool send_all(const std::vector<uint8_t>& data);
    bool recv_exact(size_t n, std::vector<uint8_t>& out);

private:
    int fd_ = -1;
};

} // namespace kv_cluster
