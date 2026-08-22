#include "cluster/tcp_socket.h"

#include <arpa/inet.h>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace kv_cluster {

TcpSocket::~TcpSocket() { close(); }

TcpSocket::TcpSocket(TcpSocket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

TcpSocket& TcpSocket::operator=(TcpSocket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void TcpSocket::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool TcpSocket::connect(const std::string& host, uint16_t port) {
    close();

    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* result = nullptr;
    std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result) != 0) {
        return false;
    }

    bool connected = false;
    for (struct addrinfo* p = result; p != nullptr; p = p->ai_next) {
        int candidate = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (candidate < 0) continue;
        if (::connect(candidate, p->ai_addr, p->ai_addrlen) == 0) {
            fd_ = candidate;
            connected = true;
            break;
        }
        ::close(candidate);
    }
    freeaddrinfo(result);
    return connected;
}

bool TcpSocket::listen(uint16_t port, int backlog) {
    close();

    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    int reuse = 1;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close();
        return false;
    }
    if (::listen(fd_, backlog) < 0) {
        close();
        return false;
    }
    return true;
}

TcpSocket TcpSocket::accept() {
    int client_fd = ::accept(fd_, nullptr, nullptr);
    if (client_fd < 0) return TcpSocket(-1);
    return TcpSocket(client_fd);
}

bool TcpSocket::wait_readable(int timeout_ms) const {
    struct pollfd pfd{};
    pfd.fd = fd_;
    pfd.events = POLLIN;
    int result = poll(&pfd, 1, timeout_ms);
    if (result <= 0) return false; // timeout (0) or error (-1)
    return (pfd.revents & POLLIN) != 0;
}

uint16_t TcpSocket::bound_port() const {
    struct sockaddr_in addr{};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(fd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) < 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

bool TcpSocket::send_all(const std::vector<uint8_t>& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, 0);
        if (n <= 0) return false; // error or peer closed mid-send
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool TcpSocket::recv_exact(size_t n, std::vector<uint8_t>& out) {
    out.resize(n);
    size_t received = 0;
    while (received < n) {
        ssize_t r = ::recv(fd_, out.data() + received, n - received, 0);
        if (r <= 0) return false; // error or peer closed before n bytes arrived
        received += static_cast<size_t>(r);
    }
    return true;
}

} // namespace kv_cluster
