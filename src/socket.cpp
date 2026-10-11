#include "atlasfs/socket.hpp"
#include "atlasfs/metrics.hpp"
#include "atlasfs/transport.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

namespace atlasfs::net {
namespace {

[[noreturn]] void throw_system_error(const char* operation) {
    metrics::add(metrics::Counter::connection_failures);
    throw std::system_error(errno, std::generic_category(), operation);
}

}  // namespace

Socket::Socket(int descriptor) noexcept : descriptor_(descriptor) {}

Socket::~Socket() {
    reset();
}

Socket::Socket(Socket&& other) noexcept : descriptor_(other.release()) {}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        reset(other.release());
    }
    return *this;
}

int Socket::get() const noexcept {
    return descriptor_;
}

bool Socket::valid() const noexcept {
    return descriptor_ >= 0;
}

int Socket::release() noexcept {
    const int descriptor = descriptor_;
    descriptor_ = -1;
    return descriptor;
}

void Socket::reset(int descriptor) noexcept {
    if (descriptor_ >= 0) {
        ::close(descriptor_);
    }
    descriptor_ = descriptor;
}

void set_tcp_nodelay(int descriptor, bool enabled) {
    const int value = enabled ? 1 : 0;
    if (::setsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0)
        throw_system_error("setsockopt TCP_NODELAY");
    if (enabled) metrics::add(metrics::Counter::nodelay_connections);
}

Socket connect_loopback(std::uint16_t port, std::chrono::milliseconds timeout) {
    metrics::Timer timing(metrics::Counter::tcp_connects, metrics::Counter::tcp_connect_ns);
    if (timeout.count() <= 0 || timeout > std::chrono::minutes(5))
        throw std::invalid_argument("invalid connection timeout");
    Socket socket(::socket(AF_INET, SOCK_STREAM, 0));
    if (!socket.valid()) {
        throw_system_error("socket");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    const int flags = ::fcntl(socket.get(), F_GETFL, 0);
    make_nonblocking(socket.get());
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    const int result = ::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address),
                                sizeof(address));
    if (result < 0) {
        if (errno != EINPROGRESS && errno != EINTR) throw_system_error("connect");
        wait_socket(socket.get(), true, deadline);
        int error = 0;
        socklen_t length = sizeof(error);
        if (::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &error, &length) != 0)
            throw_system_error("getsockopt(connect)");
        if (error != 0) { metrics::add(metrics::Counter::connection_failures); throw std::system_error(error, std::generic_category(), "connect"); }
    }
    // Preserve the raw-socket compatibility API; stream users make it nonblocking.
    if (::fcntl(socket.get(), F_SETFL, flags) != 0) throw_system_error("fcntl(connect)");
    return socket;
}

void send_all(int descriptor, std::span<const std::byte> bytes) {
    std::size_t sent = 0;
    while (sent < bytes.size()) {
        const auto result = ::send(descriptor, bytes.data() + sent, bytes.size() - sent,
                                   MSG_NOSIGNAL);
        if (result > 0) {
            sent += static_cast<std::size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result == 0) {
            throw std::runtime_error("send returned zero before all bytes were written");
        }
        throw_system_error("send");
    }
}

}  // namespace atlasfs::net
