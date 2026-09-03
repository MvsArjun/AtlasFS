#include "atlasfs/socket.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

namespace atlasfs::net {
namespace {

[[noreturn]] void throw_system_error(const char* operation) {
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

Socket connect_loopback(std::uint16_t port) {
    Socket socket(::socket(AF_INET, SOCK_STREAM, 0));
    if (!socket.valid()) {
        throw_system_error("socket");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    int result;
    do {
        result = ::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address),
                           sizeof(address));
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
        throw_system_error("connect");
    }
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
