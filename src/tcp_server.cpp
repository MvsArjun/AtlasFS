#include "atlasfs/tcp_server.hpp"

#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <netinet/in.h>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace atlasfs::net {
namespace {

[[noreturn]] void throw_system_error(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

std::vector<std::byte> to_bytes(std::string_view text) {
    const auto* begin = reinterpret_cast<const std::byte*>(text.data());
    return {begin, begin + text.size()};
}

}  // namespace

protocol::Message echo_response(const protocol::Message& request) {
    switch (request.type) {
        case protocol::MessageType::put_request:
        case protocol::MessageType::get_request:
        case protocol::MessageType::delete_request:
        case protocol::MessageType::metadata_request:
            return {protocol::MessageType::response, request.flags, request.request_id,
                    request.payload};
        case protocol::MessageType::response:
        case protocol::MessageType::error_response:
        case protocol::MessageType::put_response:
        case protocol::MessageType::get_response:
        case protocol::MessageType::delete_response:
            return {protocol::MessageType::error_response, 0, request.request_id,
                    to_bytes("unexpected response message")};
    }
    throw std::logic_error("unhandled AtlasFS message type");
}

TcpServer::TcpServer(std::uint16_t port, RequestHandler handler,
                     std::uint32_t max_payload_size)
    : handler_(std::move(handler)), max_payload_size_(max_payload_size) {
    if (!handler_) {
        throw std::invalid_argument("TCP server requires a request handler");
    }

    listener_.reset(::socket(AF_INET, SOCK_STREAM, 0));
    if (!listener_.valid()) {
        throw_system_error("socket");
    }

    const int reuse_address = 1;
    if (::setsockopt(listener_.get(), SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                     sizeof(reuse_address)) < 0) {
        throw_system_error("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener_.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) <
        0) {
        throw_system_error("bind");
    }
    if (::listen(listener_.get(), SOMAXCONN) < 0) {
        throw_system_error("listen");
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener_.get(), reinterpret_cast<sockaddr*>(&address), &address_size) <
        0) {
        throw_system_error("getsockname");
    }
    port_ = ntohs(address.sin_port);
}

TcpServer::~TcpServer() {
    stop();
}

std::uint16_t TcpServer::port() const noexcept {
    return port_;
}

void TcpServer::serve() {
    while (!stopping_.load()) {
        const int descriptor = ::accept(listener_.get(), nullptr, nullptr);
        if (descriptor < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (stopping_.load()) {
                return;
            }
            throw_system_error("accept");
        }

        Socket connection(descriptor);
        set_active_descriptor(descriptor);
        try {
            handle_connection(connection);
        } catch (const std::system_error&) {
            clear_active_descriptor(descriptor);
            if (stopping_.load()) {
                return;
            }
            continue;
        } catch (...) {
            clear_active_descriptor(descriptor);
            if (stopping_.load()) {
                return;
            }
            throw;
        }
        clear_active_descriptor(descriptor);
    }
}

void TcpServer::stop() noexcept {
    if (stopping_.exchange(true)) {
        return;
    }
    if (listener_.valid()) {
        ::shutdown(listener_.get(), SHUT_RDWR);
    }
    std::lock_guard lock(active_mutex_);
    if (active_descriptor_ >= 0) {
        ::shutdown(active_descriptor_, SHUT_RDWR);
    }
}

void TcpServer::handle_connection(Socket& connection) {
    protocol::StreamParser parser(max_payload_size_);
    std::array<std::byte, 8192> buffer{};

    while (!stopping_.load()) {
        const auto received = ::recv(connection.get(), buffer.data(), buffer.size(), 0);
        if (received > 0) {
            auto result = parser.feed(
                std::span<const std::byte>(buffer).first(static_cast<std::size_t>(received)));
            for (const auto& request : result.messages) {
                send_all(connection.get(), protocol::encode(handler_(request)));
            }
            if (result.error != protocol::ParseError::none) {
                return;
            }
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received < 0) {
            if (stopping_.load()) {
                return;
            }
            throw_system_error("recv");
        }
        return;
    }
}

void TcpServer::set_active_descriptor(int descriptor) noexcept {
    std::lock_guard lock(active_mutex_);
    active_descriptor_ = descriptor;
}

void TcpServer::clear_active_descriptor(int descriptor) noexcept {
    std::lock_guard lock(active_mutex_);
    if (active_descriptor_ == descriptor) {
        active_descriptor_ = -1;
    }
}

}  // namespace atlasfs::net
