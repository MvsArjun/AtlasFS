#include "atlasfs/tcp_client.hpp"

#include <array>
#include <cerrno>
#include <stdexcept>
#include <sys/socket.h>
#include <system_error>
#include <utility>

namespace atlasfs::net {

TcpClient::TcpClient(std::uint16_t port) : socket_(connect_loopback(port)) {}

protocol::Message TcpClient::request(const protocol::Message& message) {
    if (!socket_.valid()) {
        throw std::logic_error("request attempted on a closed TCP client");
    }
    send_all(socket_.get(), protocol::encode(message));

    while (pending_responses_.empty()) {
        std::array<std::byte, 8192> buffer{};
        const auto received = ::recv(socket_.get(), buffer.data(), buffer.size(), 0);
        if (received > 0) {
            auto result = parser_.feed(
                std::span<const std::byte>(buffer).first(static_cast<std::size_t>(received)));
            if (result.error != protocol::ParseError::none) {
                throw std::runtime_error("server returned a malformed AtlasFS frame");
            }
            for (auto& response : result.messages) {
                pending_responses_.push_back(std::move(response));
            }
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received < 0) {
            throw std::system_error(errno, std::generic_category(), "recv");
        }
        if (parser_.has_partial_message()) {
            throw std::runtime_error("server closed during an incomplete AtlasFS frame");
        }
        throw std::runtime_error("server closed before sending a response");
    }

    auto response = std::move(pending_responses_.front());
    pending_responses_.pop_front();
    if (response.request_id != message.request_id) {
        throw std::runtime_error("AtlasFS response request ID does not match the request");
    }
    return response;
}

void TcpClient::close() noexcept {
    if (socket_.valid()) {
        ::shutdown(socket_.get(), SHUT_RDWR);
        socket_.reset();
    }
}

}  // namespace atlasfs::net
