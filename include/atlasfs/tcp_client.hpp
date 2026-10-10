#pragma once

#include "atlasfs/protocol.hpp"
#include "atlasfs/socket.hpp"

#include <chrono>
#include <cstdint>
#include <deque>

namespace atlasfs::net {

class TcpClient {
public:
    explicit TcpClient(
        std::uint16_t port,
        std::chrono::milliseconds timeout = std::chrono::seconds(5));

    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;
    TcpClient(TcpClient&&) noexcept = default;
    TcpClient& operator=(TcpClient&&) noexcept = default;

    [[nodiscard]] protocol::Message request(const protocol::Message& message);
    void close() noexcept;

private:
    Socket socket_;
    protocol::StreamParser parser_;
    std::deque<protocol::Message> pending_responses_;
};

}  // namespace atlasfs::net
