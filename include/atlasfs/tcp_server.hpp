#pragma once

#include "atlasfs/protocol.hpp"
#include "atlasfs/socket.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>

namespace atlasfs::net {

using RequestHandler = std::function<protocol::Message(const protocol::Message&)>;

[[nodiscard]] protocol::Message echo_response(const protocol::Message& request);

class TcpServer {
public:
    explicit TcpServer(
        std::uint16_t port = 0,
        RequestHandler handler = echo_response,
        std::uint32_t max_payload_size = protocol::kDefaultMaxPayloadSize);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    TcpServer(TcpServer&&) = delete;
    TcpServer& operator=(TcpServer&&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept;
    void serve();
    void stop() noexcept;

private:
    void handle_connection(Socket& connection);
    void set_active_descriptor(int descriptor) noexcept;
    void clear_active_descriptor(int descriptor) noexcept;

    Socket listener_;
    std::uint16_t port_{};
    RequestHandler handler_;
    std::uint32_t max_payload_size_;
    std::atomic_bool stopping_{false};
    std::mutex active_mutex_;
    int active_descriptor_{-1};
};

}  // namespace atlasfs::net
