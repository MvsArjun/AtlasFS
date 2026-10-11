#pragma once

#include "atlasfs/protocol.hpp"
#include "atlasfs/socket.hpp"
#include "atlasfs/transport.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <memory>

namespace atlasfs::scheduling { class AdmissionQueue; }

namespace atlasfs::net {

using RequestHandler = std::function<protocol::Message(const protocol::Message&)>;
using AuthenticatedRequestHandler =
    std::function<protocol::Message(const protocol::Message&, const PeerPolicy&)>;

[[nodiscard]] protocol::Message echo_response(const protocol::Message& request);

struct DispatchConfig {
    // Reference mode retains sequential full-connection ownership for A/B runs.
    bool serial{false};
    std::size_t workers{4};
    std::size_t request_queue_capacity{4};
    std::size_t max_requests_per_connection{1000000};
};

struct DispatchSnapshot {
    std::uint64_t accepted{}, rejected{}, active{}, peak_active{}, requests{};
    std::uint64_t request_limit_closes{}, admission_rejected{}, admission_expired{};
    std::uint64_t disconnect_cancellations{}, request_waiting{}, peak_request_waiting{};
};

class TcpServer {
public:
    explicit TcpServer(
        std::uint16_t port = 0,
        RequestHandler handler = echo_response,
        std::uint32_t max_payload_size = protocol::kDefaultMaxPayloadSize,
        const DispatchConfig& dispatch = {});
    TcpServer(TlsConfig config, std::uint16_t port, AuthenticatedRequestHandler handler,
              std::uint32_t max_payload_size = protocol::kDefaultMaxPayloadSize,
              const DispatchConfig& dispatch = {});
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    TcpServer(TcpServer&&) = delete;
    TcpServer& operator=(TcpServer&&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept;
    [[nodiscard]] DispatchSnapshot snapshot() const noexcept;
    void serve();
    void stop() noexcept;

private:
    void handle_connection(Socket& connection);
    void serve_serial();
    void worker(std::size_t index) noexcept;
    void fail(std::exception_ptr failure) noexcept;
    bool disconnected(int descriptor) const noexcept;
    struct DispatchState;

    Socket listener_;
    std::uint16_t port_{};
    RequestHandler handler_;
    std::shared_ptr<TlsContext> tls_;
    AuthenticatedRequestHandler authenticated_handler_;
    std::uint32_t max_payload_size_;
    DispatchConfig dispatch_;
    std::unique_ptr<DispatchState> state_;
    std::unique_ptr<scheduling::AdmissionQueue> admission_;
    std::atomic_bool stopping_{false};
    std::atomic_bool serving_{false};
};

}  // namespace atlasfs::net
