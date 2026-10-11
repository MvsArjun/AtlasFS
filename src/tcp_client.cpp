#include "atlasfs/tcp_client.hpp"
#include "atlasfs/metrics.hpp"

#include <array>
#include <stdexcept>
#include <sys/socket.h>
#include <utility>

namespace atlasfs::net {

TcpClient::TcpClient(std::uint16_t port, std::chrono::milliseconds timeout)
    : socket_(connect_loopback(port, timeout)), stream_(plaintext_stream(socket_.get())),
      timeout_(timeout) {}

TcpClient::TcpClient(std::uint16_t port, TlsConfig config, const PeerIdentity& expected_peer)
    : tls_(std::make_shared<TlsContext>(std::move(config))), timeout_(tls_->timeout()) {
    socket_ = connect_loopback(port, timeout_);
    if (tls_->tcp_nodelay()) set_tcp_nodelay(socket_.get(), true);
    stream_ = tls_->connect(socket_.get(), expected_peer);
}

protocol::Message TcpClient::request(const protocol::Message& message) {
    metrics::Timer timing(metrics::Counter::rpc_calls, metrics::Counter::rpc_ns);
    if (!socket_.valid()) {
        throw std::logic_error("request attempted on a closed TCP client");
    }
    try {
        const auto deadline = std::chrono::steady_clock::now() + timeout_;
        stream_->write_all(protocol::encode(message), deadline);
        metrics::add(metrics::Counter::frames_sent);
        metrics::add(metrics::Counter::payload_sent, message.payload.size());
        metrics::add(metrics::Counter::framed_sent, message.payload.size() + protocol::kHeaderSize);
        while (pending_responses_.empty()) {
            std::array<std::byte, 8192> buffer{};
            const auto received = stream_->read(buffer, deadline);
            if (received == 0) {
                throw TransportError(parser_.has_partial_message()
                    ? "server closed during an incomplete AtlasFS frame"
                    : "server closed before sending a response");
            }
            auto result = parser_.feed(
                std::span<const std::byte>(buffer).first(static_cast<std::size_t>(received)));
            if (result.error != protocol::ParseError::none) {
                throw TransportError("server returned a malformed AtlasFS frame");
            }
            for (auto& response : result.messages) {
                metrics::add(metrics::Counter::frames_received);
                metrics::add(metrics::Counter::payload_received, response.payload.size());
                pending_responses_.push_back(std::move(response));
            }
        }
        auto response = std::move(pending_responses_.front());
        pending_responses_.pop_front();
        if (response.request_id != message.request_id)
            throw TransportError("AtlasFS response request ID does not match the request");
        return response;
    } catch (...) {
        metrics::add(metrics::Counter::rpc_failures);
        // Partial I/O has an ambiguous outcome. Do not reuse or implicitly retry.
        close();
        throw;
    }
}

void TcpClient::close() noexcept {
    stream_.reset();
    if (socket_.valid()) {
        ::shutdown(socket_.get(), SHUT_RDWR);
        socket_.reset();
    }
    pending_responses_.clear();
}

}  // namespace atlasfs::net
