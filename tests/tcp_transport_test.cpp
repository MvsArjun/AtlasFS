#include "atlasfs/protocol.hpp"
#include "atlasfs/socket.hpp"
#include "atlasfs/tcp_client.hpp"
#include "atlasfs/tcp_server.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <iterator>
#include <span>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <utility>
#include <vector>

namespace atlasfs::net {
namespace {

std::vector<std::byte> bytes(std::initializer_list<unsigned char> values) {
    std::vector<std::byte> result;
    result.reserve(values.size());
    for (const auto value : values) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

protocol::Message request(std::uint64_t id, std::vector<std::byte> payload = {}) {
    return {protocol::MessageType::metadata_request, 0, id, std::move(payload)};
}

class RunningServer {
public:
    explicit RunningServer(
        std::uint32_t max_payload_size = protocol::kDefaultMaxPayloadSize)
        : server_(0, echo_response, max_payload_size),
          thread_([this] {
              try {
                  server_.serve();
              } catch (...) {
                  error_ = std::current_exception();
              }
          }) {}

    ~RunningServer() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    RunningServer(const RunningServer&) = delete;
    RunningServer& operator=(const RunningServer&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

    void stop_and_rethrow() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
        if (error_) {
            std::rethrow_exception(error_);
        }
    }

private:
    TcpServer server_;
    std::exception_ptr error_;
    std::thread thread_;
};

std::vector<protocol::Message> receive_messages(int descriptor, std::size_t count) {
    protocol::StreamParser parser;
    std::vector<protocol::Message> messages;
    std::array<std::byte, 8192> buffer{};
    while (messages.size() < count) {
        const auto received = ::recv(descriptor, buffer.data(), buffer.size(), 0);
        if (received <= 0) {
            throw std::runtime_error("connection closed before expected responses arrived");
        }
        auto result = parser.feed(
            std::span<const std::byte>(buffer).first(static_cast<std::size_t>(received)));
        if (result.error != protocol::ParseError::none) {
            throw std::runtime_error("malformed response in integration test");
        }
        messages.insert(messages.end(), std::make_move_iterator(result.messages.begin()),
                        std::make_move_iterator(result.messages.end()));
    }
    return messages;
}

void expect_server_still_accepts(std::uint16_t port) {
    TcpClient client(port);
    const auto response = client.request(request(999, bytes({9})));
    EXPECT_EQ(response.type, protocol::MessageType::response);
    EXPECT_EQ(response.request_id, 999U);
    EXPECT_EQ(response.payload, bytes({9}));
}

TEST(TcpTransport, EstablishesLoopbackConnectionOnEphemeralPort) {
    RunningServer server;
    TcpClient client(server.port());
    client.close();
    expect_server_still_accepts(server.port());
    server.stop_and_rethrow();
}

TEST(TcpTransport, CompletesRequestResponseRoundTrip) {
    RunningServer server;
    TcpClient client(server.port());
    const auto input = request(42, bytes({0, 1, 0xff}));
    const auto response = client.request(input);
    EXPECT_EQ(response.type, protocol::MessageType::response);
    EXPECT_EQ(response.request_id, input.request_id);
    EXPECT_EQ(response.payload, input.payload);
    client.close();
    server.stop_and_rethrow();
}

TEST(TcpTransport, AcceptsRequestWrittenInFragments) {
    RunningServer server;
    auto connection = connect_loopback(server.port());
    const auto wire = protocol::encode(request(7, bytes({1, 2, 3, 4})));
    const std::array<std::size_t, 4> fragment_sizes{1, 2, 3, wire.size() - 6};
    std::size_t offset = 0;
    for (const auto size : fragment_sizes) {
        send_all(connection.get(), std::span(wire).subspan(offset, size));
        offset += size;
    }
    const auto responses = receive_messages(connection.get(), 1);
    ASSERT_EQ(responses.size(), 1U);
    EXPECT_EQ(responses.front().request_id, 7U);
    EXPECT_EQ(responses.front().payload, bytes({1, 2, 3, 4}));
    connection.reset();
    server.stop_and_rethrow();
}

TEST(TcpTransport, ProcessesMultipleRequestsOnOneConnection) {
    RunningServer server;
    TcpClient client(server.port());
    EXPECT_EQ(client.request(request(1)).request_id, 1U);
    EXPECT_EQ(client.request(request(2)).request_id, 2U);
    EXPECT_EQ(client.request(request(3)).request_id, 3U);
    client.close();
    server.stop_and_rethrow();
}

TEST(TcpTransport, ProcessesCoalescedFramesFromOneWrite) {
    RunningServer server;
    auto connection = connect_loopback(server.port());
    auto wire = protocol::encode(request(10, bytes({1})));
    const auto second = protocol::encode(request(11, bytes({2})));
    wire.insert(wire.end(), second.begin(), second.end());
    send_all(connection.get(), wire);

    const auto responses = receive_messages(connection.get(), 2);
    ASSERT_EQ(responses.size(), 2U);
    EXPECT_EQ(responses[0].request_id, 10U);
    EXPECT_EQ(responses[1].request_id, 11U);
    connection.reset();
    server.stop_and_rethrow();
}

TEST(TcpTransport, HandlesCleanDisconnectAndContinuesServing) {
    RunningServer server;
    { auto connection = connect_loopback(server.port()); }
    expect_server_still_accepts(server.port());
    server.stop_and_rethrow();
}

TEST(TcpTransport, HandlesPartialFrameThenEofAndContinuesServing) {
    RunningServer server;
    {
        auto connection = connect_loopback(server.port());
        const auto wire = protocol::encode(request(20, bytes({1, 2, 3})));
        send_all(connection.get(), std::span(wire).first(protocol::kHeaderSize + 1));
    }
    expect_server_still_accepts(server.port());
    server.stop_and_rethrow();
}

TEST(TcpTransport, RejectsMalformedFrameAndContinuesServing) {
    RunningServer server;
    {
        auto connection = connect_loopback(server.port());
        auto wire = protocol::encode(request(30));
        wire[0] = std::byte{0};
        send_all(connection.get(), wire);
    }
    expect_server_still_accepts(server.port());
    server.stop_and_rethrow();
}

TEST(TcpTransport, RejectsOversizedFrameAndContinuesServing) {
    RunningServer server(4);
    {
        auto connection = connect_loopback(server.port());
        auto header = protocol::encode(request(40));
        header[23] = std::byte{5};
        send_all(connection.get(), header);
    }
    TcpClient client(server.port());
    const auto response = client.request(request(41, bytes({1, 2, 3, 4})));
    EXPECT_EQ(response.request_id, 41U);
    client.close();
    server.stop_and_rethrow();
}

TEST(TcpServer, StopUnblocksAnActiveConnection) {
    RunningServer server;
    auto connection = connect_loopback(server.port());
    server.stop_and_rethrow();
}

}  // namespace
}  // namespace atlasfs::net
