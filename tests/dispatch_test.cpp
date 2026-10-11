#include "atlasfs/tcp_client.hpp"
#include "atlasfs/tcp_server.hpp"
#include "atlasfs/metrics.hpp"
#include "tls_test_support.hpp"

#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <condition_variable>
#include <future>
#include <functional>
#include <mutex>
#include <thread>
#include <sys/socket.h>

namespace atlasfs::net {
namespace {
using namespace std::chrono_literals;
protocol::Message ping(std::uint64_t id) {
    return {protocol::MessageType::metadata_request, 0, id, {static_cast<std::byte>(id)}};
}
bool await(const std::function<bool()>& condition) {
    const auto limit = std::chrono::steady_clock::now() + 5s;
    while (!condition()) {
        if (std::chrono::steady_clock::now() >= limit) return false;
        std::this_thread::yield();
    }
    return true;
}
class Running {
public:
    explicit Running(TcpServer& server) : server_(server), thread_([this, observer = metrics::current_owner()] {
        metrics::Scope scope(observer);
        try { server_.serve(); } catch (...) { error = std::current_exception(); }
    }) {}
    ~Running() { stop(); }
    void stop() { server_.stop(); if (thread_.joinable()) thread_.join(); }
    std::exception_ptr error;
private:
    TcpServer& server_;
    std::thread thread_;
};
class Latch {
public:
    void wait() { std::unique_lock lock(mutex_); ready_.wait(lock, [&] { return open_; }); }
    void open() { std::lock_guard lock(mutex_); open_ = true; ready_.notify_all(); }
private:
    std::mutex mutex_;
    std::condition_variable ready_;
    bool open_{};
};
struct Release { Latch& latch; ~Release() { latch.open(); } };
class Dispatch : public ::testing::Test {
protected:
    test::Certificates certificates;
    const PeerIdentity coordinator{"coordinator", PeerRole::coordinator};
    const PeerIdentity storage{"d", PeerRole::storage};
    TlsConfig server_config{certificates.config(storage)};
    TlsConfig client_config{certificates.config(coordinator)};
    Dispatch() {
        test::Certificates::allow(server_config, coordinator);
        test::Certificates::allow(client_config, storage);
        server_config.timeout = client_config.timeout = 10s;
    }
};

TEST(DispatchConfig, InvalidLimitsRejectedBeforeServing) {
    for (const auto config : {DispatchConfig{false, 0, 4, 1}, DispatchConfig{false, 17, 4, 1},
                             DispatchConfig{false, 1, 65537, 1}, DispatchConfig{false, 1, 0, 0},
                             DispatchConfig{false, 1, 0, 1000001}})
        EXPECT_THROW(TcpServer server(0, echo_response, protocol::kDefaultMaxPayloadSize, config),
                     std::invalid_argument);
}
TEST_F(Dispatch, IdleAuthenticatedConnectionDoesNotBlockAnotherPeer) {
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); });
    Running running(server);
    TcpClient first(server.port(), client_config, storage);
    ASSERT_EQ(first.request(ping(1)).request_id, 1U);
    TcpClient second(server.port(), client_config, storage);
    EXPECT_EQ(second.request(ping(2)).payload, ping(2).payload);
    EXPECT_EQ(server.snapshot().active, 2U);
    running.stop();
    EXPECT_FALSE(running.error);
    EXPECT_EQ(server.snapshot().active, 0U);
}
TEST_F(Dispatch, SerialReferenceReproducesIdleConnectionHeadOfLineBlocking) {
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); },
                     protocol::kDefaultMaxPayloadSize, {true, 4, 4, 1000000});
    Running running(server);
    TcpClient first(server.port(), client_config, storage);
    ASSERT_EQ(first.request(ping(1)).request_id, 1U);
    auto short_client = client_config; short_client.timeout = 100ms;
    EXPECT_THROW(TcpClient second(server.port(), short_client, storage), TransportError);
    EXPECT_EQ(server.snapshot().requests, 1U);
    first.close();
    TcpClient next(server.port(), client_config, storage);
    EXPECT_EQ(next.request(ping(3)).request_id, 3U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, StalledHandshakeOccupiesOnlyOneBoundedSlot) {
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); });
    Running running(server);
    auto stalled = connect_loopback(server.port());
    ASSERT_TRUE(await([&] { return server.snapshot().active == 1; }));
    TcpClient active(server.port(), client_config, storage);
    EXPECT_EQ(active.request(ping(2)).request_id, 2U);
    EXPECT_EQ(server.snapshot().peak_active, 2U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, PartiallyReceivedFrameDoesNotHoldHandlerAdmission) {
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); });
    Running running(server);
    auto socket = connect_loopback(server.port());
    TlsContext context(client_config);
    auto partial = context.connect(socket.get(), storage);
    const auto wire = protocol::encode(ping(1));
    partial->write_all(std::span(wire).first(1), std::chrono::steady_clock::now() + 2s);
    TcpClient active(server.port(), client_config, storage);
    EXPECT_EQ(active.request(ping(2)).request_id, 2U);
    EXPECT_EQ(server.snapshot().requests, 1U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, ExcessConnectionsAreClosedWithoutExceedingSlots) {
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); },
                     protocol::kDefaultMaxPayloadSize, {false, 2, 2, 1000000});
    Running running(server);
    TcpClient first(server.port(), client_config, storage), second(server.port(), client_config, storage);
    ASSERT_EQ(first.request(ping(1)).request_id, 1U);
    ASSERT_EQ(second.request(ping(2)).request_id, 2U);
    EXPECT_THROW(TcpClient excess(server.port(), client_config, storage), TransportError);
    ASSERT_TRUE(await([&] { return server.snapshot().rejected == 1; }));
    EXPECT_EQ(server.snapshot().peak_active, 2U);
    EXPECT_EQ(server.snapshot().active, 2U);
    EXPECT_EQ(first.request(ping(3)).request_id, 3U);
    running.stop(); EXPECT_EQ(server.snapshot().active, 0U); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, PerConnectionRequestLimitClosesAfterLastValidResponse) {
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); },
                     protocol::kDefaultMaxPayloadSize, {false, 2, 2, 2});
    Running running(server);
    TcpClient client(server.port(), client_config, storage);
    EXPECT_EQ(client.request(ping(1)).request_id, 1U);
    EXPECT_EQ(client.request(ping(2)).request_id, 2U);
    EXPECT_THROW(static_cast<void>(client.request(ping(3))), TransportError);
    EXPECT_EQ(server.snapshot().requests, 2U);
    EXPECT_EQ(server.snapshot().request_limit_closes, 1U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, ConcurrentConnectionsHaveIsolatedIdsAndSerializedHandlers) {
    std::atomic_uint in_handler{}, peak{};
    TcpServer server(server_config, 0, [&](const auto& request, const auto&) {
        const auto count = ++in_handler;
        peak.store(std::max(peak.load(), count));
        std::this_thread::yield();
        auto response = echo_response(request);
        --in_handler;
        return response;
    });
    Running running(server);
    std::vector<std::unique_ptr<TcpClient>> clients;
    for (unsigned n = 0; n < 4; ++n)
        clients.push_back(std::make_unique<TcpClient>(server.port(), client_config, storage));
    std::barrier start(4);
    std::vector<std::future<bool>> results;
    for (unsigned n = 0; n < 4; ++n) results.push_back(std::async(std::launch::async, [&, n] {
        start.arrive_and_wait();
        for (unsigned iteration = 0; iteration < 20; ++iteration) {
            const auto request = ping(n * 100 + iteration + 1);
            const auto response = clients[n]->request(request);
            if (response.request_id != request.request_id || response.payload != request.payload) return false;
        }
        return true;
    }));
    for (auto& result : results) EXPECT_TRUE(result.get());
    EXPECT_EQ(peak, 1U);
    EXPECT_EQ(server.snapshot().requests, 80U);
    EXPECT_LE(server.snapshot().peak_request_waiting, 3U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, HandlerExceptionStopsAllWorkersAndRethrowsFromServe) {
    TcpServer server(server_config, 0, [](const auto&, const auto&) -> protocol::Message {
        throw std::runtime_error("durable action completed but response lost");
    });
    Running running(server);
    TcpClient first(server.port(), client_config, storage), idle(server.port(), client_config, storage);
    EXPECT_THROW(static_cast<void>(first.request(ping(1))), TransportError);
    running.stop();
    ASSERT_TRUE(running.error);
    try { std::rethrow_exception(running.error); }
    catch (const std::runtime_error& failure) {
        EXPECT_STREQ(failure.what(), "durable action completed but response lost");
    }
    EXPECT_EQ(server.snapshot().active, 0U);
    EXPECT_EQ(server.snapshot().requests, 1U);
}
TEST_F(Dispatch, ShutdownCancelsQueuedRequestWithoutPreemptingExecutingHandler) {
    Latch release;
    std::atomic_uint effects{};
    TcpServer server(server_config, 0, [&](const auto& request, const auto&) {
        ++effects; release.wait(); return echo_response(request);
    });
    Running running(server); Release unblock{release};
    TcpClient first(server.port(), client_config, storage), second(server.port(), client_config, storage);
    auto execute = std::async(std::launch::async, [&] {
        try { static_cast<void>(first.request(ping(1))); return false; } catch (const TransportError&) { return true; }
    });
    const bool executing = await([&] { return effects == 1; });
    if (!executing) release.open();
    ASSERT_TRUE(executing);
    auto queued = std::async(std::launch::async, [&] {
        try { static_cast<void>(second.request(ping(2))); return false; } catch (const TransportError&) { return true; }
    });
    const bool waiting = await([&] { return server.snapshot().request_waiting == 1; });
    if (!waiting) release.open();
    ASSERT_TRUE(waiting);
    server.stop(); release.open();
    EXPECT_TRUE(execute.get()); EXPECT_TRUE(queued.get());
    running.stop();
    EXPECT_EQ(effects, 1U);
    EXPECT_EQ(server.snapshot().request_waiting, 0U);
    EXPECT_EQ(server.snapshot().active, 0U);
    EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, WaitingRequestExpiresBeforeAnyHandlerEffect) {
    server_config.timeout = 100ms;
    Latch release;
    std::atomic_uint effects{};
    TcpServer server(server_config, 0, [&](const auto& request, const auto&) {
        ++effects; release.wait(); return echo_response(request);
    });
    Running running(server); Release unblock{release};
    TcpClient first(server.port(), client_config, storage), second(server.port(), client_config, storage);
    auto execute = std::async(std::launch::async, [&] {
        try { static_cast<void>(first.request(ping(1))); } catch (const TransportError&) {}
    });
    const bool executing = await([&] { return effects == 1; });
    if (!executing) release.open();
    ASSERT_TRUE(executing);
    EXPECT_THROW(static_cast<void>(second.request(ping(2))), TransportError);
    EXPECT_EQ(server.snapshot().admission_expired, 1U);
    EXPECT_EQ(effects, 1U);
    release.open(); execute.get(); running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, ZeroWaitingCapacityRejectsOverloadWithoutHandlerEffect) {
    Latch release;
    std::atomic_uint effects{};
    TcpServer server(server_config, 0, [&](const auto& request, const auto&) {
        ++effects; release.wait(); return echo_response(request);
    }, protocol::kDefaultMaxPayloadSize, {false, 2, 0, 1000000});
    Running running(server); Release unblock{release};
    TcpClient first(server.port(), client_config, storage), second(server.port(), client_config, storage);
    auto execute = std::async(std::launch::async, [&] { return first.request(ping(1)).request_id; });
    const bool executing = await([&] { return effects == 1; });
    if (!executing) release.open();
    ASSERT_TRUE(executing);
    EXPECT_THROW(static_cast<void>(second.request(ping(2))), TransportError);
    EXPECT_EQ(server.snapshot().admission_rejected, 1U);
    EXPECT_EQ(server.snapshot().peak_request_waiting, 0U);
    EXPECT_EQ(effects, 1U);
    release.open(); EXPECT_EQ(execute.get(), 1U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST(DispatchDisconnect, QueuedConfirmedDisconnectDoesNotExecute) {
    Latch release;
    std::atomic_uint effects{};
    TcpServer server(0, [&](const auto& request) {
        ++effects; release.wait(); return echo_response(request);
    });
    Running running(server); Release unblock{release};
    TcpClient first(server.port());
    auto execute = std::async(std::launch::async, [&] { return first.request(ping(1)).request_id; });
    const bool executing = await([&] { return effects == 1; });
    if (!executing) release.open();
    ASSERT_TRUE(executing);
    auto queued = connect_loopback(server.port());
    send_all(queued.get(), protocol::encode(ping(2)));
    const bool waiting = await([&] { return server.snapshot().request_waiting == 1; });
    if (!waiting) release.open();
    ASSERT_TRUE(waiting);
    ::shutdown(queued.get(), SHUT_RDWR); queued.reset();
    release.open(); EXPECT_EQ(execute.get(), 1U);
    ASSERT_TRUE(await([&] { return server.snapshot().disconnect_cancellations == 1; }));
    EXPECT_EQ(effects, 1U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST(DispatchDisconnect, ExecutingDisconnectDoesNotUndoEffectsOrStopServer) {
    Latch release;
    std::atomic_uint effects{};
    TcpServer server(0, [&](const auto& request) {
        if (request.request_id == 1) { ++effects; release.wait(); }
        return echo_response(request);
    });
    Running running(server); Release unblock{release};
    auto executing = connect_loopback(server.port());
    send_all(executing.get(), protocol::encode(ping(1)));
    ASSERT_TRUE(await([&] { return effects == 1; }));
    ::shutdown(executing.get(), SHUT_RDWR); executing.reset();
    release.open();
    TcpClient next(server.port());
    EXPECT_EQ(next.request(ping(2)).request_id, 2U);
    EXPECT_EQ(effects, 1U);
    running.stop(); EXPECT_FALSE(running.error);
}
TEST_F(Dispatch, MetricsScopePropagatesIntoFixedWorkers) {
    auto collector = std::make_shared<metrics::Collector>();
    metrics::Scope scope(collector);
    TcpServer server(server_config, 0, [](const auto& request, const auto&) { return echo_response(request); });
    Running running(server);
    TcpClient client(server.port(), client_config, storage);
    EXPECT_EQ(client.request(ping(1)).request_id, 1U);
    running.stop();
    const auto report = collector->snapshot();
    EXPECT_EQ(report.values[static_cast<std::size_t>(metrics::Counter::tls_handshakes)], 2U);
    EXPECT_EQ(report.values[static_cast<std::size_t>(metrics::Counter::frames_sent)], 2U);
    EXPECT_EQ(report.values[static_cast<std::size_t>(metrics::Counter::frames_received)], 2U);
    EXPECT_FALSE(running.error);
}
} // namespace
} // namespace atlasfs::net
