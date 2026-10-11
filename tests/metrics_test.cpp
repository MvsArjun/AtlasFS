#include "../benchmarks/scenario.hpp"
#include "atlasfs/tcp_client.hpp"
#include "atlasfs/tcp_server.hpp"
#include <gtest/gtest.h>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

namespace atlasfs::metrics {
namespace {
std::uint64_t apparent(const std::filesystem::path& root) {
    std::uint64_t size = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root))
        if (entry.is_regular_file() && entry.path().extension() != ".pem") size += entry.file_size();
    return size;
}
class Serving {
public:
    explicit Serving(net::TcpServer& server) : server_(server), worker_([this, owner = current_owner()] {
        Scope scope(owner);
        try { server_.serve(); } catch (...) { failure = std::current_exception(); }
    }) {}
    ~Serving() { stop(); }
    void stop() { server_.stop(); if (worker_.joinable()) worker_.join(); }
    std::exception_ptr failure;
private:
    net::TcpServer& server_;
    std::thread worker_;
};
TEST(Metrics, ConcurrentCountersHaveExactTotalsWithoutEventStorage) {
    auto collector = std::make_shared<Collector>();
    std::vector<std::thread> workers;
    for (unsigned n = 0; n < 8; ++n) workers.emplace_back([collector] {
        Scope scope(collector);
        for (unsigned k = 0; k < 10000; ++k) { add(Counter::payload_sent, 17); add(Counter::frames_sent); }
    });
    for (auto& worker : workers) worker.join();
    const auto result = collector->snapshot();
    EXPECT_EQ(result.get(Counter::payload_sent), 1360000U);
    EXPECT_EQ(result.get(Counter::frames_sent), 80000U);
    EXPECT_EQ(result.tracked_paths, 0U); EXPECT_FALSE(result.saturated);
}
TEST(Metrics, OverflowSaturatesAndQuiescentResetPreservesSnapshots) {
    Collector collector;
    collector.add(Counter::socket_sent, std::numeric_limits<std::uint64_t>::max());
    collector.add(Counter::socket_sent, 1);
    const auto old = collector.snapshot(); collector.reset();
    EXPECT_TRUE(old.saturated); EXPECT_EQ(old.get(Counter::socket_sent), std::numeric_limits<std::uint64_t>::max());
    EXPECT_EQ(collector.snapshot().get(Counter::socket_sent), 0U); EXPECT_FALSE(collector.snapshot().saturated);
}
TEST(Metrics, NestedScopesRestoreOwnersAndDisabledModeDoesNotUpdate) {
    auto outer = std::make_shared<Collector>(), inner = std::make_shared<Collector>();
    EXPECT_EQ(current(), nullptr);
    { Scope first(outer); add(Counter::frames_sent);
      { Scope second(inner); add(Counter::frames_sent, 2); }
      { Scope disabled(nullptr); add(Counter::frames_sent, 99); EXPECT_EQ(current(), nullptr); }
      add(Counter::frames_sent); }
    EXPECT_EQ(current(), nullptr);
    EXPECT_EQ(outer->snapshot().get(Counter::frames_sent), 2U);
    EXPECT_EQ(inner->snapshot().get(Counter::frames_sent), 2U);
}
TEST(Metrics, TimersKeepTheirCollectorAliveAcrossScopeChanges) {
    auto collector = std::make_shared<Collector>(); std::weak_ptr<Collector> lifetime = collector;
    std::unique_ptr<Timer> timer;
    { Scope scope(collector); timer = std::make_unique<Timer>(Counter::rpc_calls, Counter::rpc_ns); }
    collector.reset(); EXPECT_FALSE(lifetime.expired()); timer.reset(); EXPECT_TRUE(lifetime.expired());
}
TEST(Metrics, FileWrappersPreserveErrorsAndDisabledReturnValues) {
    auto collector = std::make_shared<Collector>(); Scope scope(collector);
    std::byte byte{}; errno = 0;
    EXPECT_EQ(io::read(-1, &byte, 1), -1); EXPECT_EQ(errno, EBADF);
    EXPECT_EQ(io::write(-1, &byte, 1), -1); EXPECT_EQ(errno, EBADF);
    EXPECT_EQ(io::fsync(-1), -1); EXPECT_EQ(errno, EBADF);
    { Scope disabled(nullptr); EXPECT_EQ(io::write(-1, &byte, 1), -1); EXPECT_EQ(errno, EBADF); }
    EXPECT_EQ(collector->snapshot().get(Counter::file_write_calls), 1U);
}
TEST(Metrics, PlaintextSeparatesPayloadAndFramingWithActualSocketTotals) {
    auto collector = std::make_shared<Collector>(); Scope scope(collector);
    net::TcpServer server; Serving serving(server);
    net::TcpClient client(server.port());
    const protocol::Message request{protocol::MessageType::metadata_request, 0, 7, bench::payload(17, 42)};
    EXPECT_EQ(client.request(request).payload, request.payload);
    client.close(); serving.stop(); EXPECT_FALSE(serving.failure);
    const auto result = collector->snapshot();
    EXPECT_EQ(result.get(Counter::payload_sent), 34U); EXPECT_EQ(result.get(Counter::payload_received), 34U);
    EXPECT_EQ(result.get(Counter::frames_sent), 2U); EXPECT_EQ(result.get(Counter::frames_received), 2U);
    EXPECT_EQ(result.get(Counter::framed_sent), 34U + 2 * protocol::kHeaderSize);
    EXPECT_EQ(result.get(Counter::framed_received), result.get(Counter::framed_sent));
    EXPECT_EQ(result.get(Counter::socket_sent), result.get(Counter::framed_sent));
    EXPECT_EQ(result.get(Counter::socket_received), result.get(Counter::socket_sent));
    EXPECT_EQ(result.get(Counter::tls_sent), 0U); EXPECT_GT(result.get(Counter::socket_partial_reads), 0U);
}
TEST(Metrics, TlsCountsEncryptedSocketTrafficAndHandshakeSeparately) {
    test::Certificates certificates;
    auto server_config = certificates.config({"d", net::PeerRole::storage});
    auto client_config = certificates.config({"coordinator", net::PeerRole::coordinator});
    server_config.tcp_nodelay = client_config.tcp_nodelay = true;
    test::Certificates::allow(server_config, client_config.local_identity);
    test::Certificates::allow(client_config, server_config.local_identity);
    auto collector = std::make_shared<Collector>(); Scope scope(collector);
    net::TcpServer server(server_config, 0, [](const auto& message, const auto&) { return net::echo_response(message); });
    Serving serving(server); net::TcpClient client(server.port(), client_config, server_config.local_identity);
    const protocol::Message request{protocol::MessageType::metadata_request, 0, 7, bench::payload(32768, 42)};
    EXPECT_EQ(client.request(request).payload, request.payload);
    client.close(); serving.stop(); EXPECT_FALSE(serving.failure);
    const auto result = collector->snapshot();
    EXPECT_EQ(result.get(Counter::tls_handshakes), 2U); EXPECT_GT(result.get(Counter::tls_handshake_ns), 0U);
    EXPECT_EQ(result.get(Counter::nodelay_connections), 2U);
    EXPECT_EQ(result.get(Counter::tls_sent), 2 * (request.payload.size() + protocol::kHeaderSize));
    EXPECT_EQ(result.get(Counter::tls_received), result.get(Counter::tls_sent));
    EXPECT_GT(result.get(Counter::socket_sent), result.get(Counter::tls_sent));
    EXPECT_EQ(result.get(Counter::socket_sent), result.get(Counter::socket_received));
    EXPECT_EQ(result.get(Counter::payload_sent), 2 * request.payload.size());
}
TEST(Metrics, TcpNodelayOptionCanBeEnabledAndDisabledWithoutChangingPlaintextDefaults) {
    auto collector = std::make_shared<Collector>(); Scope scope(collector);
    net::Socket socket(::socket(AF_INET, SOCK_STREAM, 0)); ASSERT_TRUE(socket.valid());
    auto option = [&] {
        int value = -1; socklen_t size = sizeof(value);
        EXPECT_EQ(::getsockopt(socket.get(), IPPROTO_TCP, TCP_NODELAY, &value, &size), 0);
        return value;
    };
    EXPECT_EQ(option(), 0);
    net::set_tcp_nodelay(socket.get(), true); EXPECT_EQ(option(), 1);
    net::set_tcp_nodelay(socket.get(), false); EXPECT_EQ(option(), 0);
    EXPECT_EQ(collector->snapshot().get(Counter::nodelay_connections), 1U);
    EXPECT_THROW(net::set_tcp_nodelay(-1, true), std::system_error);
}
TEST(Metrics, ExpiredDeadlineIsObservedWithoutChangingTimeoutBehavior) {
    int descriptors[2]; ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
    net::Socket left(descriptors[0]), right(descriptors[1]);
    auto stream = net::plaintext_stream(left.get());
    auto collector = std::make_shared<Collector>(); Scope scope(collector); std::array<std::byte, 1> buffer{};
    EXPECT_THROW(static_cast<void>(stream->read(buffer, std::chrono::steady_clock::now())), net::TransportError);
    EXPECT_EQ(collector->snapshot().get(Counter::deadlines), 1U);
    EXPECT_EQ(collector->snapshot().get(Counter::socket_read_calls), 0U);
}
TEST(Metrics, SuccessfulChunkPublicationTracksPeakAndRenameWithoutDoubleCounting) {
    test::Certificates temporary; auto collector = std::make_shared<Collector>(); Scope scope(collector);
    storage::ObjectStore store(temporary.root() / "store", object_protocol::kMaxObjectDataSize, 1024);
    collector->begin_files(temporary.root()); const auto data = bench::payload(4096, 42);
    ASSERT_EQ(store.put("object", data, integrity::crc32c(data)), storage::StoreError::none);
    const auto result = collector->snapshot();
    EXPECT_EQ(result.temporary_current, 0U); EXPECT_GE(result.temporary_peak, data.size());
    EXPECT_EQ(result.disk_current, apparent(temporary.root())); EXPECT_GE(result.disk_peak, result.disk_current);
    EXPECT_EQ(result.get(Counter::chunk_write_bytes), data.size());
    EXPECT_EQ(result.get(Counter::temporary_committed), 6U); // Four chunks, generation manifest, committed manifest.
    EXPECT_EQ(result.get(Counter::file_sync_calls), 6U); EXPECT_EQ(result.get(Counter::directory_sync_calls), 3U);
    EXPECT_EQ(result.get(Counter::rename_calls), 2U); EXPECT_TRUE(result.accounting_complete);
    EXPECT_EQ(store.get("object").data, data);
    EXPECT_GE(collector->snapshot().get(Counter::chunk_read_bytes), data.size());
}
TEST(Metrics, FailedPublicationLeavesAccountingConsistentWithObservableFiles) {
    for (auto point : {storage::PutFaultPoint::chunk_sync, storage::PutFaultPoint::manifest_sync,
                       storage::PutFaultPoint::generation_rename, storage::PutFaultPoint::commit_rename}) {
        test::Certificates temporary; auto collector = std::make_shared<Collector>(); Scope scope(collector);
        storage::ObjectStore store(temporary.root() / "store", object_protocol::kMaxObjectDataSize, 1024,
            [=](auto at) { return at == point; });
        collector->begin_files(temporary.root()); const auto data = bench::payload(4096, 42);
        EXPECT_EQ(store.put("object", data, integrity::crc32c(data)), storage::StoreError::filesystem_error);
        const auto result = collector->snapshot(); EXPECT_EQ(result.temporary_current, 0U);
        EXPECT_EQ(result.disk_current, apparent(temporary.root())); EXPECT_GT(result.temporary_peak, 0U);
        EXPECT_GT(result.get(Counter::temporary_cleaned) + result.get(Counter::temporary_committed), 0U);
        EXPECT_TRUE(result.accounting_complete);
    }
}
TEST(Metrics, ReplacementRenameAccountsDestinationAndTemporaryFileExactlyOnce) {
    test::Certificates temporary; auto collector = std::make_shared<Collector>(); Scope scope(collector);
    collector->begin_files(temporary.root()); const auto bytes = bench::payload(20, 42);
    const auto final = temporary.root() / "final", pending = temporary.root() / "next.tmp";
    const int first = io::open(final.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600); ASSERT_GE(first, 0);
    EXPECT_EQ(io::write(first, bytes.data(), 20), 20); EXPECT_EQ(io::close(first), 0);
    const int second = io::open(pending.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600); ASSERT_GE(second, 0);
    EXPECT_EQ(io::write(second, bytes.data(), 10), 10); EXPECT_EQ(io::close(second), 0);
    std::error_code error; io::rename(pending, final, error); ASSERT_FALSE(error);
    const auto result = collector->snapshot(); EXPECT_EQ(result.disk_current, 10U); EXPECT_EQ(result.disk_peak, 30U);
    EXPECT_EQ(result.temporary_peak, 10U); EXPECT_EQ(result.temporary_current, 0U);
    EXPECT_EQ(result.get(Counter::temporary_committed), 1U);
}
TEST(Metrics, RestartCleanupRemovesPreviouslyAccountedTransactionBytes) {
    test::Certificates temporary; auto collector = std::make_shared<Collector>(); Scope scope(collector);
    storage::ObjectStore initial(temporary.root() / "store", object_protocol::kMaxObjectDataSize, 1024);
    const auto pending = initial.transactions_directory() / "unfinished";
    std::filesystem::create_directory(pending); collector->begin_files(temporary.root());
    const auto path = pending / "chunk_000000";
    const auto data = bench::payload(4096, 42);
    const int fd = io::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600); ASSERT_GE(fd, 0);
    ASSERT_EQ(io::write(fd, data.data(), data.size()), 4096); ASSERT_EQ(io::close(fd), 0);
    EXPECT_EQ(collector->snapshot().temporary_current, 4096U);
    storage::ObjectStore reopened(temporary.root() / "store", object_protocol::kMaxObjectDataSize, 1024);
    EXPECT_EQ(collector->snapshot().temporary_current, 0U);
    EXPECT_EQ(collector->snapshot().disk_current, apparent(temporary.root()));
    EXPECT_GT(collector->snapshot().get(Counter::startup_entries), 0U);
}
TEST(Metrics, LostResponseAndReplayRemainIdempotentWithCollectionEnabled) {
    auto collector = std::make_shared<Collector>(); Scope scope(collector); bench::Cluster cluster(1, 1024);
    cluster.restart(0, 1024, true); collector->begin_files(cluster.root());
    const auto data = bench::payload(1024, 42);
    const auto manifest = bench::manifest(data, 1024, "generation");
    EXPECT_EQ(cluster.nodes[0].client->publish_repair("plan", "execution", "node-0", "object", manifest, manifest.chunks[0], data).status,
              replication::RepairPublishStatus::result_unknown);
    cluster.restart(0, 1024);
    EXPECT_EQ(cluster.nodes[0].client->publish_repair("plan", "execution", "node-0", "object", manifest, manifest.chunks[0], data).status,
              replication::RepairPublishStatus::published);
    const auto prior = collector->snapshot();
    EXPECT_EQ(cluster.nodes[0].client->publish_repair("plan", "execution", "node-0", "object", manifest, manifest.chunks[0], data).status,
              replication::RepairPublishStatus::published);
    EXPECT_EQ(cluster.nodes[0].client->get_chunk("object", "generation", 0).data, data);
    cluster.stop(0); const auto result = collector->snapshot();
    EXPECT_EQ(result.get(Counter::file_write_bytes), prior.get(Counter::file_write_bytes));
    EXPECT_EQ(result.get(Counter::chunk_write_bytes), data.size()); EXPECT_EQ(result.temporary_current, 0U);
    EXPECT_EQ(result.disk_current, apparent(cluster.root())); EXPECT_GT(result.get(Counter::rpc_failures), 0U);
}
TEST(Metrics, PathAccountingIsBoundedAndReportsIncompleteInsteadOfInventingPeaks) {
    test::Certificates temporary; auto collector = std::make_shared<Collector>(16); Scope scope(collector);
    collector->begin_files(temporary.root()); const auto path = temporary.root() / "file";
    const int fd = io::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600); ASSERT_GE(fd, 0);
    for (unsigned n = 0; n < 32; ++n) collector->opened(fd, (temporary.root() / ("file-" + std::to_string(n))).c_str(), 0);
    EXPECT_EQ(collector->snapshot().tracked_paths, 16U); EXPECT_FALSE(collector->snapshot().accounting_complete);
    EXPECT_EQ(io::close(fd), 0);
    EXPECT_THROW(Collector(Collector::capacity + 1), std::invalid_argument);
}
TEST(Metrics, ConcurrentFileLifecyclesKeepAccountingWhenDescriptorsAreReused) {
    test::Certificates temporary; auto collector = std::make_shared<Collector>(); Scope scope(collector);
    collector->begin_files(temporary.root()); const auto data = bench::payload(64, 42);
    std::vector<std::thread> workers;
    for (unsigned n = 0; n < 8; ++n) workers.emplace_back([&, n] {
        Scope worker_scope(collector);
        for (unsigned k = 0; k < 64; ++k) {
            const auto path = temporary.root() / ("worker-" + std::to_string(n) + '-' + std::to_string(k));
            const int fd = io::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
            EXPECT_GE(fd, 0);
            if (fd >= 0) { EXPECT_EQ(io::write(fd, data.data(), data.size()), 64); EXPECT_EQ(io::close(fd), 0); }
        }
    });
    for (auto& worker : workers) worker.join();
    const auto result = collector->snapshot(); EXPECT_TRUE(result.accounting_complete);
    EXPECT_EQ(result.get(Counter::file_write_bytes), 32768U);
    EXPECT_EQ(result.disk_current, apparent(temporary.root())); EXPECT_EQ(result.disk_current, 32768U);
}
}
} // namespace atlasfs::metrics
