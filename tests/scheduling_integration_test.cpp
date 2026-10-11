#include "../benchmarks/scenario.hpp"
#include "atlasfs/admission.hpp"
#include "atlasfs/epoch_protocol.hpp"
#include <gtest/gtest.h>
#include <array>
#include <algorithm>
#include <atomic>
#include <barrier>
#include <condition_variable>
#include <future>
#include <sys/socket.h>

namespace atlasfs::scheduling {
namespace {
using Clock = std::chrono::steady_clock;
struct PublicationPause {
    std::mutex mutex;
    std::condition_variable changed;
    bool entered{}, released{};
    void pause() {
        std::unique_lock lock(mutex); entered = true; changed.notify_all();
        changed.wait(lock, [&] { return released; });
    }
    bool wait() {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(5), [&] { return entered; });
    }
    void release() { std::lock_guard lock(mutex); released = true; changed.notify_all(); }
};
struct ReleaseOnExit { PublicationPause& pause; ~ReleaseOnExit() { pause.release(); } };
template<class F> bool eventually(F predicate) {
    const auto deadline = Clock::now() + std::chrono::seconds(5);
    while (!predicate()) { if (Clock::now() >= deadline) return false; std::this_thread::yield(); }
    return true;
}
class FencedNode {
public:
    test::Certificates certificates;
    const epoch::Token one{"scheduling", 1, "primary", "activation-1"};
    const epoch::Token two{"scheduling", 2, "replacement", "activation-2"};
    const std::filesystem::path journal{certificates.root() / "journal"};
    net::TlsConfig server_config{certificates.config({"d", net::PeerRole::storage})};
    net::TlsConfig primary{certificates.config({"primary", net::PeerRole::coordinator})};
    net::TlsConfig replacement{certificates.config({"replacement", net::PeerRole::coordinator})};
    std::shared_ptr<epoch::Store> fence, authority;
    std::shared_ptr<replication::LocalStorageNode> node;
    std::unique_ptr<replication::StorageNodeServer> server;
    std::thread thread;
    std::exception_ptr failure;
    const std::vector<std::byte> data{bench::payload(8, 42)};
    const storage::ObjectManifest manifest{bench::manifest(data, 8, "generation-1")};
    explicit FencedNode(storage::PutFaultInjector fault = {}) {
        std::filesystem::create_directory(journal);
        epoch::Store::provision(journal / "epoch", one);
        fence = std::make_shared<epoch::Store>(journal / "epoch", one);
        std::filesystem::create_directory(certificates.root() / "coordinator");
        epoch::Store::provision(certificates.root() / "coordinator" / "epoch", one);
        authority = std::make_shared<epoch::Store>(certificates.root() / "coordinator" / "epoch", one);
        node = std::make_shared<replication::LocalStorageNode>("d", certificates.root() / "data", 1024, 8, std::move(fault));
        test::Certificates::allow(server_config, primary.local_identity, {"d"});
        test::Certificates::allow(server_config, replacement.local_identity, {"d"});
        server_config.peers.at(net::certificate_identity(replacement.local_identity)).may_activate_epoch = true;
        test::Certificates::allow(primary, server_config.local_identity, {"d"});
        test::Certificates::allow(replacement, server_config.local_identity, {"d"});
        server_config.timeout = primary.timeout = replacement.timeout = std::chrono::seconds(20);
    }
    ~FencedNode() { stop(); }
    void start(bool lost = false) {
        failure = nullptr;
        server = std::make_unique<replication::StorageNodeServer>(node, journal, server_config, 0, false, lost, fence);
        thread = std::thread([this] { try { server->serve(); } catch (...) { failure = std::current_exception(); } });
    }
    void stop() { if (server) server->stop(); if (thread.joinable()) thread.join(); }
    std::unique_ptr<replication::RemoteStorageNodeClient> client() {
        return std::make_unique<replication::RemoteStorageNodeClient>("d", server->port(), primary, one);
    }
    protocol::Message repair_request(std::string object, std::string execution) const {
        const auto payload = storage_node_protocol::encode_repair_chunk(
            {"plan", std::move(execution), "d", std::move(object), manifest, manifest.chunks[0], data});
        return {protocol::MessageType::storage_repair_chunk_request, 0, 77, epoch::encode_request(one, payload)};
    }
};
TEST(SchedulingIntegration, SharedClientAndDurableCoordinatorHistoryRemainSerialized) {
    FencedNode fixture; fixture.start();
    auto remote = std::make_shared<replication::RemoteStorageNodeClient>("d", fixture.server->port(), fixture.primary, fixture.one);
    replication::ReplicaManager replicas({remote});
    metadata::CoordinatorOperationStore history(fixture.certificates.root());
    metadata::DurableReplicaManager durable(history, replicas);
    AdmissionQueue gate(4); std::barrier start(4);
    std::array<std::thread, 4> workers;
    std::atomic_uint failures{0};
    for (std::size_t worker = 0; worker < workers.size(); ++worker) workers[worker] = std::thread([&, worker] {
        start.arrive_and_wait();
        for (unsigned iteration = 0; iteration < 3; ++iteration) {
            try {
                const auto id = "object-" + std::to_string(worker) + '-' + std::to_string(iteration);
                const auto manifest = bench::manifest(fixture.data, 8, "generation-" + id);
                const auto plan = bench::placement(id, manifest, {"d"});
                auto permit = gate.acquire(Clock::now() + std::chrono::seconds(20));
                if (!permit.lease) { ++failures; continue; }
                // Acquire caller admission before the distinct coordinator epoch guard.
                // Holding the storage node's own guard across an RPC would deadlock.
                auto authority = fixture.authority->authorize(fixture.one);
                if (durable.put(plan, fixture.data, "operation-" + id, 77).status != replication::ReplicationStatus::success ||
                    replicas.get(plan).data != fixture.data) ++failures;
            } catch (...) { ++failures; }
        }
    });
    for (auto& worker : workers) worker.join();
    fixture.stop();
    EXPECT_EQ(failures, 0U); EXPECT_FALSE(fixture.failure);
    EXPECT_EQ(history.list().size(), 12U); EXPECT_EQ(gate.snapshot().admitted, 12U);
    for (const auto& record : history.list()) EXPECT_EQ(record.state, metadata::CoordinatorOperationState::committed);
    metadata::CoordinatorOperationStore reopened(fixture.certificates.root()); EXPECT_EQ(reopened.list(), history.list());
}
TEST(SchedulingIntegration, SimultaneousRepairReplayUsesIsolatedClientsAndOneDurableIdentity) {
    FencedNode fixture; fixture.start(); std::barrier start(4);
    std::array<replication::RepairPublishStatus, 4> results{}; std::array<std::thread, 4> workers;
    for (std::size_t n = 0; n < workers.size(); ++n) workers[n] = std::thread([&, n] {
        bool synchronized = false;
        try {
            auto client = fixture.client(); start.arrive_and_wait();
            synchronized = true;
            results[n] = client->publish_repair("plan", "execution", "d", "object", fixture.manifest, fixture.manifest.chunks[0], fixture.data).status;
        } catch (...) {
            results[n] = replication::RepairPublishStatus::connection_failure;
            if (!synchronized) start.arrive_and_drop();
        }
    });
    for (auto& worker : workers) worker.join();
    fixture.stop();
    // Exact replay returns the original durable status, not a new publication.
    EXPECT_EQ(std::count(results.begin(), results.end(), replication::RepairPublishStatus::published), 4);
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(fixture.journal / "repairs"), {}), 1);
    EXPECT_EQ(fixture.node->get_chunk("object", "generation-1", 0).data, fixture.data); EXPECT_FALSE(fixture.failure);
}
TEST(SchedulingIntegration, QueuedActivationFencesTheFollowingStaleRequestAfterPublication) {
    PublicationPause pause; bool first = true;
    FencedNode fixture([&](auto point) {
        if (point == storage::PutFaultPoint::generation_rename && first) { first = false; pause.pause(); }
        return false;
    }); fixture.start();
    auto original = fixture.client(), stale = fixture.client();
    replication::RemoteStorageNodeClient replacement("d", fixture.server->port(), fixture.replacement, fixture.two);
    std::future<replication::RepairPublishResult> publication, late;
    std::future<epoch::Result> activation;
    ReleaseOnExit release{pause};
    publication = std::async(std::launch::async, [&] {
        return original->publish_repair("plan", "early-execution", "d", "early", fixture.manifest, fixture.manifest.chunks[0], fixture.data);
    });
    EXPECT_TRUE(pause.wait());
    activation = std::async(std::launch::async, [&] { return replacement.activate_epoch(fixture.one, fixture.two); });
    const bool activation_queued = eventually([&] { return fixture.server->snapshot().request_waiting == 1; });
    EXPECT_TRUE(activation_queued);
    late = std::async(std::launch::async, [&] {
        return stale->publish_repair("plan", "late-execution", "d", "late", fixture.manifest, fixture.manifest.chunks[0], fixture.data);
    });
    EXPECT_TRUE(eventually([&] { return fixture.server->snapshot().request_waiting == 2; })); pause.release();
    EXPECT_EQ(publication.get().status, replication::RepairPublishStatus::published);
    EXPECT_EQ(activation.get().status, epoch::Status::accepted);
    EXPECT_EQ(late.get().status, replication::RepairPublishStatus::stale_epoch);
    fixture.stop(); EXPECT_FALSE(fixture.failure);
    EXPECT_FALSE(std::filesystem::exists(fixture.journal / "repairs" / "late-execution"));
    EXPECT_EQ(fixture.node->get_chunk("late", "generation-1", 0).error, storage::StoreError::not_found);
}
TEST(SchedulingIntegration, DisconnectedQueuedRepairDoesNotPublish) {
    PublicationPause pause; bool first = true;
    FencedNode fixture([&](auto point) {
        if (point == storage::PutFaultPoint::generation_rename && first) { first = false; pause.pause(); }
        return false;
    }); fixture.start(); auto original = fixture.client();
    auto socket = net::connect_loopback(fixture.server->port()); net::TlsContext context(fixture.primary);
    auto stream = context.connect(socket.get(), fixture.server_config.local_identity);
    std::future<replication::RepairPublishResult> publication;
    ReleaseOnExit release{pause};
    publication = std::async(std::launch::async, [&] {
        return original->publish_repair("plan", "early-execution", "d", "early", fixture.manifest, fixture.manifest.chunks[0], fixture.data);
    });
    EXPECT_TRUE(pause.wait());
    stream->write_all(protocol::encode(fixture.repair_request("late", "late-execution")), Clock::now() + std::chrono::seconds(5));
    EXPECT_TRUE(eventually([&] { return fixture.server->snapshot().request_waiting == 1; }));
    stream.reset(); ::shutdown(socket.get(), SHUT_RDWR); socket.reset(); pause.release();
    EXPECT_EQ(publication.get().status, replication::RepairPublishStatus::published);
    EXPECT_TRUE(eventually([&] { return fixture.server->snapshot().disconnect_cancellations == 1; }));
    fixture.stop(); EXPECT_FALSE(fixture.failure);
    EXPECT_FALSE(std::filesystem::exists(fixture.journal / "repairs" / "late-execution"));
    EXPECT_EQ(fixture.node->get_chunk("late", "generation-1", 0).error, storage::StoreError::not_found);
}
TEST(SchedulingIntegration, ExecutingDisconnectCannotUndoPublicationAndRequiresExactReplay) {
    PublicationPause pause; bool first = true;
    FencedNode fixture([&](auto point) {
        if (point == storage::PutFaultPoint::generation_rename && first) { first = false; pause.pause(); }
        return false;
    }); fixture.start();
    auto socket = net::connect_loopback(fixture.server->port()); net::TlsContext context(fixture.primary);
    auto stream = context.connect(socket.get(), fixture.server_config.local_identity);
    ReleaseOnExit release{pause};
    stream->write_all(protocol::encode(fixture.repair_request("object", "execution")), Clock::now() + std::chrono::seconds(5));
    EXPECT_TRUE(pause.wait());
    stream.reset(); ::shutdown(socket.get(), SHUT_RDWR); socket.reset(); pause.release();
    auto replay = fixture.client();
    EXPECT_EQ(replay->publish_repair("plan", "execution", "d", "object", fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    EXPECT_EQ(replay->publish_repair("changed-plan", "execution", "d", "object", fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::target_rejected);
    fixture.stop(); EXPECT_FALSE(fixture.failure);
    EXPECT_EQ(fixture.node->get_chunk("object", "generation-1", 0).data, fixture.data);
}
TEST(SchedulingIntegration, LostPublicationResponseUnderOtherSessionsRemainsUnknownAndReplayable) {
    FencedNode fixture; fixture.start(true); auto publishing = fixture.client(), idle = fixture.client();
    EXPECT_EQ(publishing->publish_repair("plan", "execution", "d", "object", fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::result_unknown);
    fixture.stop(); EXPECT_TRUE(fixture.failure); publishing.reset(); idle.reset(); fixture.server.reset();
    fixture.node = std::make_shared<replication::LocalStorageNode>("d", fixture.certificates.root() / "data", 1024, 8);
    fixture.start(); auto replay = fixture.client();
    EXPECT_EQ(replay->publish_repair("plan", "execution", "d", "object", fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    fixture.stop(); EXPECT_FALSE(fixture.failure);
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(fixture.journal / "repairs"), {}), 1);
}
TEST(SchedulingIntegration, PhysicalPublicationAndConflictingCatalogCasNeverRewriteHistoricalOperations) {
    FencedNode fixture; fixture.start();
    const auto plan = bench::placement("object", fixture.manifest, {"a", "b"});
    metadata::DurableReplicaCatalog catalog(fixture.certificates.root(), fixture.authority, fixture.one);
    bench::seed_catalog(catalog, plan);
    metadata::CoordinatorOperationStore history(fixture.certificates.root());
    ASSERT_EQ(history.create(metadata::make_coordinator_operation(plan, "historical", 77)), metadata::CoordinatorRecordError::none);
    const auto historical = metadata::encode_coordinator_operation(*history.get("historical"));
    const auto before = catalog.snapshot();
    auto remote = fixture.client();
    ASSERT_EQ(remote->publish_repair("plan", "execution", "d", "object", fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    EXPECT_EQ(catalog.snapshot(), before); // Physical bytes are not authoritative.
    const auto record = *catalog.get(plan.chunks[0].identity);
    AdmissionQueue guard(2); auto held = guard.acquire();
    std::array<metadata::ReplicaCatalogCasStatus, 2> results{};
    std::atomic_uint cas_failures{0};
    std::array<std::thread, 2> threads;
    for (unsigned n = 0; n < threads.size(); ++n) threads[n] = std::thread([&, n] {
        try {
            auto permit = guard.acquire();
            if (!permit.lease) { ++cas_failures; return; }
            auto desired = record.replicas; desired.back().node_id = n == 0 ? "d" : "c";
            results[n] = catalog.compare_exchange({record.identity, before.catalog_version, record.replicas, desired}).status;
        } catch (...) { ++cas_failures; }
    });
    const bool queued = eventually([&] { return guard.snapshot().waiting == 2; }); held.lease.release();
    for (auto& thread : threads) thread.join();
    EXPECT_TRUE(queued);
    EXPECT_EQ(cas_failures, 0U);
    EXPECT_EQ(std::count(results.begin(), results.end(), metadata::ReplicaCatalogCasStatus::committed), 1);
    EXPECT_EQ(std::count(results.begin(), results.end(), metadata::ReplicaCatalogCasStatus::conflict), 1);
    EXPECT_EQ(metadata::encode_coordinator_operation(*history.get("historical")), historical);
    fixture.stop(); EXPECT_FALSE(fixture.failure);
    metadata::DurableReplicaCatalog reopened(fixture.certificates.root(), fixture.authority, fixture.one);
    EXPECT_EQ(reopened.snapshot(), catalog.snapshot());
}
} // namespace
} // namespace atlasfs::scheduling
