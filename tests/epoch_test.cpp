#include "atlasfs/epoch.hpp"
#include "atlasfs/epoch_protocol.hpp"
#include "atlasfs/replica_catalog.hpp"
#include "atlasfs/remote_storage_node.hpp"
#include "atlasfs/checksum.hpp"
#include "tls_test_support.hpp"
#include <gtest/gtest.h>
#include <atomic>
#include <fstream>
#include <future>
#include <limits>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>

namespace atlasfs::epoch {
namespace {
const Token zero{"deployment", 0, "", ""};
const Token one{"deployment", 1, "primary", "activation-1"};
const Token two{"deployment", 2, "replacement", "activation-2"};
class EpochStore : public ::testing::Test {
protected:
    test::Certificates temporary;
    std::filesystem::path directory{temporary.root() / "epoch"};
    EpochStore() { Store::provision(directory, one); }
    std::shared_ptr<Store> open(FaultInjector fault = {}) { return std::make_shared<Store>(directory, one, std::move(fault)); }
};
class Node : public Participant {
public:
    Node(std::string name, std::shared_ptr<Store> store) : name(std::move(name)), store(std::move(store)) {}
    std::string_view id() const noexcept override { return name; }
    Result activate_epoch(const Token& old, const Token& next) override {
        ++calls;
        if (partition) return {};
        const auto result = store->advance(old, next);
        if (lost) { lost = false; return {}; }
        return result;
    }
    Result query_epoch() const override {
        if (partition) return {};
        return {Status::accepted, store->snapshot().current};
    }
    std::string name;
    std::shared_ptr<Store> store;
    bool partition{}, lost{};
    unsigned calls{};
};
TEST(EpochRecord, StrictRoundTripAndMalformedEncoding) {
    const State state{one, two, {"a", "b"}};
    const auto bytes = encode_state(state);
    EXPECT_EQ(decode_state(bytes), state);
    for (std::size_t size = 0; size < bytes.size(); ++size)
        EXPECT_FALSE(decode_state(std::span(bytes).first(size)));
    auto changed = bytes; changed.back() ^= std::byte{1}; EXPECT_FALSE(decode_state(changed));
    changed = bytes; changed.push_back(std::byte{0}); EXPECT_FALSE(decode_state(changed));
    EXPECT_THROW(encode_state({one, two, {"b", "a"}}), std::invalid_argument);
    EXPECT_THROW(encode_state({one, two, {"a", "a"}}), std::invalid_argument);
    EXPECT_THROW(encode_token({"deployment", 0, "primary", "id"}), std::invalid_argument);
    std::vector<std::string> oversized;
    for (unsigned n = 0; n < 4096; ++n) {
        auto id = std::to_string(10000 + n); id.resize(255, 'a'); oversized.push_back(std::move(id));
    }
    EXPECT_THROW(encode_state({one, two, oversized}), std::length_error);
}
TEST(EpochWire, EnvelopesPreserveCanonicalPayloadAndRejectMalformedData) {
    const std::vector<std::byte> canonical{std::byte{0}, std::byte{2}, std::byte{42}};
    const auto bytes = encode_request(one, canonical);
    ASSERT_TRUE(decode_request(bytes));
    EXPECT_EQ(decode_request(bytes)->token, one); EXPECT_EQ(decode_request(bytes)->payload, canonical);
    for (std::size_t n = 0; n < bytes.size(); ++n) EXPECT_FALSE(decode_request(std::span(bytes).first(n)));
    auto changed = bytes; changed[3] = std::byte{1}; EXPECT_FALSE(decode_request(changed));
    changed = bytes; changed.push_back(std::byte{0}); EXPECT_FALSE(decode_request(changed));
    EXPECT_FALSE(decode_request(canonical));
    const auto activation = encode_activation({one, two});
    ASSERT_TRUE(decode_activation(activation)); EXPECT_EQ(decode_activation(activation)->desired, two);
    EXPECT_EQ(decode_result(encode_result({Status::accepted, two}))->token, two);
    EXPECT_THROW(encode_result({Status::accepted, std::nullopt}), std::invalid_argument);
}
TEST_F(EpochStore, ActiveEpochAndSuccessorPersistAcrossRestart) {
    { auto store = open(); EXPECT_NO_THROW(store->authorize(one));
      EXPECT_EQ(store->advance(one, two).status, Status::accepted);
      EXPECT_THROW(store->authorize(one), Error); EXPECT_NO_THROW(store->authorize(two)); }
    auto restarted = open(); EXPECT_EQ(restarted->snapshot().current, two); EXPECT_NO_THROW(restarted->authorize(two));
}
TEST_F(EpochStore, DuplicateConflictingStaleAndSkippedActivation) {
    auto store = open(); ASSERT_EQ(store->advance(one, two).status, Status::accepted);
    EXPECT_EQ(store->advance(one, two).status, Status::accepted);
    auto different = two; different.activation = "different";
    EXPECT_EQ(store->advance(one, different).status, Status::conflict);
    EXPECT_EQ(store->advance(zero, one).status, Status::stale);
    auto skipped = two; skipped.number = 4;
    EXPECT_EQ(store->advance(two, skipped).status, Status::conflict);
    EXPECT_EQ(store->snapshot().current, two);
}
TEST_F(EpochStore, MissingRecordNeverAutoResetsOrReenrolls) {
    auto store = open(); std::filesystem::remove(directory / "record");
    EXPECT_THROW(store->snapshot(), Error); EXPECT_ANY_THROW(Store(directory, one));
    EXPECT_THROW(Store::provision(directory, one), Error);
    std::filesystem::remove_all(directory);
    EXPECT_THROW(Store::provision(directory, one), Error);
    EXPECT_TRUE(std::filesystem::exists(temporary.root() / "epoch.enrolled"));
}
TEST_F(EpochStore, TruncatedCorruptAndForeignRecordsFailClosed) {
    for (const auto& bytes : {std::vector<std::byte>{}, std::vector<std::byte>{std::byte{42}},
         encode_state({Token{"foreign", 2, "replacement", "activation-2"}, std::nullopt, {}})}) {
        std::ofstream output(directory / "record", std::ios::binary | std::ios::trunc);
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size())); output.close();
        EXPECT_ANY_THROW(Store(directory, one));
    }
}
TEST_F(EpochStore, MissingLockAndEnrollmentFailClosed) {
    auto store = open(); std::filesystem::remove(directory / "lock");
    EXPECT_THROW(store->authorize(one), Error); EXPECT_ANY_THROW(Store(directory, one));
}
TEST_F(EpochStore, MissingEnrollmentFailsClosed) {
    auto store = open(); std::filesystem::remove(temporary.root() / "epoch.enrolled");
    EXPECT_THROW(store->snapshot(), Error); EXPECT_ANY_THROW(Store(directory, one));
}
TEST_F(EpochStore, ReplacedLockAndRolledBackPendingIntentFailClosed) {
    auto store = open();
    ASSERT_EQ(store->begin(two, {"a"}).status, Status::accepted);
    const auto old = encode_state({one, std::nullopt, {}});
    { std::ofstream output(directory / "record", std::ios::binary | std::ios::trunc);
      output.write(reinterpret_cast<const char*>(old.data()), static_cast<std::streamsize>(old.size())); }
    EXPECT_THROW(store->authorize(one), Error);
    std::filesystem::rename(directory / "lock", directory / "old-lock");
    { std::ofstream output(directory / "lock"); }
    EXPECT_THROW(store->snapshot(), Error);
}
TEST_F(EpochStore, ConfiguredMinimumRejectsValidOlderBackup) {
    EXPECT_ANY_THROW(Store(directory, two));
    auto store = open(); ASSERT_EQ(store->advance(one, two).status, Status::accepted);
    auto bytes = encode_state({one, std::nullopt, {}});
    std::ofstream output(directory / "record", std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size())); output.close();
    EXPECT_THROW(store->snapshot(), Error); EXPECT_ANY_THROW(Store(directory, two));
}
TEST_F(EpochStore, EveryDurableFailureIsUnresolvedAndNeverAcknowledged) {
    for (auto point : {FaultPoint::creation, FaultPoint::write, FaultPoint::file_sync, FaultPoint::rename}) {
        auto store = open([point](auto actual) { return point == actual; });
        EXPECT_EQ(store->advance(one, two).status, Status::unresolved);
        EXPECT_EQ(store->snapshot().current, one);
    }
}
TEST_F(EpochStore, PostRenameFailurePoisonsAllHandlesUntilReopen) {
    { auto other = open(); auto failed = open([](auto point) { return point == FaultPoint::directory_sync; });
      EXPECT_EQ(failed->advance(one, two).status, Status::unresolved);
      EXPECT_THROW(other->snapshot(), Error);
      EXPECT_THROW(failed->authorize(two), Error); }
    auto restarted = open(); EXPECT_EQ(restarted->snapshot().current, two);
}
TEST_F(EpochStore, PendingActivationBlocksOldAndNewTokens) {
    auto store = open(); ASSERT_EQ(store->begin(two, {"a", "b"}).status, Status::accepted);
    EXPECT_THROW(store->authorize(one), Error);
    EXPECT_THROW(store->authorize(two), Error);
    EXPECT_EQ(store->begin(two, {"a", "b"}).status, Status::accepted);
    EXPECT_EQ(store->begin(two, {"a"}).status, Status::conflict);
}
TEST_F(EpochStore, FailedCoordinatorIntentBlocksAllLocalHandles) {
    { auto other = open(); auto failed = open([](auto point) { return point == FaultPoint::rename; });
      EXPECT_EQ(failed->begin(two, {"a"}).status, Status::unresolved);
      EXPECT_THROW(other->authorize(one), Error); }
    auto restarted = open(); EXPECT_EQ(restarted->snapshot().current, one);
}
TEST_F(EpochStore, ProtectedMutationAndNestedGuardsSerializeActivation) {
    auto store = open(); auto another = open();
    auto outer = store->authorize(one); { auto nested = another->authorize(one); }
    std::promise<void> attempting; auto started = attempting.get_future();
    auto future = std::async(std::launch::async, [&] { attempting.set_value(); return another->advance(one, two); });
    started.wait(); EXPECT_EQ(future.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
    outer.reset(); EXPECT_EQ(future.get().status, Status::accepted);
    EXPECT_THROW(store->authorize(one), Error);
}
TEST_F(EpochStore, CounterExhaustionCannotWrap) {
    auto token = one; token.number = std::numeric_limits<std::uint64_t>::max();
    const auto bytes = encode_state({token, std::nullopt, {}});
    std::ofstream output(directory / "record", std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size())); output.close();
    auto store = open(); EXPECT_EQ(store->advance(token, zero).status, Status::conflict);
}
class EpochActivation : public EpochStore {
protected:
    std::shared_ptr<Store> coordinator{open()};
    std::shared_ptr<Node> node(std::string id) {
        const auto path = temporary.root() / id; Store::provision(path, one);
        return std::make_shared<Node>(std::move(id), std::make_shared<Store>(path, one));
    }
};
TEST_F(EpochActivation, CompleteActivationAndDuplicateAreDurable) {
    auto a = node("a"), b = node("b"); Coordinator service(*coordinator, {a, b});
    EXPECT_EQ(service.activate(two.coordinator, two.activation).token, two);
    EXPECT_EQ(coordinator->snapshot().current, two);
    EXPECT_EQ(service.activate(two.coordinator, two.activation).token, two);
    EXPECT_EQ(a->calls, 1U); EXPECT_EQ(b->calls, 1U);
}
TEST(EpochActivationEnrollment, ZeroCannotAuthorizeAndFirstEpochRequiresAllNodes) {
    test::Certificates temporary;
    Store::provision(temporary.root() / "coordinator", zero);
    Store authority(temporary.root() / "coordinator", zero);
    Store::provision(temporary.root() / "node", zero);
    auto participant = std::make_shared<Node>("a", std::make_shared<Store>(temporary.root() / "node", zero));
    EXPECT_THROW(authority.authorize(zero), Error);
    Coordinator service(authority, {participant});
    EXPECT_EQ(service.activate(one.coordinator, one.activation).token, one);
    EXPECT_NO_THROW(authority.authorize(one));
    EXPECT_NO_THROW(participant->store->authorize(one));
}
TEST_F(EpochActivation, LostResponseRequiresExplicitAllNodeConfirmation) {
    auto a = node("a"), b = node("b"); a->lost = true;
    Coordinator service(*coordinator, {a, b});
    EXPECT_EQ(service.activate(two.coordinator, two.activation).status, Status::unresolved);
    EXPECT_EQ(coordinator->snapshot().proposed, two);
    EXPECT_THROW(coordinator->authorize(one), Error);
    Coordinator restarted(*coordinator, {a, b});
    EXPECT_EQ(restarted.reconcile().token, two);
    EXPECT_EQ(a->calls, 1U); EXPECT_EQ(b->calls, 1U);
}
TEST_F(EpochActivation, PartitionAndDisagreementCannotBePromotedToSuccess) {
    auto a = node("a"), b = node("b"); b->partition = true;
    Coordinator service(*coordinator, {a, b});
    EXPECT_EQ(service.activate(two.coordinator, two.activation).status, Status::unresolved);
    EXPECT_EQ(a->store->snapshot().current, two); EXPECT_EQ(b->store->snapshot().current, one);
    EXPECT_EQ(service.reconcile().status, Status::unresolved);
    b->partition = false;
    EXPECT_EQ(service.reconcile().status, Status::unresolved);
    EXPECT_EQ(service.activate("replacement", "other").status, Status::conflict);
    EXPECT_EQ(service.resume_activation().token, two);
    EXPECT_EQ(coordinator->snapshot().current, two);
}
TEST_F(EpochActivation, RestartCannotOmitAnUnconfirmedParticipant) {
    auto a = node("a"), b = node("b"); b->partition = true;
    Coordinator service(*coordinator, {a, b});
    ASSERT_EQ(service.activate(two.coordinator, two.activation).status, Status::unresolved);
    Coordinator incomplete(*coordinator, {a}); EXPECT_EQ(incomplete.reconcile().status, Status::conflict);
    EXPECT_EQ(coordinator->snapshot().current, one);
}
TEST_F(EpochActivation, IndependentNodeAdvanceConflictsWithPendingIntent) {
    auto a = node("a"), b = node("b");
    Token foreign = two; foreign.activation = "foreign";
    ASSERT_EQ(b->store->advance(one, foreign).status, Status::accepted);
    Coordinator service(*coordinator, {a, b});
    EXPECT_EQ(service.activate(two.coordinator, two.activation).status, Status::unresolved);
    EXPECT_EQ(service.reconcile().status, Status::unresolved);
    EXPECT_THROW(coordinator->authorize(two), Error);
}
class EpochRpc : public ::testing::Test {
protected:
    test::Certificates temporary;
    std::filesystem::path journal{temporary.root() / "journal"};
    std::vector<std::byte> data{std::byte{42}};
    std::uint32_t checksum{integrity::crc32c(data)};
    storage::ObjectManifest manifest{1, 1, checksum, "generation-1", {{0, 1, checksum}}};
    net::TlsConfig server_config{temporary.config({"d", net::PeerRole::storage})};
    net::TlsConfig old_config{temporary.config({"primary", net::PeerRole::coordinator})};
    net::TlsConfig new_config{temporary.config({"replacement", net::PeerRole::coordinator})};
    std::shared_ptr<Store> fence;
    std::shared_ptr<replication::LocalStorageNode> storage_node;
    EpochRpc() {
        std::filesystem::create_directory(journal); Store::provision(journal / "epoch", one);
        fence = std::make_shared<Store>(journal / "epoch", one);
        storage_node = std::make_shared<replication::LocalStorageNode>("d", temporary.root() / "data", 1024, 1);
        test::Certificates::allow(server_config, old_config.local_identity);
        test::Certificates::allow(server_config, new_config.local_identity);
        server_config.peers[net::certificate_identity(new_config.local_identity)].may_activate_epoch = true;
        test::Certificates::allow(old_config, server_config.local_identity);
        test::Certificates::allow(new_config, server_config.local_identity);
    }
    class Running {
    public:
        Running(std::shared_ptr<replication::StorageNode> node, const std::filesystem::path& journal,
                net::TlsConfig config, std::shared_ptr<Store> fence, bool lost = false)
            : server(std::move(node), journal, std::move(config), 0, false, false, std::move(fence), lost),
              thread([this] { try { server.serve(); } catch (...) { error = std::current_exception(); } }) {}
        ~Running() { stop(); }
        void stop() { server.stop(); if (thread.joinable()) thread.join(); }
        replication::StorageNodeServer server;
        std::exception_ptr error;
        std::thread thread;
    };
};
TEST_F(EpochRpc, ActiveEpochMutationsPreserveCanonicalPrepareFingerprint) {
    Running running(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient client("d", running.server.port(), old_config, one);
    const std::vector<std::uint32_t> indices{0};
    ASSERT_EQ(client.prepare("operation", "object", manifest, indices, data), storage::StoreError::none);
    const auto canonical = storage_node_protocol::encode_prepare({"operation", "object", manifest, indices, data});
    EXPECT_EQ(client.query_operation("operation").fingerprint, integrity::crc32c(canonical));
    EXPECT_EQ(client.commit("operation", "object", "generation-1"), storage::StoreError::none);
    EXPECT_EQ(client.get_chunk("object", "generation-1", 0).data, data);
}
TEST_F(EpochRpc, PublicationGuardOrdersConcurrentActivationBeforeNextRequest) {
    std::promise<void> publishing, release;
    auto entered = publishing.get_future(); auto proceed = release.get_future();
    bool first = true;
    storage_node = std::make_shared<replication::LocalStorageNode>("d", temporary.root() / "ordered", 1024, 1,
        [&](auto point) {
            if (point == storage::PutFaultPoint::generation_rename && first) {
                first = false; publishing.set_value(); proceed.wait();
            }
            return false;
        });
    Running running(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient client("d", running.server.port(), old_config, one);
    auto publication = std::async(std::launch::async, [&] {
        return client.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data);
    });
    if (entered.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
        release.set_value(); FAIL() << "publication did not reach durable transition";
    }
    std::promise<void> attempting; auto started = attempting.get_future();
    auto activation = std::async(std::launch::async, [&] {
        attempting.set_value(); return fence->advance(one, two);
    });
    started.wait(); EXPECT_EQ(activation.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(publication.get().status, replication::RepairPublishStatus::published);
    EXPECT_EQ(activation.get().status, Status::accepted);
    EXPECT_EQ(client.remove("object", "generation-1"), storage::StoreError::stale_epoch);
    EXPECT_EQ(client.get_chunk("object", "generation-1", 0).data, data);
}
TEST_F(EpochRpc, CorruptEpochDuringOpenSessionRejectsPublicationWithoutJournal) {
    Running running(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient client("d", running.server.port(), old_config, one);
    { std::ofstream output(journal / "epoch" / "record", std::ios::trunc); output << "truncated"; }
    EXPECT_EQ(client.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data).status,
              replication::RepairPublishStatus::epoch_unresolved);
    EXPECT_TRUE(std::filesystem::is_empty(journal / "repairs"));
    EXPECT_EQ(storage_node->get_chunk("object", "generation-1", 0).error, storage::StoreError::not_found);
}
TEST_F(EpochRpc, MalformedFencedOperationRecordStopsRestart) {
    std::filesystem::create_directory(journal / "operations");
    { std::ofstream output(journal / "operations" / "operation"); output << "broken"; }
    EXPECT_ANY_THROW(replication::StorageNodeServer(storage_node, journal, server_config, 0,
        false, false, fence));
}
TEST_F(EpochRpc, PersistentOldTlsSessionRejectsAllMutationsAfterNewEpoch) {
    Running running(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient client("d", running.server.port(), old_config, one);
    ASSERT_EQ(client.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data).status,
              replication::RepairPublishStatus::published);
    ASSERT_EQ(fence->advance(one, two).status, Status::accepted);
    const std::vector<std::uint32_t> indices{0};
    EXPECT_EQ(client.prepare("new-op", "new-object", manifest, indices, data), storage::StoreError::stale_epoch);
    EXPECT_EQ(client.commit("operation", "object", "generation-1"), storage::StoreError::stale_epoch);
    EXPECT_EQ(client.abort("operation", "object", "generation-1"), storage::StoreError::stale_epoch);
    EXPECT_EQ(client.remove("object", "generation-1"), storage::StoreError::stale_epoch);
    EXPECT_EQ(client.publish_repair("new-plan", "new-execution", "d", "other", manifest,
        manifest.chunks[0], data).status, replication::RepairPublishStatus::stale_epoch);
    EXPECT_EQ(client.get_chunk("object", "generation-1", 0).data, data);
    EXPECT_FALSE(std::filesystem::exists(journal / "repairs" / "new-execution"));
    EXPECT_TRUE(std::filesystem::is_empty(journal / "operations"));
}
TEST_F(EpochRpc, TlsAuthenticationDoesNotAuthorizeActivationOrUnfencedMutations) {
    Running running(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient client("d", running.server.port(), old_config);
    const auto publication = client.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data);
    EXPECT_EQ(publication.status, replication::RepairPublishStatus::stale_epoch);
    auto own = two; own.coordinator = one.coordinator;
    EXPECT_EQ(client.activate_epoch(one, own).status, Status::unauthorized);
    EXPECT_EQ(fence->snapshot().current, one);
    EXPECT_TRUE(std::filesystem::is_empty(journal / "repairs"));
}
TEST_F(EpochRpc, ActivationIdentityBindingDuplicateAndConflict) {
    Running running(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient client("d", running.server.port(), new_config);
    auto claimed = two; claimed.coordinator = one.coordinator;
    EXPECT_EQ(client.activate_epoch(one, claimed).status, Status::unauthorized);
    ASSERT_EQ(client.activate_epoch(one, two).token, two);
    EXPECT_EQ(client.activate_epoch(one, two).status, Status::accepted);
    auto conflicting = two; conflicting.activation = "conflicting";
    EXPECT_EQ(client.activate_epoch(one, conflicting).status, Status::conflict);
    EXPECT_EQ(client.query_epoch().token, two);
}
TEST_F(EpochRpc, RestartRetainsHighWaterAndBlocksEpochAdoptionOfOldRepair) {
    { Running original(storage_node, journal, server_config, fence);
      replication::RemoteStorageNodeClient old("d", original.server.port(), old_config, one);
      ASSERT_EQ(old.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data).status,
                replication::RepairPublishStatus::published); }
    ASSERT_EQ(fence->advance(one, two).status, Status::accepted);
    fence.reset(); fence = std::make_shared<Store>(journal / "epoch", one);
    { Running restarted(storage_node, journal, server_config, fence);
      replication::RemoteStorageNodeClient old("d", restarted.server.port(), old_config, one);
      EXPECT_EQ(old.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data).status,
                replication::RepairPublishStatus::stale_epoch); }
    { Running restarted(storage_node, journal, server_config, fence);
      replication::RemoteStorageNodeClient current("d", restarted.server.port(), new_config, two);
      EXPECT_EQ(current.publish_repair("plan", "execution", "d", "object", manifest, manifest.chunks[0], data).status,
                replication::RepairPublishStatus::target_rejected); }
    EXPECT_EQ(storage_node->get_chunk("object", "generation-1", 0).data, data);
}
TEST_F(EpochRpc, NewEpochCannotCommitAnOldEpochPreparedOperation) {
    { Running original(storage_node, journal, server_config, fence);
      replication::RemoteStorageNodeClient old("d", original.server.port(), old_config, one);
      ASSERT_EQ(old.prepare("operation", "object", manifest, std::vector<std::uint32_t>{0}, data), storage::StoreError::none); }
    ASSERT_EQ(fence->advance(one, two).status, Status::accepted);
    Running restarted(storage_node, journal, server_config, fence);
    replication::RemoteStorageNodeClient current("d", restarted.server.port(), new_config, two);
    EXPECT_EQ(current.commit("operation", "object", "generation-1"), storage::StoreError::stale_epoch);
    EXPECT_EQ(current.abort("operation", "object", "generation-1"), storage::StoreError::stale_epoch);
}
TEST_F(EpochRpc, LostActivationResponseReconcilesOnlyAfterAuthenticatedQuery) {
    const auto coordinator_root = temporary.root() / "coordinator";
    std::filesystem::create_directory(coordinator_root); Store::provision(coordinator_root / "epoch", one);
    Store authority(coordinator_root / "epoch", one);
    { Running first(storage_node, journal, server_config, fence, true);
      auto remote = std::make_shared<replication::RemoteStorageNodeClient>("d", first.server.port(), new_config);
      Coordinator service(authority, {remote});
      EXPECT_EQ(service.activate(two.coordinator, two.activation).status, Status::unresolved);
      first.stop(); EXPECT_TRUE(first.error);
      EXPECT_EQ(authority.snapshot().proposed, two); EXPECT_THROW(authority.authorize(two), Error); }
    Running restarted(storage_node, journal, server_config, fence);
    auto reconnected = std::make_shared<replication::RemoteStorageNodeClient>("d", restarted.server.port(), new_config);
    Coordinator recovered(authority, {reconnected});
    EXPECT_EQ(recovered.reconcile().token, two); EXPECT_NO_THROW(authority.authorize(two));
}
TEST_F(EpochRpc, TwoNodeAuthenticatedActivationRequiresBothDurableAcknowledgements) {
    const auto coordinator_root = temporary.root() / "coordinator";
    std::filesystem::create_directory(coordinator_root); Store::provision(coordinator_root / "epoch", one);
    Store authority(coordinator_root / "epoch", one);
    const auto second_journal = temporary.root() / "second";
    std::filesystem::create_directory(second_journal); Store::provision(second_journal / "epoch", one);
    std::atomic_bool fail{true};
    auto second_fence = std::make_shared<Store>(second_journal / "epoch", one,
        [&](auto point) { return fail && point == FaultPoint::file_sync; });
    auto second = std::make_shared<replication::LocalStorageNode>("e", temporary.root() / "second-data", 1024, 1);
    auto second_config = temporary.config({"e", net::PeerRole::storage});
    test::Certificates::allow(second_config, new_config.local_identity, {"e"});
    second_config.peers.begin()->second.may_activate_epoch = true;
    test::Certificates::allow(new_config, second_config.local_identity, {"e"});
    Running first(storage_node, journal, server_config, fence);
    Running other(second, second_journal, second_config, second_fence);
    auto d = std::make_shared<replication::RemoteStorageNodeClient>("d", first.server.port(), new_config);
    auto e = std::make_shared<replication::RemoteStorageNodeClient>("e", other.server.port(), new_config);
    Coordinator service(authority, {d, e});
    EXPECT_EQ(service.activate(two.coordinator, two.activation).status, Status::unresolved);
    EXPECT_EQ(service.reconcile().status, Status::unresolved);
    EXPECT_EQ(fence->snapshot().current, two); EXPECT_EQ(second_fence->snapshot().current, one);
    EXPECT_THROW(authority.authorize(one), Error);
    EXPECT_THROW(authority.authorize(two), Error);
    fail = false;
    EXPECT_EQ(service.resume_activation().token, two);
}
TEST_F(EpochRpc, EnrolledRootCannotBeOpenedWithPlaintextOrUnfencedTls) {
    EXPECT_THROW(replication::StorageNodeServer(storage_node, journal), std::invalid_argument);
    EXPECT_THROW(replication::StorageNodeServer(storage_node, journal, server_config), std::invalid_argument);
    EXPECT_THROW(replication::RemoteStorageNodeClient("d", 1, old_config, two), std::invalid_argument);
}
class EpochCatalog : public ::testing::Test {
protected:
    test::Certificates temporary;
    std::filesystem::path root{temporary.root()};
    std::shared_ptr<Store> authority;
    std::shared_ptr<Node> participant;
    EpochCatalog() {
        std::filesystem::create_directory(root / "coordinator"); Store::provision(root / "coordinator" / "epoch", one);
        authority = std::make_shared<Store>(root / "coordinator" / "epoch", one);
        Store::provision(root / "node", one);
        participant = std::make_shared<Node>("a", std::make_shared<Store>(root / "node", one));
    }
    const metadata::ChunkIdentity identity{"object", "generation-1", 0};
    const storage::ChunkMetadata chunk{0, 1, 42};
    const std::vector<metadata::CatalogReplica> replicas{{"a", metadata::CatalogReplicaState::present, 42, 1}};
    void activate() { Coordinator service(*authority, {participant}); ASSERT_EQ(service.activate(two.coordinator, two.activation).token, two); }
};
TEST_F(EpochCatalog, SupersededSessionCannotUpsertEraseOrCas) {
    metadata::DurableReplicaCatalog old(root, authority, one);
    ASSERT_EQ(old.upsert(identity, chunk, replicas), metadata::ReplicaCatalogError::none);
    const auto before = old.snapshot(); activate();
    auto changed = replicas; changed[0].evidence_version = 2;
    EXPECT_EQ(old.upsert(identity, chunk, changed), metadata::ReplicaCatalogError::epoch_rejected);
    EXPECT_EQ(old.erase(identity), metadata::ReplicaCatalogError::epoch_rejected);
    EXPECT_EQ(old.compare_exchange({identity, before.catalog_version, replicas, changed}).status,
              metadata::ReplicaCatalogCasStatus::epoch_rejected);
    EXPECT_EQ(old.snapshot(), before);
    metadata::DurableReplicaCatalog current(root, authority, two);
    EXPECT_EQ(current.compare_exchange({identity, before.catalog_version, replicas, changed}).status,
              metadata::ReplicaCatalogCasStatus::committed);
}
TEST_F(EpochCatalog, PendingActivationBlocksCatalogWithoutChangingVersion) {
    metadata::DurableReplicaCatalog catalog(root, authority, one);
    ASSERT_EQ(catalog.upsert(identity, chunk, replicas), metadata::ReplicaCatalogError::none);
    ASSERT_EQ(authority->begin(two, {"a"}).status, Status::accepted);
    EXPECT_EQ(catalog.erase(identity), metadata::ReplicaCatalogError::epoch_rejected);
    EXPECT_EQ(catalog.version(), 1U);
    EXPECT_THROW(metadata::DurableReplicaCatalog rejected(root), std::invalid_argument);
}
TEST(EpochCatalogMigration, ExistingUnfencedInstanceCannotBypassEnrollment) {
    test::Certificates temporary;
    metadata::DurableReplicaCatalog old(temporary.root());
    Store::provision(temporary.root() / "coordinator" / "epoch", one);
    EXPECT_EQ(old.upsert({"object", "generation-1", 0}, {0, 1, 42},
        {{"a", metadata::CatalogReplicaState::present, 42, 1}}), metadata::ReplicaCatalogError::epoch_rejected);
    EXPECT_EQ(old.version(), 0U);
}
TEST_F(EpochCatalog, EpochCannotChangeDuringCatalogPublication) {
    std::promise<void> at_sync, release;
    auto arrived = at_sync.get_future(); auto released = release.get_future();
    metadata::DurableReplicaCatalog catalog(root, authority, one, [&](auto point) {
        if (point == metadata::ReplicaCatalogFaultPoint::record_sync) { at_sync.set_value(); released.wait(); }
        return false;
    });
    auto publication = std::async(std::launch::async, [&] { return catalog.upsert(identity, chunk, replicas); });
    arrived.wait();
    auto activation = std::async(std::launch::async, [&] { Coordinator service(*authority, {participant});
        return service.activate(two.coordinator, two.activation); });
    EXPECT_EQ(activation.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
    release.set_value(); EXPECT_EQ(publication.get(), metadata::ReplicaCatalogError::none);
    EXPECT_EQ(activation.get().token, two);
    EXPECT_EQ(catalog.erase(identity), metadata::ReplicaCatalogError::epoch_rejected);
}
TEST_F(EpochCatalog, ObsoleteProcessReloadsAuthorityBeforeCatalogMutation) {
    metadata::DurableReplicaCatalog initial(root, authority, one);
    ASSERT_EQ(initial.upsert(identity, chunk, replicas), metadata::ReplicaCatalogError::none);
    int ready[2]{}, go[2]{}; ASSERT_EQ(::pipe(ready), 0); ASSERT_EQ(::pipe(go), 0);
    const pid_t child = ::fork(); ASSERT_GE(child, 0);
    if (child == 0) {
        ::close(ready[0]); ::close(go[1]);
        try {
            auto own = std::make_shared<Store>(root / "coordinator" / "epoch", one);
            metadata::DurableReplicaCatalog old(root, own, one);
            char byte = 'x'; if (::write(ready[1], &byte, 1) != 1 || ::read(go[0], &byte, 1) != 1) ::_exit(2);
            ::_exit(old.erase(identity) == metadata::ReplicaCatalogError::epoch_rejected ? 0 : 3);
        } catch (...) { ::_exit(4); }
    }
    ::close(ready[1]); ::close(go[0]); char byte{};
    EXPECT_EQ(::read(ready[0], &byte, 1), 1); activate();
    EXPECT_EQ(::write(go[1], &byte, 1), 1);
    int status{}; EXPECT_EQ(::waitpid(child, &status, 0), child);
    EXPECT_TRUE(WIFEXITED(status)); EXPECT_EQ(WEXITSTATUS(status), 0);
    ::close(ready[0]); ::close(go[1]);
    EXPECT_EQ(initial.version(), 1U);
}
} // namespace
} // namespace atlasfs::epoch
