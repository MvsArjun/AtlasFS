#include "atlasfs/checksum.hpp"
#include "atlasfs/remote_storage_node.hpp"
#include "atlasfs/recovery.hpp"
#include "atlasfs/storage_node_protocol.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <thread>
#include <unistd.h>

namespace atlasfs::replication {
namespace {

class TempRoot {
public:
    TempRoot() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() /
            ("atlasfs-rpc-test-" + std::to_string(static_cast<unsigned long long>(::getpid())) +
             "-" + std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }
    ~TempRoot() { std::error_code error; std::filesystem::remove_all(path_, error); }
    const std::filesystem::path& path() const { return path_; }
private:
    std::filesystem::path path_;
};

std::vector<std::byte> data() {
    return {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5}};
}

storage::ObjectManifest manifest(std::string generation = "generation-rpc") {
    const auto bytes = data();
    return {bytes.size(), 4, integrity::crc32c(bytes), std::move(generation),
            {{0, 4, integrity::crc32c(std::span(bytes).first(4))},
             {1, 1, integrity::crc32c(std::span(bytes).subspan(4))}}};
}

metadata::PlacementPlan plan_for(const storage::ObjectManifest& value) {
    metadata::MetadataCoordinator coordinator(
        std::make_unique<metadata::RendezvousPlacement>(), 2);
    EXPECT_EQ(coordinator.register_node(
        {"node-a", {"127.0.0.1", 7000}, metadata::NodeState::active}),
        metadata::CoordinatorError::none);
    EXPECT_EQ(coordinator.register_node(
        {"node-b", {"127.0.0.1", 7001}, metadata::NodeState::active}),
        metadata::CoordinatorError::none);
    return *coordinator.place_object({"object", value}).plan;
}

class RunningStorageServer {
public:
    RunningStorageServer(std::shared_ptr<StorageNode> node,
                         const std::filesystem::path& journal)
        : server(std::move(node), journal), thread([this] { server.serve(); }) {}
    ~RunningStorageServer() { server.stop(); thread.join(); }
    StorageNodeServer server;
private:
    std::thread thread;
};

class ObservedStorageNode final : public StorageNode {
public:
    explicit ObservedStorageNode(std::shared_ptr<LocalStorageNode> value) : local(std::move(value)) {}
    std::string_view id() const noexcept override { return local->id(); }
    storage::StoreError prepare(std::string_view operation, std::string_view object,
        const storage::ObjectManifest& value, std::span<const std::uint32_t> chunks,
        std::span<const std::byte> bytes) override {
        ++prepares; return local->prepare(operation, object, value, chunks, bytes);
    }
    storage::StoreError verify_prepared(std::string_view operation, std::string_view object,
        std::string_view generation, std::span<const std::uint32_t> chunks) const override {
        return local->verify_prepared(operation, object, generation, chunks);
    }
    storage::StoreError commit(std::string_view operation, std::string_view object,
        std::string_view generation) override {
        ++commits; if (before_commit) before_commit();
        return local->commit(operation, object, generation);
    }
    storage::StoreError abort(std::string_view operation, std::string_view object,
        std::string_view generation) override {
        ++aborts; if (before_abort) before_abort();
        return local->abort(operation, object, generation);
    }
    storage::StoreError finalize(std::string_view operation, std::string_view object,
        std::string_view generation) override { return local->finalize(operation, object, generation); }
    storage::ChunkResult get_chunk(std::string_view object, std::string_view generation,
        std::uint32_t index) const override { return local->get_chunk(object, generation, index); }
    storage::StoreError remove(std::string_view object, std::string_view generation) override {
        return local->remove(object, generation);
    }
    OperationQueryResult query_operation(std::string_view operation) const override {
        return local->query_operation(operation);
    }
    std::shared_ptr<LocalStorageNode> local;
    std::function<void()> before_commit, before_abort;
    std::atomic_uint prepares{}, commits{}, aborts{};
};

TEST(StorageNodeProtocol, RoundTripsAllBoundedPayloadShapes) {
    namespace snp = storage_node_protocol;
    const snp::PrepareRequest prepare{"op-1", "object", manifest(), {0, 1}, data()};
    ASSERT_EQ(snp::decode_prepare(snp::encode_prepare(prepare))->operation_id, "op-1");
    const snp::IdentityRequest identity{"op-1", "object", "generation-rpc"};
    EXPECT_EQ(snp::decode_identity(snp::encode_identity(identity))->generation_id,
              "generation-rpc");
    const snp::GetChunkRequest get{"object", "generation-rpc", 1};
    EXPECT_EQ(snp::decode_get_chunk(snp::encode_get_chunk(get))->chunk_index, 1U);
    snp::Response response{storage::StoreError::none, "op-1",
                           snp::OperationState::prepared, 123, {}};
    EXPECT_EQ(snp::decode_response(snp::encode_response(response))->state,
              snp::OperationState::prepared);
    const snp::RepairChunkRequest repair{"repair-plan", "execution-1", "node-b",
        "object", manifest(), manifest().chunks[1], {std::byte{5}}};
    EXPECT_EQ(snp::decode_repair_chunk(snp::encode_repair_chunk(repair)), repair);
    const snp::RepairResponse repair_response{
        snp::RepairResponseStatus::published, "repair-plan", "execution-1", 42};
    EXPECT_EQ(snp::decode_repair_response(snp::encode_repair_response(repair_response)),
              repair_response);
}

TEST(StorageNodeProtocol, RejectsTruncationUnknownVersionAndUnsafeIdentifiers) {
    namespace snp = storage_node_protocol;
    auto encoded = snp::encode_identity({"op-1", "object", "generation-rpc"});
    encoded.pop_back();
    EXPECT_FALSE(snp::decode_identity(encoded));
    encoded = snp::encode_get_chunk({"object", "generation-rpc", 0});
    encoded[1] = std::byte{3};
    EXPECT_FALSE(snp::decode_get_chunk(encoded));
    EXPECT_FALSE(snp::valid_operation_id("../operation"));
    EXPECT_THROW(static_cast<void>(snp::encode_identity(
        {"../operation", "object", "generation-rpc"})),
                 std::invalid_argument);
}

TEST(StorageNodeProtocol, StrictlyRejectsMalformedRepairPayloads) {
    namespace snp = storage_node_protocol;
    const snp::RepairChunkRequest request{"repair-plan", "execution-1", "node-b",
        "object", manifest(), manifest().chunks[1], {std::byte{5}}};
    const auto encoded = snp::encode_repair_chunk(request);
    for (std::size_t size = 0; size < encoded.size(); ++size) {
        EXPECT_FALSE(snp::decode_repair_chunk(
            std::span<const std::byte>(encoded).first(size)));
    }
    auto unsupported = encoded;
    unsupported[1] = std::byte{3};
    EXPECT_FALSE(snp::decode_repair_chunk(unsupported));
    auto trailing = encoded;
    trailing.push_back(std::byte{0});
    EXPECT_FALSE(snp::decode_repair_chunk(trailing));
    auto reserved = encoded;
    reserved[3] = std::byte{1};
    EXPECT_FALSE(snp::decode_repair_chunk(reserved));
    EXPECT_THROW(static_cast<void>(snp::encode_repair_chunk(
        {"repair-plan", "execution-1", "../node", "object", manifest(),
         manifest().chunks[1], {std::byte{5}}})),
        std::invalid_argument);
    EXPECT_THROW(static_cast<void>(snp::encode_repair_chunk(
        {"../plan", "execution-1", "node-b", "object", manifest(),
         manifest().chunks[1], {std::byte{5}}})),
        std::invalid_argument);
}

TEST(StorageNodeProtocol, RemoteRepairValidatesIdentityAndIsIdempotent) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>(
        "node-a", root.path() / "store", 1024, 4);
    StorageNodeServer server(local, root.path() / "journal");
    std::thread thread([&] { server.serve(); });
    RemoteStorageNodeClient remote("node-a", server.port());
    const auto value = manifest();
    const std::vector<std::byte> chunk{std::byte{5}};

    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-wrong", "node-b",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::identity_mismatch);
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-1", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::published);
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-1", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::published);
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-1", "node-a",
        "different-object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::target_rejected);
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-2", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::already_present_valid);
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-bad-checksum", "node-a",
        "object", value, value.chunks[1], std::vector<std::byte>{std::byte{4}}).status,
        RepairPublishStatus::checksum_mismatch);
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-bad-manifest", "node-a",
        "object", value,
        storage::ChunkMetadata{1, 1, value.chunks[1].checksum + 1U}, chunk).status,
        RepairPublishStatus::invalid_manifest);
    EXPECT_EQ(remote.get_chunk("object", value.generation_id, 1).data, chunk);
    {
        std::ofstream corrupt(local->store().chunks_directory() /
            value.generation_id / "chunk_000001", std::ios::binary | std::ios::trunc);
        corrupt.put('\0');
    }
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-corrupt", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::target_rejected);
    server.stop();
    thread.join();
}

TEST(StorageNodeProtocol, LostRepairResponseIsUnknownAndDurableRetryIsIdempotent) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>(
        "node-a", root.path() / "store", 1024, 4);
    const auto value = manifest();
    const std::vector<std::byte> chunk{std::byte{5}};
    {
        StorageNodeServer server(local, root.path() / "journal", 0, false, true);
        std::thread thread([&] { try { server.serve(); } catch (...) {} });
        RemoteStorageNodeClient remote("node-a", server.port());
        EXPECT_EQ(remote.publish_repair("repair-plan", "execution-lost", "node-a",
            "object", value, value.chunks[1], chunk).status,
            RepairPublishStatus::result_unknown);
        thread.join();
    }
    EXPECT_EQ(local->get_chunk("object", value.generation_id, 1).data, chunk);
    StorageNodeServer restarted(local, root.path() / "journal");
    std::thread thread([&] { restarted.serve(); });
    RemoteStorageNodeClient remote("node-a", restarted.port());
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-lost", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::published);
    restarted.stop();
    thread.join();
}

TEST(StorageNodeProtocol, FailedRemotePublicationNeverExposesPartialReplica) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>(
        "node-a", root.path() / "store", 1024, 4,
        [](storage::PutFaultPoint point) {
            return point == storage::PutFaultPoint::chunk_write;
        });
    StorageNodeServer server(local, root.path() / "journal");
    std::thread thread([&] { server.serve(); });
    RemoteStorageNodeClient remote("node-a", server.port());
    const auto value = manifest();
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-failed", "node-a",
        "object", value, value.chunks[1], std::vector<std::byte>{std::byte{5}}).status,
        RepairPublishStatus::publication_failure);
    EXPECT_EQ(local->get_chunk("object", value.generation_id, 1).error,
              storage::StoreError::not_found);
    server.stop();
    thread.join();
}

TEST(StorageNodeProtocol, RemoteDirectorySyncAmbiguityIsNeverReportedCommitted) {
    TempRoot root;
    unsigned directory_syncs{};
    auto local = std::make_shared<LocalStorageNode>(
        "node-a", root.path() / "store", 1024, 4,
        [&](storage::PutFaultPoint point) {
            return point == storage::PutFaultPoint::directory_sync &&
                   ++directory_syncs == 4U;
        });
    StorageNodeServer server(local, root.path() / "journal");
    std::thread thread([&] { server.serve(); });
    RemoteStorageNodeClient remote("node-a", server.port());
    const auto value = manifest();
    const std::vector<std::byte> chunk{std::byte{5}};
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-unknown", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::result_unknown);
    EXPECT_EQ(local->get_chunk("object", value.generation_id, 1).data, chunk);
    server.stop();
    thread.join();
}

TEST(StorageNodeProtocol, CorruptDurableRepairJournalStopsRestart) {
    TempRoot root;
    const auto repairs = root.path() / "journal" / "repairs";
    std::filesystem::create_directories(repairs);
    {
        std::ofstream output(repairs / "execution-corrupt", std::ios::trunc);
        output << "not a repair record\n";
    }
    auto local = std::make_shared<LocalStorageNode>(
        "node-a", root.path() / "store", 1024, 4);
    EXPECT_THROW(StorageNodeServer(local, root.path() / "journal"),
                 std::runtime_error);
}

TEST(StorageNodeProtocol, SuccessfulJournalCannotHideMissingGenerationAfterRestart) {
    TempRoot root;
    const auto value = manifest();
    const std::vector<std::byte> chunk{std::byte{5}};
    {
        auto local = std::make_shared<LocalStorageNode>(
            "node-a", root.path() / "store", 1024, 4);
        StorageNodeServer server(local, root.path() / "journal");
        std::thread thread([&] { server.serve(); });
        RemoteStorageNodeClient remote("node-a", server.port());
        EXPECT_EQ(remote.publish_repair("repair-plan", "execution-missing", "node-a",
            "object", value, value.chunks[1], chunk).status,
            RepairPublishStatus::published);
        server.stop(); thread.join();
        std::filesystem::remove_all(local->store().chunks_directory() / value.generation_id);
    }
    auto local = std::make_shared<LocalStorageNode>(
        "node-a", root.path() / "store", 1024, 4);
    StorageNodeServer server(local, root.path() / "journal");
    std::thread thread([&] { server.serve(); });
    RemoteStorageNodeClient remote("node-a", server.port());
    EXPECT_EQ(remote.publish_repair("repair-plan", "execution-missing", "node-a",
        "object", value, value.chunks[1], chunk).status,
        RepairPublishStatus::target_rejected);
    server.stop(); thread.join();
}

TEST(StorageNodeProtocol, DuplicatePrepareCommitAndConflictAreDeterministic) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    StorageNodeServer server(local, root.path() / "journal");
    std::thread thread([&] { server.serve(); });
    RemoteStorageNodeClient remote("node-a", server.port());
    const auto bytes = data();
    const auto first = manifest();
    ASSERT_EQ(remote.prepare("op-1", "object", first, std::vector<std::uint32_t>{0, 1}, bytes),
              storage::StoreError::none);
    EXPECT_EQ(remote.prepare("op-1", "object", first, std::vector<std::uint32_t>{0, 1}, bytes),
              storage::StoreError::none);
    auto conflicting = manifest("generation-rpc");
    conflicting.object_checksum++;
    EXPECT_EQ(remote.prepare("op-1", "object", conflicting, std::vector<std::uint32_t>{0, 1}, bytes),
              storage::StoreError::operation_conflict);
    EXPECT_EQ(remote.commit("op-1", "object", first.generation_id), storage::StoreError::none);
    EXPECT_EQ(remote.commit("op-1", "object", first.generation_id), storage::StoreError::none);
    EXPECT_EQ(remote.abort("op-1", "object", first.generation_id),
              storage::StoreError::invalid_operation_state);
    EXPECT_EQ(remote.get_chunk("object", first.generation_id, 1).data,
              std::vector<std::byte>{std::byte{5}});
    EXPECT_EQ(remote.remove("object", first.generation_id), storage::StoreError::none);
    server.stop(); thread.join();
}

TEST(StorageNodeProtocol, CommitJournalFailureCannotBecomeSuccessThroughReplayOrQuery) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    const auto journal = root.path() / "journal";
    const auto temporary = journal / "operations" / "op-persist.tmp";
    const auto value = manifest();
    auto observed = std::make_shared<ObservedStorageNode>(local);
    observed->before_commit = [&] { std::filesystem::create_directory(temporary); };
    {
        RunningStorageServer running(observed, journal);
        RemoteStorageNodeClient remote("node-a", running.server.port());
        ASSERT_EQ(remote.prepare("op-persist", "object", value,
            std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::none);
        EXPECT_EQ(remote.commit("op-persist", "object", value.generation_id),
                  storage::StoreError::result_unknown);
        EXPECT_EQ(remote.query("op-persist"), storage_node_protocol::OperationState::unknown);
        EXPECT_EQ(remote.commit("op-persist", "object", value.generation_id),
                  storage::StoreError::result_unknown);
        EXPECT_EQ(remote.abort("op-persist", "object", value.generation_id),
                  storage::StoreError::invalid_operation_state);
        EXPECT_EQ(remote.get_chunk("object", value.generation_id, 1).data,
                  std::vector<std::byte>{std::byte{5}});
        ASSERT_TRUE(std::filesystem::remove(temporary));
        EXPECT_EQ(remote.commit("op-persist", "object", value.generation_id),
                  storage::StoreError::none);
        EXPECT_EQ(remote.query("op-persist"), storage_node_protocol::OperationState::committed);
        EXPECT_EQ(observed->commits.load(), 1U);
        EXPECT_EQ(observed->aborts.load(), 0U);
    }
    RunningStorageServer restarted(local, journal);
    RemoteStorageNodeClient remote("node-a", restarted.server.port());
    EXPECT_EQ(remote.query("op-persist"), storage_node_protocol::OperationState::committed);
    EXPECT_EQ(remote.commit("op-persist", "object", value.generation_id), storage::StoreError::none);
}

TEST(StorageNodeProtocol, AbortJournalFailureCannotBecomeSuccessThroughReplayOrQuery) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    const auto journal = root.path() / "journal";
    const auto temporary = journal / "operations" / "op-abort.tmp";
    const auto value = manifest();
    auto observed = std::make_shared<ObservedStorageNode>(local);
    observed->before_abort = [&] { std::filesystem::create_directory(temporary); };
    {
        RunningStorageServer running(observed, journal);
        RemoteStorageNodeClient remote("node-a", running.server.port());
        ASSERT_EQ(remote.prepare("op-abort", "object", value,
            std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::none);
        EXPECT_EQ(remote.abort("op-abort", "object", value.generation_id),
                  storage::StoreError::result_unknown);
        EXPECT_EQ(remote.query("op-abort"), storage_node_protocol::OperationState::unknown);
        EXPECT_EQ(remote.abort("op-abort", "object", value.generation_id),
                  storage::StoreError::result_unknown);
        EXPECT_EQ(remote.commit("op-abort", "object", value.generation_id),
                  storage::StoreError::invalid_operation_state);
        ASSERT_TRUE(std::filesystem::remove(temporary));
        EXPECT_EQ(remote.abort("op-abort", "object", value.generation_id),
                  storage::StoreError::none);
        EXPECT_EQ(remote.query("op-abort"), storage_node_protocol::OperationState::aborted);
        EXPECT_EQ(observed->aborts.load(), 1U);
        EXPECT_EQ(observed->commits.load(), 0U);
    }
    RunningStorageServer restarted(local, journal);
    RemoteStorageNodeClient remote("node-a", restarted.server.port());
    EXPECT_EQ(remote.query("op-abort"), storage_node_protocol::OperationState::aborted);
    EXPECT_EQ(remote.abort("op-abort", "object", value.generation_id), storage::StoreError::none);
}

TEST(StorageNodeProtocol, PrepareJournalFailureRequiresOriginalPrepareBeforeMutation) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    const auto journal = root.path() / "journal";
    const auto temporary = journal / "operations" / "op-prepare.tmp";
    const auto value = manifest();
    RunningStorageServer running(local, journal);
    RemoteStorageNodeClient remote("node-a", running.server.port());
    ASSERT_TRUE(std::filesystem::create_directory(temporary));
    EXPECT_EQ(remote.prepare("op-prepare", "object", value,
        std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::result_unknown);
    EXPECT_EQ(remote.query("op-prepare"), storage_node_protocol::OperationState::unknown);
    EXPECT_EQ(remote.commit("op-prepare", "object", value.generation_id),
              storage::StoreError::result_unknown);
    EXPECT_EQ(remote.abort("op-prepare", "object", value.generation_id),
              storage::StoreError::result_unknown);
    EXPECT_EQ(remote.prepare("op-prepare", "object", value,
        std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::result_unknown);
    auto changed = value;
    ++changed.object_checksum;
    EXPECT_EQ(remote.prepare("op-prepare", "object", changed,
        std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::operation_conflict);
    ASSERT_TRUE(std::filesystem::remove(temporary));
    EXPECT_EQ(remote.prepare("op-prepare", "object", value,
        std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::none);
    EXPECT_EQ(remote.query("op-prepare"), storage_node_protocol::OperationState::prepared);
    EXPECT_EQ(remote.commit("op-prepare", "object", value.generation_id), storage::StoreError::none);
}

TEST(StorageNodeProtocol, CommitIntentMustBeDurableBeforePhysicalExecution) {
    TempRoot root;
    const auto journal = root.path() / "journal";
    const auto temporary = journal / "operations" / "op-intent.tmp";
    auto observed = std::make_shared<ObservedStorageNode>(
        std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4));
    const auto value = manifest();
    RunningStorageServer running(observed, journal);
    RemoteStorageNodeClient remote("node-a", running.server.port());
    ASSERT_EQ(remote.prepare("op-intent", "object", value,
        std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::none);
    ASSERT_TRUE(std::filesystem::create_directory(temporary));
    EXPECT_EQ(remote.commit("op-intent", "object", value.generation_id),
              storage::StoreError::result_unknown);
    EXPECT_EQ(remote.query("op-intent"), storage_node_protocol::OperationState::unknown);
    EXPECT_EQ(remote.commit("op-intent", "object", value.generation_id),
              storage::StoreError::result_unknown);
    EXPECT_EQ(observed->commits.load(), 0U);
    EXPECT_EQ(remote.abort("op-intent", "object", value.generation_id),
              storage::StoreError::invalid_operation_state);
    EXPECT_EQ(observed->aborts.load(), 0U);
    ASSERT_TRUE(std::filesystem::remove(temporary));
    EXPECT_EQ(remote.commit("op-intent", "object", value.generation_id), storage::StoreError::none);
    EXPECT_EQ(observed->commits.load(), 1U);
}

TEST(StorageNodeProtocol, TerminalJournalFailureRemainsUnknownAfterProcessRestart) {
    for (const bool commit : {true, false}) {
        SCOPED_TRACE(commit ? "commit" : "abort");
        TempRoot root;
        const auto journal = root.path() / "journal";
        const auto temporary = journal / "operations" / "op-restart.tmp";
        const auto value = manifest();
        {
            auto observed = std::make_shared<ObservedStorageNode>(
                std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4));
            auto block_terminal_journal = [&] { std::filesystem::create_directory(temporary); };
            if (commit) observed->before_commit = block_terminal_journal;
            else observed->before_abort = block_terminal_journal;
            RunningStorageServer running(observed, journal);
            RemoteStorageNodeClient remote("node-a", running.server.port());
            ASSERT_EQ(remote.prepare("op-restart", "object", value,
                std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::none);
            EXPECT_EQ(commit ? remote.commit("op-restart", "object", value.generation_id)
                : remote.abort("op-restart", "object", value.generation_id),
                storage::StoreError::result_unknown);
            EXPECT_EQ(remote.query("op-restart"), storage_node_protocol::OperationState::unknown);
        }
        ASSERT_TRUE(std::filesystem::remove(temporary));
        auto restarted_node = std::make_shared<LocalStorageNode>(
            "node-a", root.path() / "store", 1024, 4);
        RunningStorageServer restarted(restarted_node, journal);
        auto remote = std::make_shared<RemoteStorageNodeClient>("node-a", restarted.server.port());
        EXPECT_EQ(remote->query("op-restart"), storage_node_protocol::OperationState::unknown);
        EXPECT_EQ(remote->commit("op-restart", "object", value.generation_id), storage::StoreError::result_unknown);
        EXPECT_EQ(remote->abort("op-restart", "object", value.generation_id), storage::StoreError::result_unknown);
        RecoveryCoordinator recovery({remote});
        const auto fingerprint = integrity::crc32c(storage_node_protocol::encode_prepare(
            {"op-restart", "object", value, {0, 1}, data()}));
        const RecoveryRequest request{"op-restart", fingerprint, "object", value.generation_id, {"node-a"}};
        const auto report = recovery.inspect(request);
        EXPECT_EQ(report.classification, RecoveryClassification::unknown_participant);
        EXPECT_FALSE(report.completion_allowed);
        EXPECT_FALSE(report.rollback_allowed);
        EXPECT_EQ(recovery.complete(request).status, RecoveryActionStatus::rejected);
        EXPECT_EQ(recovery.rollback(request).status, RecoveryActionStatus::rejected);
        EXPECT_EQ(remote->get_chunk("object", value.generation_id, 1).error,
                  commit ? storage::StoreError::none : storage::StoreError::not_found);
    }
}

TEST(StorageNodeProtocol, PhysicalCommitDurabilityUnknownNeverRevertsToPreparedEvidence) {
    TempRoot root;
    std::atomic_bool fail_sync{};
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4,
        [&](storage::PutFaultPoint point) {
            return fail_sync.load() && point == storage::PutFaultPoint::directory_sync;
        });
    const auto journal = root.path() / "journal";
    const auto value = manifest();
    {
        RunningStorageServer running(local, journal);
        RemoteStorageNodeClient remote("node-a", running.server.port());
        ASSERT_EQ(remote.prepare("op-sync", "object", value,
            std::vector<std::uint32_t>{0, 1}, data()), storage::StoreError::none);
        fail_sync.store(true);
        EXPECT_EQ(remote.commit("op-sync", "object", value.generation_id), storage::StoreError::result_unknown);
        fail_sync.store(false);
        EXPECT_EQ(remote.query("op-sync"), storage_node_protocol::OperationState::unknown);
        EXPECT_EQ(remote.commit("op-sync", "object", value.generation_id), storage::StoreError::result_unknown);
        EXPECT_EQ(remote.abort("op-sync", "object", value.generation_id), storage::StoreError::result_unknown);
        EXPECT_EQ(remote.get_chunk("object", value.generation_id, 1).data,
                  std::vector<std::byte>{std::byte{5}});
    }
    auto restarted_node = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    RunningStorageServer restarted(restarted_node, journal);
    RemoteStorageNodeClient remote("node-a", restarted.server.port());
    EXPECT_EQ(remote.query("op-sync"), storage_node_protocol::OperationState::unknown);
    EXPECT_EQ(remote.commit("op-sync", "object", value.generation_id), storage::StoreError::result_unknown);
    EXPECT_EQ(remote.abort("op-sync", "object", value.generation_id), storage::StoreError::result_unknown);
}

TEST(StorageNodeProtocol, ProtectedOperationJournalRejectsStateCorruptionAndMalformedWrapper) {
    const std::string payload = "0 42 0 object generation-rpc\n";
    const auto checksum = integrity::crc32c(std::as_bytes(std::span(payload)));
    const auto valid = "RECORD1\n" + payload + "CRC32C " + std::to_string(checksum) + "\n";
    auto changed_state = valid;
    changed_state[std::string("RECORD1\n").size()] = '2';
    const std::string incomplete_payload = "0 42 0 object\n";
    const auto incomplete_checksum = integrity::crc32c(std::as_bytes(std::span(incomplete_payload)));
    for (const auto& malformed : {
            changed_state, "RECORD2\n" + payload + "CRC32C " + std::to_string(checksum) + "\n",
            "RECORD1\n" + payload, valid + "trailing\n",
            "RECORD1\n" + payload + "CRC32C\n",
            "RECORD1\n" + payload + "CRC32C " + std::to_string(checksum + 1U) + "\n",
            "RECORD1\n" + incomplete_payload + "CRC32C " + std::to_string(incomplete_checksum) + "\n"}) {
        TempRoot root;
        const auto operations = root.path() / "journal" / "operations";
        std::filesystem::create_directories(operations);
        { std::ofstream output(operations / "op-corrupt", std::ios::binary); output << malformed; }
        auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
        EXPECT_THROW(StorageNodeServer(local, root.path() / "journal"), std::runtime_error);
    }
}

TEST(StorageNodeProtocol, ProtectedRepairJournalRejectsStatusCorruption) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    const auto journal = root.path() / "journal";
    const auto value = manifest();
    {
        RunningStorageServer running(local, journal);
        RemoteStorageNodeClient remote("node-a", running.server.port());
        // A rejected publication must not become published after a status-bit
        // alteration that leaves the request fingerprint and IDs untouched.
        EXPECT_EQ(remote.publish_repair("plan", "execution", "node-a", "object", value,
            value.chunks[1], std::vector<std::byte>{std::byte{4}}).status,
            RepairPublishStatus::checksum_mismatch);
    }
    {
        std::fstream record(journal / "repairs" / "execution",
            std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(record);
        record.seekp(std::string("RECORD1\n").size()); record.put('0');
    }
    EXPECT_THROW(StorageNodeServer(local, journal), std::runtime_error);
}

TEST(StorageNodeProtocol, NodeJournalReadsAreBoundedAndRequireRegularFiles) {
    for (const bool directory : {false, true}) {
        TempRoot root;
        const auto operations = root.path() / "journal" / "operations";
        std::filesystem::create_directories(operations);
        if (directory) std::filesystem::create_directory(operations / "op-invalid");
        else { std::ofstream output(operations / "op-invalid", std::ios::binary);
               output << std::string(4097, '0'); }
        auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
        EXPECT_THROW(StorageNodeServer(local, root.path() / "journal"), std::runtime_error);
    }
}

TEST(StorageNodeProtocol, LegacyUnknownRecordRemainsReadableButNeverAuthorizesMutation) {
    TempRoot root;
    const auto journal = root.path() / "journal";
    const auto operations = journal / "operations";
    std::filesystem::create_directories(operations);
    { std::ofstream output(operations / "op-legacy"); output << "0 42 0 object generation-rpc\n"; }
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    RunningStorageServer running(local, journal);
    RemoteStorageNodeClient remote("node-a", running.server.port());
    EXPECT_EQ(remote.query("op-legacy"), storage_node_protocol::OperationState::unknown);
    EXPECT_EQ(remote.commit("op-legacy", "object", "generation-rpc"), storage::StoreError::result_unknown);
    EXPECT_EQ(remote.abort("op-legacy", "object", "generation-rpc"), storage::StoreError::result_unknown);
}

TEST(StorageNodeProtocol, LegacyPreparedProbeCorruptionIsUnknownRatherThanCommitted) {
    TempRoot root;
    const auto journal = root.path() / "journal";
    const auto operations = journal / "operations";
    std::filesystem::create_directories(operations);
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    const auto value = manifest();
    ASSERT_EQ(local->prepare("op-legacy", "object", value, std::vector<std::uint32_t>{0, 1}, data()),
              storage::StoreError::none);
    ASSERT_EQ(local->commit("op-legacy", "object", value.generation_id), storage::StoreError::none);
    { std::ofstream output(operations / "op-legacy"); output << "1 42 0 object generation-rpc\n"; }
    { std::ofstream output(local->store().chunks_directory() / value.generation_id / "chunk_000000",
        std::ios::binary | std::ios::trunc); output << "bad!"; }
    RunningStorageServer running(local, journal);
    RemoteStorageNodeClient remote("node-a", running.server.port());
    EXPECT_EQ(remote.query("op-legacy"), storage_node_protocol::OperationState::unknown);
    EXPECT_EQ(remote.commit("op-legacy", "object", value.generation_id), storage::StoreError::result_unknown);
}

TEST(StorageNodeProtocol, LostCommitResponseIsUnknownAndDurableQueryResolvesIt) {
    TempRoot root;
    auto local = std::make_shared<LocalStorageNode>("node-a", root.path() / "store", 1024, 4);
    std::uint16_t first_port{};
    {
        StorageNodeServer server(local, root.path() / "journal", 0, true);
        first_port = server.port();
        std::thread thread([&] { try { server.serve(); } catch (...) {} });
        RemoteStorageNodeClient remote("node-a", first_port);
        const auto bytes = data(); const auto value = manifest("generation-lost");
        ASSERT_EQ(remote.prepare("op-lost", "object", value,
                                 std::vector<std::uint32_t>{0, 1}, bytes),
                  storage::StoreError::none);
        EXPECT_EQ(remote.commit("op-lost", "object", value.generation_id),
                  storage::StoreError::result_unknown);
        thread.join();
    }
    StorageNodeServer restarted(local, root.path() / "journal");
    std::thread thread([&] { restarted.serve(); });
    RemoteStorageNodeClient query("node-a", restarted.port());
    EXPECT_EQ(query.query("op-lost"),
              storage_node_protocol::OperationState::committed);
    EXPECT_EQ(query.commit("op-lost", "object", "generation-lost"),
              storage::StoreError::none);
    restarted.stop(); thread.join();
}

TEST(StorageNodeProtocol, ReplicaManagerTreatsLocalAndRemoteNodesUniformly) {
    TempRoot root;
    auto local_a = std::make_shared<LocalStorageNode>("node-a", root.path() / "a", 1024, 4);
    auto local_b = std::make_shared<LocalStorageNode>("node-b", root.path() / "b", 1024, 4);
    StorageNodeServer server(local_b, root.path() / "journal");
    std::thread thread([&] { server.serve(); });
    auto remote_b = std::make_shared<RemoteStorageNodeClient>("node-b", server.port());
    std::vector<std::shared_ptr<StorageNode>> nodes{local_a, remote_b};
    ReplicaManager manager(std::move(nodes));
    const auto bytes = data();
    const auto plan = plan_for(manifest("generation-mixed"));
    ASSERT_EQ(manager.put(plan, bytes).status, ReplicationStatus::success);
    EXPECT_EQ(manager.get(plan).data, bytes);
    server.stop(); thread.join();
}

}  // namespace
}  // namespace atlasfs::replication
