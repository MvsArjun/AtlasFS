#include "atlasfs/checksum.hpp"
#include "atlasfs/remote_storage_node.hpp"
#include "atlasfs/storage_node_protocol.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
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
    EXPECT_THROW(snp::encode_identity({"../operation", "object", "generation-rpc"}),
                 std::invalid_argument);
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
