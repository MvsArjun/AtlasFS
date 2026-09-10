#include "atlasfs/checksum.hpp"
#include "atlasfs/replication.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace atlasfs::replication {
namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() /
            ("atlasfs-replication-test-" +
             std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
             std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }
    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    const std::filesystem::path& path() const noexcept { return path_; }
private:
    std::filesystem::path path_;
};

std::vector<std::byte> sample_data() {
    return {std::byte{0}, std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4},
            std::byte{5}, std::byte{6}, std::byte{7}, std::byte{8}, std::byte{9}};
}

storage::ObjectManifest make_manifest(std::string generation,
                                      const std::vector<std::byte>& data) {
    storage::ObjectManifest manifest{data.size(), 4, integrity::crc32c(data),
                                     std::move(generation), {}};
    for (std::uint32_t index = 0, offset = 0; offset < data.size(); ++index, offset += 4) {
        const auto length = static_cast<std::uint32_t>(std::min<std::size_t>(4, data.size() - offset));
        manifest.chunks.push_back(
            {index, length, integrity::crc32c(std::span(data).subspan(offset, length))});
    }
    return manifest;
}

metadata::PlacementPlan make_plan(std::uint32_t factor,
                                  const storage::ObjectManifest& manifest,
                                  std::size_t node_count = 3) {
    metadata::MetadataCoordinator coordinator(
        std::make_unique<metadata::RendezvousPlacement>(), factor);
    for (std::size_t index = 0; index < node_count; ++index) {
        const auto id = "node-" + std::string(1, static_cast<char>('a' + index));
        EXPECT_EQ(coordinator.register_node({id, {"127.0.0.1", 7000},
                                             metadata::NodeState::active}),
                  metadata::CoordinatorError::none);
    }
    const auto result = coordinator.place_object({"object", manifest});
    EXPECT_EQ(result.error, metadata::CoordinatorError::none);
    EXPECT_TRUE(result.plan);
    return *result.plan;
}

using LocalNodes = std::vector<std::shared_ptr<LocalStorageNode>>;

LocalNodes make_nodes(const std::filesystem::path& root,
                      storage::PutFaultInjector node_a = {},
                      storage::PutFaultInjector node_b = {},
                      storage::PutFaultInjector node_c = {}) {
    return {
        std::make_shared<LocalStorageNode>("node-a", root / "node-a", 1024, 4,
                                           std::move(node_a)),
        std::make_shared<LocalStorageNode>("node-b", root / "node-b", 1024, 4,
                                           std::move(node_b)),
        std::make_shared<LocalStorageNode>("node-c", root / "node-c", 1024, 4,
                                           std::move(node_c))};
}

std::vector<std::shared_ptr<StorageNode>> as_storage_nodes(const LocalNodes& nodes) {
    return std::vector<std::shared_ptr<StorageNode>>(nodes.begin(), nodes.end());
}

void corrupt_file(const std::filesystem::path& path) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.put('x');
}

TEST(ReplicaManager, WritesAndReadsOneTwoAndThreeReplicaPlans) {
    for (std::uint32_t factor : {1U, 2U, 3U}) {
        TemporaryDirectory directory;
        const auto data = sample_data();
        const auto manifest = make_manifest("generation-" + std::to_string(factor), data);
        const auto plan = make_plan(factor, manifest);
        auto nodes = make_nodes(directory.path());
        ReplicaManager manager(as_storage_nodes(nodes));
        const auto put = manager.put(plan, data);
        EXPECT_EQ(put.status, ReplicationStatus::success);
        EXPECT_EQ(manager.get(plan).data, data);
        for (const auto& placement : plan.chunks) {
            EXPECT_EQ(placement.replica_node_ids.size(), factor);
        }
    }
}

TEST(ReplicaManager, RefusesAnUnderReplicatedPlanBeforeWriting) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-short", data), 2);
    auto nodes = make_nodes(directory.path());
    ReplicaManager manager(as_storage_nodes(nodes));
    const auto result = manager.put(plan, data);
    EXPECT_EQ(result.status, ReplicationStatus::under_replicated);
    EXPECT_TRUE(result.successful_nodes.empty());
}

TEST(ReplicaManager, RollsBackFirstMiddleAndLastReplicaWriteFailures) {
    for (const auto failed_id : {std::string("node-a"), std::string("node-b"),
                                 std::string("node-c")}) {
        TemporaryDirectory directory;
        const auto data = sample_data();
        const auto plan = make_plan(3, make_manifest("generation-failed-" + failed_id, data));
        storage::PutFaultInjector fail = [](storage::PutFaultPoint point) {
            return point == storage::PutFaultPoint::chunk_write;
        };
        storage::PutFaultInjector fail_a = failed_id == "node-a" ? fail : storage::PutFaultInjector{};
        storage::PutFaultInjector fail_b = failed_id == "node-b" ? fail : storage::PutFaultInjector{};
        storage::PutFaultInjector fail_c = failed_id == "node-c" ? fail : storage::PutFaultInjector{};
        auto nodes = make_nodes(directory.path(), std::move(fail_a), std::move(fail_b),
                                std::move(fail_c));
        ReplicaManager manager(as_storage_nodes(nodes));
        const auto result = manager.put(plan, data);
        EXPECT_EQ(result.status, ReplicationStatus::replica_write_failed);
        EXPECT_NE(std::find(result.failed_nodes.begin(), result.failed_nodes.end(), failed_id),
                  result.failed_nodes.end());
        EXPECT_EQ(manager.get(plan).status, ReplicationStatus::all_replicas_unavailable);
    }
}

TEST(ReplicaManager, ReportsRollbackFailure) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-rollback", data));
    const auto rollback_failure = [](storage::PutFaultPoint point) {
        return point == storage::PutFaultPoint::rollback_deletion;
    };
    const auto write_failure = [](storage::PutFaultPoint point) {
        return point == storage::PutFaultPoint::chunk_write;
    };
    auto nodes = make_nodes(directory.path(), rollback_failure, {}, write_failure);
    ReplicaManager manager(as_storage_nodes(nodes));
    const auto result = manager.put(plan, data);
    EXPECT_EQ(result.status, ReplicationStatus::rollback_failed);
    EXPECT_FALSE(result.rollback_failed_nodes.empty());
}

TEST(ReplicaManager, ExposesEachReplicaPrepareFailureBoundary) {
    const std::vector<storage::PutFaultPoint> points{
        storage::PutFaultPoint::transaction_creation,
        storage::PutFaultPoint::chunk_creation,
        storage::PutFaultPoint::chunk_write,
        storage::PutFaultPoint::chunk_sync,
        storage::PutFaultPoint::manifest_creation,
        storage::PutFaultPoint::manifest_write,
        storage::PutFaultPoint::manifest_sync,
        storage::PutFaultPoint::generation_rename};
    for (const auto point : points) {
        TemporaryDirectory directory;
        const auto data = sample_data();
        const auto plan = make_plan(1, make_manifest("generation-stage", data), 1);
        storage::PutFaultInjector fail = [point](storage::PutFaultPoint current) {
            return current == point;
        };
        auto node = std::make_shared<LocalStorageNode>(
            "node-a", directory.path() / "node-a", 1024, 4, std::move(fail));
        ReplicaManager manager({node});
        EXPECT_EQ(manager.put(plan, data).status, ReplicationStatus::replica_write_failed);
    }
}

TEST(ReplicaManager, FailedReplacementPreservesOldGeneration) {
    TemporaryDirectory directory;
    const auto old_data = sample_data();
    auto new_data = old_data;
    new_data[0] = std::byte{99};
    const auto old_plan = make_plan(3, make_manifest("generation-old", old_data));
    {
        auto nodes = make_nodes(directory.path());
        ReplicaManager manager(as_storage_nodes(nodes));
        ASSERT_EQ(manager.put(old_plan, old_data).status, ReplicationStatus::success);
    }
    const auto new_plan = make_plan(3, make_manifest("generation-new", new_data));
    const auto commit_failure = [](storage::PutFaultPoint point) {
        return point == storage::PutFaultPoint::commit_rename;
    };
    auto nodes = make_nodes(directory.path(), {}, {}, commit_failure);
    ReplicaManager manager(as_storage_nodes(nodes));
    EXPECT_EQ(manager.put(new_plan, new_data).status, ReplicationStatus::replica_write_failed);
    const auto restored = manager.get(old_plan);
    EXPECT_EQ(restored.status, ReplicationStatus::success);
    EXPECT_EQ(restored.data, old_data);
}

TEST(ReplicaManager, FallsBackFromCorruptAndMissingReplicas) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-fallback", data));
    auto nodes = make_nodes(directory.path());
    ReplicaManager manager(as_storage_nodes(nodes));
    ASSERT_EQ(manager.put(plan, data).status, ReplicationStatus::success);
    const auto& replicas = plan.chunks.front().replica_node_ids;
    const auto find_node = [&](const std::string& id) -> LocalStorageNode& {
        return **std::find_if(nodes.begin(), nodes.end(), [&](const auto& node) {
            return node->id() == id;
        });
    };
    corrupt_file(find_node(replicas[0]).store().chunks_directory() /
                 plan.generation_id / "chunk_000000");
    std::filesystem::remove(find_node(replicas[1]).store().chunks_directory() /
                            plan.generation_id / "chunk_000000");
    const auto result = manager.get(plan);
    EXPECT_EQ(result.status, ReplicationStatus::success);
    EXPECT_EQ(result.data, data);
    EXPECT_EQ(result.observations[0].state, ReplicaState::corrupt);
    EXPECT_EQ(result.observations[1].state, ReplicaState::missing);
}

TEST(ReplicaManager, PersistsAcrossRestartAndToleratesPartialLoss) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-restart", data));
    {
        auto nodes = make_nodes(directory.path());
        ReplicaManager manager(as_storage_nodes(nodes));
        ASSERT_EQ(manager.put(plan, data).status, ReplicationStatus::success);
    }
    std::filesystem::remove_all(directory.path() / "node-b" / "objects" / "chunks" /
                                plan.generation_id);
    auto restarted = make_nodes(directory.path());
    ReplicaManager manager(as_storage_nodes(restarted));
    const auto result = manager.get(plan);
    EXPECT_EQ(result.status, ReplicationStatus::success);
    EXPECT_EQ(result.data, data);
}

TEST(ReplicaManager, DeleteIsIdempotentAndReportsAllKnownNodes) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-delete", data));
    auto nodes = make_nodes(directory.path());
    ReplicaManager manager(as_storage_nodes(nodes));
    ASSERT_EQ(manager.put(plan, data).status, ReplicationStatus::success);
    EXPECT_EQ(manager.remove(plan).status, ReplicationStatus::success);
    const auto repeated = manager.remove(plan);
    EXPECT_EQ(repeated.status, ReplicationStatus::success);
    EXPECT_EQ(repeated.deleted_or_missing_nodes.size(), 3U);
}

TEST(ReplicaManager, DistinguishesAllMissingFromAllCorrupt) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-invalid", data));
    auto nodes = make_nodes(directory.path());
    ReplicaManager manager(as_storage_nodes(nodes));
    EXPECT_EQ(manager.get(plan).status, ReplicationStatus::all_replicas_unavailable);
    ASSERT_EQ(manager.put(plan, data).status, ReplicationStatus::success);
    for (const auto& node : nodes) {
        corrupt_file(node->store().objects_directory() / plan.object_id);
    }
    EXPECT_EQ(manager.get(plan).status, ReplicationStatus::all_replicas_corrupt);
}

TEST(ReplicaManager, ReportsPartialDeleteWhenAPlannedNodeIsUnavailable) {
    TemporaryDirectory directory;
    const auto data = sample_data();
    const auto plan = make_plan(3, make_manifest("generation-partial-delete", data));
    auto nodes = make_nodes(directory.path());
    ReplicaManager writer(as_storage_nodes(nodes));
    ASSERT_EQ(writer.put(plan, data).status, ReplicationStatus::success);
    std::vector<std::shared_ptr<StorageNode>> available{nodes[0], nodes[1]};
    ReplicaManager incomplete(std::move(available));
    const auto result = incomplete.remove(plan);
    EXPECT_EQ(result.status, ReplicationStatus::partial_delete);
    EXPECT_EQ(result.failed_nodes, std::vector<std::string>{"node-c"});
}

}  // namespace
}  // namespace atlasfs::replication
