#include "atlasfs/coordinator.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace atlasfs::metadata {
namespace {

class InvalidPolicy final : public PlacementPolicy {
public:
    std::optional<std::string> select(
        const ChunkIdentity&, std::span<const StorageNode>) const override {
        return "not-a-member";
    }
};

StorageNode node(std::string id, NodeState state = NodeState::active) {
    return {std::move(id), {"127.0.0.1", 7000}, state};
}

storage::ObjectManifest manifest(std::string generation = "generation-1") {
    return {9, 4, 100, std::move(generation),
            {{0, 4, 10}, {1, 4, 11}, {2, 1, 12}}};
}

void add_nodes(MetadataCoordinator& coordinator,
               const std::vector<std::string>& node_ids) {
    for (const auto& id : node_ids) {
        ASSERT_EQ(coordinator.register_node(node(id)), CoordinatorError::none);
    }
}

TEST(MetadataCoordinator, PlacesChunkOnOnlyActiveNode) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a"});
    const auto result = coordinator.place_chunk({"object", "generation-1", 0});
    EXPECT_EQ(result.error, CoordinatorError::none);
    EXPECT_EQ(result.node_id, "node-a");
}

TEST(MetadataCoordinator, ProducesDeterministicPlacementForOneTwoAndThreeNodes) {
    const ChunkIdentity chunk{"object", "generation-1", 7};
    for (const std::vector<std::string> ids : {
             std::vector<std::string>{"node-a"},
             std::vector<std::string>{"node-a", "node-b"},
             std::vector<std::string>{"node-a", "node-b", "node-c"}}) {
        MetadataCoordinator coordinator;
        add_nodes(coordinator, ids);
        const auto first = coordinator.place_chunk(chunk);
        ASSERT_EQ(first.error, CoordinatorError::none);
        for (unsigned repeat = 0; repeat < 10; ++repeat) {
            EXPECT_EQ(coordinator.place_chunk(chunk).node_id, first.node_id);
        }
    }
}

TEST(MetadataCoordinator, IsIndependentOfInsertionOrderAndCoordinatorInstance) {
    MetadataCoordinator first;
    MetadataCoordinator second;
    add_nodes(first, {"node-a", "node-b", "node-c"});
    add_nodes(second, {"node-c", "node-a", "node-b"});
    for (std::uint32_t index = 0; index < 100; ++index) {
        const ChunkIdentity chunk{"object", "generation-1", index};
        EXPECT_EQ(first.place_chunk(chunk).node_id, second.place_chunk(chunk).node_id);
    }
}

TEST(MetadataCoordinator, ExcludesDrainingAndUnavailableNodes) {
    MetadataCoordinator coordinator;
    ASSERT_EQ(coordinator.register_node(node("active")), CoordinatorError::none);
    ASSERT_EQ(coordinator.register_node(node("draining", NodeState::draining)),
              CoordinatorError::none);
    ASSERT_EQ(coordinator.register_node(node("unavailable", NodeState::unavailable)),
              CoordinatorError::none);
    for (std::uint32_t index = 0; index < 100; ++index) {
        EXPECT_EQ(coordinator.place_chunk({"object", "generation-1", index}).node_id,
                  "active");
    }
}

TEST(MetadataCoordinator, ReturnsErrorWithNoEligibleNodes) {
    MetadataCoordinator coordinator;
    ASSERT_EQ(coordinator.register_node(node("node-a", NodeState::draining)),
              CoordinatorError::none);
    const auto result = coordinator.place_chunk({"object", "generation-1", 0});
    EXPECT_EQ(result.error, CoordinatorError::no_active_nodes);
    EXPECT_FALSE(result.node_id);
}

TEST(MetadataCoordinator, MembershipChangesAdvanceVersionAndExcludeRemovedPrimary) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a", "node-b", "node-c"});
    const ChunkIdentity chunk{"object", "generation-1", 0};
    const auto original = coordinator.place_chunk(chunk).node_id;
    ASSERT_TRUE(original);
    const auto version = coordinator.membership_version();
    ASSERT_EQ(coordinator.remove_node(*original), CoordinatorError::none);
    EXPECT_EQ(coordinator.membership_version(), version + 1U);
    const auto replacement = coordinator.place_chunk(chunk).node_id;
    ASSERT_TRUE(replacement);
    EXPECT_NE(replacement, original);
}

TEST(MetadataCoordinator, StateChangeExcludesFormerPrimaryAndVersionsNewPlan) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a", "node-b"});
    const ChunkIdentity chunk{"object", "generation-1", 3};
    const auto original = coordinator.place_chunk(chunk).node_id;
    ASSERT_TRUE(original);
    ASSERT_EQ(coordinator.set_node_state(*original, NodeState::draining),
              CoordinatorError::none);
    const auto replacement = coordinator.place_chunk(chunk).node_id;
    ASSERT_TRUE(replacement);
    EXPECT_NE(replacement, original);
    const auto plan = coordinator.place_object({"object", manifest()});
    ASSERT_TRUE(plan.plan);
    EXPECT_EQ(plan.plan->membership_version, coordinator.membership_version());
}

TEST(MetadataCoordinator, ChunkIdentityIncludesGenerationAndIndex) {
    const ChunkIdentity first{"object", "generation-1", 0};
    const ChunkIdentity second{"object", "generation-2", 0};
    const ChunkIdentity third{"object", "generation-1", 1};
    EXPECT_NE(stable_chunk_hash(first), stable_chunk_hash(second));
    EXPECT_NE(stable_chunk_hash(first), stable_chunk_hash(third));
}

TEST(MetadataCoordinator, StableHashHasProcessIndependentGoldenValue) {
    EXPECT_EQ(stable_chunk_hash({"object", "generation-1", 7}),
              0x732c3bd6146e55b9ULL);
}

TEST(MetadataCoordinator, AddingNodeOnlyKeepsPrimaryOrMovesToAddedNode) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a", "node-b"});
    std::vector<std::optional<std::string>> before;
    for (std::uint32_t index = 0; index < 100; ++index) {
        before.push_back(
            coordinator.place_chunk({"object", "generation-1", index}).node_id);
    }
    ASSERT_EQ(coordinator.register_node(node("node-c")), CoordinatorError::none);
    for (std::uint32_t index = 0; index < 100; ++index) {
        const auto after =
            coordinator.place_chunk({"object", "generation-1", index}).node_id;
        EXPECT_TRUE(after == before[index] || after == "node-c");
    }
}

TEST(MetadataCoordinator, PlacementPlanPreservesAllManifestMetadata) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a", "node-b", "node-c"});
    const ObjectMetadata object{"object", manifest()};
    const auto result = coordinator.place_object(object);
    ASSERT_EQ(result.error, CoordinatorError::none);
    ASSERT_TRUE(result.plan);
    EXPECT_EQ(result.plan->object_id, object.object_id);
    EXPECT_EQ(result.plan->generation_id, object.manifest.generation_id);
    EXPECT_EQ(result.plan->object_size, object.manifest.object_size);
    EXPECT_EQ(result.plan->chunk_size, object.manifest.chunk_size);
    EXPECT_EQ(result.plan->object_checksum, object.manifest.object_checksum);
    EXPECT_EQ(result.plan->membership_version, coordinator.membership_version());
    ASSERT_EQ(result.plan->chunks.size(), object.manifest.chunks.size());
    for (std::size_t index = 0; index < result.plan->chunks.size(); ++index) {
        EXPECT_EQ(result.plan->chunks[index].chunk, object.manifest.chunks[index]);
        EXPECT_EQ(result.plan->chunks[index].identity.chunk_index, index);
        EXPECT_FALSE(result.plan->chunks[index].primary_node_id.empty());
    }
}

TEST(MetadataCoordinator, ProducesDeterministicObjectPlan) {
    MetadataCoordinator first;
    MetadataCoordinator second;
    add_nodes(first, {"node-a", "node-b", "node-c"});
    add_nodes(second, {"node-c", "node-b", "node-a"});
    const ObjectMetadata object{"object", manifest()};
    EXPECT_EQ(first.place_object(object).plan, second.place_object(object).plan);
}

TEST(MetadataCoordinator, AllowsEmptyObjectPlanWithoutActiveNodes) {
    MetadataCoordinator coordinator;
    const ObjectMetadata object{"empty", {0, 4, 0, "generation-empty", {}}};
    const auto result = coordinator.place_object(object);
    ASSERT_EQ(result.error, CoordinatorError::none);
    ASSERT_TRUE(result.plan);
    EXPECT_TRUE(result.plan->chunks.empty());
    EXPECT_EQ(result.plan->membership_version, 0U);
}

TEST(MetadataCoordinator, RejectsInvalidChunkAndGeneration) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a"});
    EXPECT_EQ(coordinator.place_chunk({"../object", "generation-1", 0}).error,
              CoordinatorError::invalid_chunk);
    EXPECT_EQ(coordinator.place_chunk({"object", "", 0}).error,
              CoordinatorError::invalid_generation);
    EXPECT_EQ(coordinator.place_chunk(
                  {"object", "generation-1", storage::kMaxChunkCount}).error,
              CoordinatorError::invalid_chunk);
}

TEST(MetadataCoordinator, RejectsInconsistentObjectMetadata) {
    MetadataCoordinator coordinator;
    add_nodes(coordinator, {"node-a"});
    auto invalid_count = manifest();
    invalid_count.chunks.pop_back();
    EXPECT_EQ(coordinator.place_object({"object", invalid_count}).error,
              CoordinatorError::invalid_object_metadata);
    auto duplicate_index = manifest();
    duplicate_index.chunks[1].index = 0;
    EXPECT_EQ(coordinator.place_object({"object", duplicate_index}).error,
              CoordinatorError::invalid_object_metadata);
    EXPECT_EQ(coordinator.place_object({"../object", manifest()}).error,
              CoordinatorError::invalid_object_metadata);
    EXPECT_EQ(coordinator.place_object({"object", manifest("")}).error,
              CoordinatorError::invalid_generation);
    const storage::ObjectManifest invalid_empty{0, 4, 1, "generation-empty", {}};
    EXPECT_EQ(coordinator.place_object({"empty", invalid_empty}).error,
              CoordinatorError::invalid_object_metadata);
}

TEST(MetadataCoordinator, ExposesRegistryErrorsWithoutThrowing) {
    MetadataCoordinator coordinator;
    EXPECT_EQ(coordinator.register_node(node("bad/id")), CoordinatorError::invalid_node);
    ASSERT_EQ(coordinator.register_node(node("node-a")), CoordinatorError::none);
    EXPECT_EQ(coordinator.register_node(node("node-a")), CoordinatorError::duplicate_node);
    EXPECT_EQ(coordinator.remove_node("missing"), CoordinatorError::unknown_node);
    EXPECT_EQ(coordinator.set_node_state("missing", NodeState::active),
              CoordinatorError::unknown_node);
}

TEST(MetadataCoordinator, RejectsPlacementPolicyOutputOutsideMembership) {
    MetadataCoordinator coordinator(std::make_unique<InvalidPolicy>());
    add_nodes(coordinator, {"node-a"});
    EXPECT_EQ(coordinator.place_chunk({"object", "generation-1", 0}).error,
              CoordinatorError::invalid_membership);
    EXPECT_EQ(coordinator.place_object({"object", manifest()}).error,
              CoordinatorError::invalid_membership);
}

}  // namespace
}  // namespace atlasfs::metadata
