#include "atlasfs/node_registry.hpp"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace atlasfs::metadata {
namespace {

StorageNode node(std::string id, NodeState state = NodeState::active) {
    return {std::move(id), {"127.0.0.1", 7000}, state};
}

TEST(NodeRegistry, AddsAndLooksUpValidNode) {
    NodeRegistry registry;
    const auto input = node("node-a");
    EXPECT_EQ(registry.add(input), RegistryError::none);
    EXPECT_EQ(registry.get("node-a"), input);
}

TEST(NodeRegistry, RejectsDuplicateIdentity) {
    NodeRegistry registry;
    ASSERT_EQ(registry.add(node("node-a")), RegistryError::none);
    EXPECT_EQ(registry.add({"node-a", {"other.example", 8000}, NodeState::active}),
              RegistryError::duplicate_node);
}

TEST(NodeRegistry, RejectsInvalidNodeIdsEndpointsAndStates) {
    NodeRegistry registry;
    for (const std::string& id :
         std::vector<std::string>{"", "node/a", "node a", std::string(65, 'x')}) {
        EXPECT_EQ(registry.add(node(id)), RegistryError::invalid_node) << id;
    }
    EXPECT_EQ(registry.add({"valid", {"", 1}, NodeState::active}),
              RegistryError::invalid_node);
    EXPECT_EQ(registry.add({"valid", {"host", 0}, NodeState::active}),
              RegistryError::invalid_node);
    EXPECT_EQ(registry.add({"valid", {"bad host", 1}, NodeState::active}),
              RegistryError::invalid_node);
    EXPECT_EQ(registry.add({"valid", {":", 1}, NodeState::active}),
              RegistryError::invalid_node);
    EXPECT_EQ(registry.add({"valid", {"host", 1}, static_cast<NodeState>(99)}),
              RegistryError::invalid_node);
}

TEST(NodeRegistry, RemovesNodesAndRejectsUnknownRemoval) {
    NodeRegistry registry;
    ASSERT_EQ(registry.add(node("node-a")), RegistryError::none);
    EXPECT_EQ(registry.remove("node-a"), RegistryError::none);
    EXPECT_FALSE(registry.get("node-a"));
    EXPECT_EQ(registry.remove("node-a"), RegistryError::unknown_node);
}

TEST(NodeRegistry, UpdatesStateAndRejectsUnknownOrInvalidState) {
    NodeRegistry registry;
    ASSERT_EQ(registry.add(node("node-a")), RegistryError::none);
    EXPECT_EQ(registry.set_state("node-a", NodeState::draining), RegistryError::none);
    ASSERT_TRUE(registry.get("node-a"));
    EXPECT_EQ(registry.get("node-a")->state, NodeState::draining);
    EXPECT_EQ(registry.set_state("missing", NodeState::active), RegistryError::unknown_node);
    EXPECT_EQ(registry.set_state("node-a", static_cast<NodeState>(99)),
              RegistryError::invalid_node);
}

TEST(NodeRegistry, ListsInNodeIdOrderRegardlessOfInsertionOrder) {
    NodeRegistry registry;
    ASSERT_EQ(registry.add(node("node-c")), RegistryError::none);
    ASSERT_EQ(registry.add(node("node-a")), RegistryError::none);
    ASSERT_EQ(registry.add(node("node-b")), RegistryError::none);
    const auto nodes = registry.list();
    ASSERT_EQ(nodes.size(), 3U);
    EXPECT_EQ(nodes[0].node_id, "node-a");
    EXPECT_EQ(nodes[1].node_id, "node-b");
    EXPECT_EQ(nodes[2].node_id, "node-c");
}

TEST(NodeRegistry, PlacementEligibilityIncludesOnlyActiveNodes) {
    NodeRegistry registry;
    ASSERT_EQ(registry.add(node("active", NodeState::active)), RegistryError::none);
    ASSERT_EQ(registry.add(node("draining", NodeState::draining)), RegistryError::none);
    ASSERT_EQ(registry.add(node("unavailable", NodeState::unavailable)), RegistryError::none);
    const auto eligible = registry.eligible_nodes();
    ASSERT_EQ(eligible.size(), 1U);
    EXPECT_EQ(eligible.front().node_id, "active");
}

TEST(NodeRegistry, VersionAdvancesOnlyForSuccessfulMembershipChanges) {
    NodeRegistry registry;
    EXPECT_EQ(registry.version(), 0U);
    ASSERT_EQ(registry.add(node("node-a")), RegistryError::none);
    EXPECT_EQ(registry.version(), 1U);
    EXPECT_EQ(registry.add(node("node-a")), RegistryError::duplicate_node);
    EXPECT_EQ(registry.version(), 1U);
    EXPECT_EQ(registry.set_state("node-a", NodeState::active), RegistryError::none);
    EXPECT_EQ(registry.version(), 1U);
    EXPECT_EQ(registry.set_state("node-a", NodeState::draining), RegistryError::none);
    EXPECT_EQ(registry.version(), 2U);
    EXPECT_EQ(registry.remove("node-a"), RegistryError::none);
    EXPECT_EQ(registry.version(), 3U);
}

}  // namespace
}  // namespace atlasfs::metadata
