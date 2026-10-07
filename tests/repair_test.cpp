#include "atlasfs/repair.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <unistd.h>

namespace atlasfs::repair {
namespace {
class TempRoot {
public:
    TempRoot() { static std::atomic_uint64_t sequence{0}; path_ = std::filesystem::temp_directory_path() /
        ("atlasfs-repair-test-" + std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
         std::to_string(sequence.fetch_add(1))); std::filesystem::create_directories(path_); }
    ~TempRoot() { std::error_code error; std::filesystem::remove_all(path_, error); }
    const auto& path() const { return path_; }
private: std::filesystem::path path_;
};
metadata::StorageNode node(std::string id, metadata::NodeState state = metadata::NodeState::active,
    metadata::NodeHealthState health = metadata::NodeHealthState::healthy, bool current = true) {
    return {std::move(id), {"127.0.0.1", 7000}, state, health, 0, current};
}
metadata::MembershipSnapshot membership(std::vector<metadata::StorageNode> nodes, std::uint64_t version = 10) {
    std::sort(nodes.begin(), nodes.end(), [](const auto& a, const auto& b) { return a.node_id < b.node_id; });
    return {version, std::move(nodes)};
}
metadata::PlacementPlan placement(std::vector<std::string> replicas, std::uint32_t factor = 3) {
    return {"object", "generation-1", 1, 1, 0, 10, factor, false,
        {{{"object", "generation-1", 0}, {0, 1, 0}, std::move(replicas)}}};
}
RepairPlan sample() {
    auto result = RepairPlanner{}.plan(placement({"a", "b", "c"}),
        membership({node("a"), node("b", metadata::NodeState::active,
            metadata::NodeHealthState::unavailable), node("c"), node("d")}));
    return result.plans.front();
}

TEST(RepairPlanner, ProducesNoPlanWhenAllReplicasAreHealthy) {
    const auto result = RepairPlanner{}.plan(placement({"a", "b", "c"}),
        membership({node("a"), node("b"), node("c"), node("d")}));
    EXPECT_EQ(result.error, PlanningError::none); EXPECT_TRUE(result.plans.empty());
}
TEST(RepairPlanner, SelectsDeterministicHealthySourceAndTarget) {
    const auto input = membership({node("d"), node("c"), node("b", metadata::NodeState::active,
        metadata::NodeHealthState::unavailable), node("a")});
    const auto first = RepairPlanner{}.plan(placement({"c", "b", "a"}), input);
    const auto second = RepairPlanner{}.plan(placement({"a", "b", "c"}), input);
    ASSERT_EQ(first.plans.size(), 1U); ASSERT_EQ(second.plans.size(), 1U);
    EXPECT_EQ(first.plans, second.plans); EXPECT_EQ(first.plans[0].source_node, "a");
    EXPECT_EQ(first.plans[0].target_node, "d");
}
TEST(RepairPlanner, CreatesOnePlanForEveryDurabilityDeficit) {
    const auto result = RepairPlanner{}.plan(placement({"a", "b", "c"}), membership({
        node("a"), node("b", metadata::NodeState::active, metadata::NodeHealthState::unavailable),
        node("c", metadata::NodeState::active, metadata::NodeHealthState::suspect), node("d"), node("e")}));
    ASSERT_EQ(result.plans.size(), 2U); EXPECT_EQ(result.plans[0].target_node, "d");
    EXPECT_EQ(result.plans[1].target_node, "e");
}
TEST(RepairPlanner, ReportsNoSourceAsUnrecoverable) {
    const auto result = RepairPlanner{}.plan(placement({"a", "b", "c"}), membership({
        node("a", metadata::NodeState::active, metadata::NodeHealthState::unavailable),
        node("b", metadata::NodeState::draining), node("c", metadata::NodeState::active,
        metadata::NodeHealthState::suspect), node("d")}));
    ASSERT_EQ(result.plans.size(), 3U); EXPECT_EQ(result.plans[0].state, RepairPlanState::unrecoverable);
    EXPECT_EQ(result.plans[0].reason, RepairReason::source_unavailable);
    EXPECT_TRUE(result.plans[0].target_node.empty());
}
TEST(RepairPlanner, ExcludesDrainingSuspectStaleAndOccupiedTargets) {
    const auto result = RepairPlanner{}.plan(placement({"a", "b"}), membership({node("a"),
        node("b", metadata::NodeState::active, metadata::NodeHealthState::unavailable),
        node("c", metadata::NodeState::draining), node("d", metadata::NodeState::active,
            metadata::NodeHealthState::suspect), node("e", metadata::NodeState::active,
            metadata::NodeHealthState::healthy, false)}));
    ASSERT_EQ(result.plans.size(), 2U); EXPECT_TRUE(result.plans[0].target_node.empty());
    EXPECT_EQ(result.plans[0].reason, RepairReason::no_eligible_target);
}
TEST(RepairPlanner, DetectsMembershipHealthAndPlacementStaleness) {
    const auto original_membership = membership({node("a"), node("b", metadata::NodeState::active,
        metadata::NodeHealthState::unavailable), node("c"), node("d")});
    const auto original_placement = placement({"a", "b", "c"});
    const auto value = RepairPlanner{}.plan(original_placement, original_membership).plans.front();
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, original_membership), PlanValidation::current);
    auto changed_version = original_membership; changed_version.version++;
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, changed_version), PlanValidation::stale_membership);
    auto changed_health = original_membership; changed_health.nodes[0].health = metadata::NodeHealthState::suspect;
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, changed_health), PlanValidation::stale_health);
    auto changed_placement = original_placement; changed_placement.chunks[0].replica_node_ids = {"a", "c", "d"};
    EXPECT_EQ(RepairPlanner{}.validate(value, changed_placement, original_membership), PlanValidation::stale_placement);
}
TEST(RepairPlanSerialization, RoundTripsAndRejectsCorruption) {
    const auto value = sample(); const auto bytes = encode_repair_plan(value);
    EXPECT_EQ(decode_repair_plan(bytes), value);
    auto truncated = bytes; truncated.pop_back(); EXPECT_FALSE(decode_repair_plan(truncated));
    auto trailing = bytes; trailing.push_back(std::byte{0}); EXPECT_FALSE(decode_repair_plan(trailing));
    auto corrupt = bytes; corrupt[0] ^= std::byte{1}; EXPECT_FALSE(decode_repair_plan(corrupt));
}
TEST(RepairPlanStore, PersistsReloadsAndOrdersPlans) {
    TempRoot root; auto one = sample(); auto two = one; two.plan_id = "repair-ffffffff";
    { RepairPlanStore store(root.path()); ASSERT_EQ(store.create(two), RepairStoreError::none);
      ASSERT_EQ(store.create(one), RepairStoreError::none); EXPECT_EQ(store.create(one), RepairStoreError::none); }
    RepairPlanStore restarted(root.path()); ASSERT_EQ(restarted.list().size(), 2U);
    EXPECT_LT(restarted.list()[0].plan_id, restarted.list()[1].plan_id);
}
TEST(RepairPlanStore, PublicationFailuresNeverEnterLiveIndex) {
    for (const auto point : {RepairFaultPoint::record_creation, RepairFaultPoint::record_write,
         RepairFaultPoint::record_sync, RepairFaultPoint::record_rename, RepairFaultPoint::directory_sync}) {
        TempRoot root; RepairPlanStore store(root.path(), [point](auto current) { return point == current; });
        const auto value = sample(); EXPECT_EQ(store.create(value), RepairStoreError::filesystem_error);
        EXPECT_FALSE(store.get(value.plan_id));
    }
}
TEST(RepairPlanStore, CorruptPublishedPlanStopsStartupWithoutDeletion) {
    TempRoot root; std::filesystem::path path; auto value = sample();
    { RepairPlanStore store(root.path()); ASSERT_EQ(store.create(value), RepairStoreError::none);
      path = store.plans_directory() / value.plan_id; }
    std::ofstream(path, std::ios::binary | std::ios::trunc).put('x');
    EXPECT_THROW(RepairPlanStore(root.path()), std::runtime_error); EXPECT_TRUE(std::filesystem::exists(path));
}
}  // namespace
}  // namespace atlasfs::repair
