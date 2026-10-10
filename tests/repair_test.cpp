#include "atlasfs/repair.hpp"
#include "atlasfs/checksum.hpp"
#include "atlasfs/coordinator_operation.hpp"
#include "atlasfs/remote_storage_node.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <thread>
#include <utility>
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
metadata::ReplicaCatalogSnapshot catalog_snapshot(
    const metadata::PlacementPlan& value, std::uint64_t version = 1) {
    metadata::ReplicaCatalogSnapshot result{version, {}};
    auto record_version = version;
    for (const auto& chunk : value.chunks) {
        std::vector<metadata::CatalogReplica> replicas;
        for (const auto& node_id : chunk.replica_node_ids) {
            replicas.push_back({node_id, metadata::CatalogReplicaState::present,
                                chunk.chunk.checksum, 10});
        }
        std::sort(replicas.begin(), replicas.end(),
            [](const auto& left, const auto& right) {
                return left.node_id < right.node_id;
            });
        result.records.push_back({chunk.identity, chunk.chunk, record_version,
                                  std::move(replicas)});
        result.catalog_version = record_version++;
    }
    return result;
}
PlanningResult make_plans(const metadata::PlacementPlan& value,
                          const metadata::MembershipSnapshot& members) {
    return RepairPlanner{}.plan(value, members, catalog_snapshot(value));
}
RepairPlan sample() {
    const auto placed = placement({"a", "b", "c"});
    auto result = RepairPlanner{}.plan(placed,
        membership({node("a"), node("b", metadata::NodeState::active,
            metadata::NodeHealthState::unavailable), node("c"), node("d")}),
        catalog_snapshot(placed));
    return result.plans.front();
}

class FakeRepairNode final : public replication::StorageNode {
public:
    explicit FakeRepairNode(std::string id) : id_(std::move(id)) {}
    std::string_view id() const noexcept override { return id_; }
    storage::StoreError prepare(std::string_view, std::string_view,
        const storage::ObjectManifest&, std::span<const std::uint32_t>,
        std::span<const std::byte>) override { return storage::StoreError::none; }
    storage::StoreError verify_prepared(std::string_view, std::string_view,
        std::string_view, std::span<const std::uint32_t>) const override {
        return storage::StoreError::none;
    }
    storage::StoreError commit(std::string_view, std::string_view,
        std::string_view) override { return storage::StoreError::none; }
    storage::StoreError abort(std::string_view, std::string_view,
        std::string_view) override { return storage::StoreError::none; }
    storage::StoreError finalize(std::string_view, std::string_view,
        std::string_view) override { return storage::StoreError::none; }
    storage::ChunkResult get_chunk(std::string_view, std::string_view,
        std::uint32_t) const override {
        return chunk_ ? *chunk_ : storage::ChunkResult{storage::StoreError::not_found, {}, {}, {}};
    }
    storage::StoreError remove(std::string_view, std::string_view) override {
        return storage::StoreError::none;
    }
    replication::OperationQueryResult query_operation(std::string_view id) const override {
        return {storage::StoreError::none, std::string(id),
                replication::OperationState::unknown, 0};
    }
    replication::RepairPublishResult publish_repair(
        std::string_view, std::string_view, std::string_view,
        std::string_view, const storage::ObjectManifest& manifest,
        const storage::ChunkMetadata& chunk, std::span<const std::byte> data) override {
        ++repairs;
        if (repair_error != storage::StoreError::none)
            return {replication::RepairPublishStatus::publication_failure, repair_error};
        chunk_ = storage::ChunkResult{storage::StoreError::none, manifest, chunk,
                                     {data.begin(), data.end()}};
        if (after_repair) after_repair();
        return {replication::RepairPublishStatus::published,
                storage::StoreError::none};
    }
    std::string id_;
    std::optional<storage::ChunkResult> chunk_;
    storage::StoreError repair_error{storage::StoreError::none};
    std::function<void()> after_repair;
    unsigned repairs{};
};

struct ExecutionFixture {
    std::vector<std::byte> data{std::byte{42}};
    std::uint32_t checksum{integrity::crc32c(data)};
    storage::ObjectManifest manifest{1, 1, checksum, "generation-1", {{0, 1, checksum}}};
    metadata::PlacementPlan placement{"object", "generation-1", 1, 1, checksum, 10, 3,
        false, {{{"object", "generation-1", 0}, {0, 1, checksum}, {"a", "b", "c"}}}};
    metadata::MembershipSnapshot members{membership({node("a"), node("b", metadata::NodeState::active,
        metadata::NodeHealthState::unavailable), node("c"), node("d")})};
};

metadata::ReplicaCatalogError seed_catalog(metadata::DurableReplicaCatalog& catalog,
    const metadata::PlacementPlan& value, std::uint64_t evidence_version = 10) {
    for (const auto& chunk : value.chunks) {
        std::vector<metadata::CatalogReplica> replicas;
        for (const auto& node_id : chunk.replica_node_ids) {
            replicas.push_back({node_id, metadata::CatalogReplicaState::present,
                                chunk.chunk.checksum, evidence_version});
        }
        std::sort(replicas.begin(), replicas.end(),
            [](const auto& left, const auto& right) {
                return left.node_id < right.node_id;
            });
        const auto error = catalog.upsert(chunk.identity, chunk.chunk,
                                          std::move(replicas));
        if (error != metadata::ReplicaCatalogError::none) return error;
    }
    return metadata::ReplicaCatalogError::none;
}

RepairPlan execution_plan(const ExecutionFixture& fixture,
                          const metadata::DurableReplicaCatalog& catalog) {
    return RepairPlanner{}.plan(fixture.placement, fixture.members,
                                catalog.snapshot()).plans.front();
}

TEST(RepairPlanner, ProducesNoPlanWhenAllReplicasAreHealthy) {
    const auto result = make_plans(placement({"a", "b", "c"}),
        membership({node("a"), node("b"), node("c"), node("d")}));
    EXPECT_EQ(result.error, PlanningError::none); EXPECT_TRUE(result.plans.empty());
}
TEST(RepairPlanner, SelectsDeterministicHealthySourceAndTarget) {
    const auto input = membership({node("d"), node("c"), node("b", metadata::NodeState::active,
        metadata::NodeHealthState::unavailable), node("a")});
    const auto first = make_plans(placement({"c", "b", "a"}), input);
    const auto second = make_plans(placement({"a", "b", "c"}), input);
    ASSERT_EQ(first.plans.size(), 1U); ASSERT_EQ(second.plans.size(), 1U);
    EXPECT_EQ(first.plans, second.plans); EXPECT_EQ(first.plans[0].source_node, "a");
    EXPECT_EQ(first.plans[0].target_node, "d");
}
TEST(RepairPlanner, CreatesOnePlanForEveryDurabilityDeficit) {
    const auto result = make_plans(placement({"a", "b", "c"}), membership({
        node("a"), node("b", metadata::NodeState::active, metadata::NodeHealthState::unavailable),
        node("c", metadata::NodeState::active, metadata::NodeHealthState::suspect), node("d"), node("e")}));
    ASSERT_EQ(result.plans.size(), 2U); EXPECT_EQ(result.plans[0].target_node, "d");
    EXPECT_EQ(result.plans[1].target_node, "e");
}
TEST(RepairPlanner, ReportsNoSourceAsUnrecoverable) {
    const auto result = make_plans(placement({"a", "b", "c"}), membership({
        node("a", metadata::NodeState::active, metadata::NodeHealthState::unavailable),
        node("b", metadata::NodeState::draining), node("c", metadata::NodeState::active,
        metadata::NodeHealthState::suspect), node("d")}));
    ASSERT_EQ(result.plans.size(), 3U); EXPECT_EQ(result.plans[0].state, RepairPlanState::unrecoverable);
    EXPECT_EQ(result.plans[0].reason, RepairReason::source_unavailable);
    EXPECT_TRUE(result.plans[0].target_node.empty());
}
TEST(RepairPlanner, ExcludesDrainingSuspectStaleAndOccupiedTargets) {
    const auto result = make_plans(placement({"a", "b"}), membership({node("a"),
        node("b", metadata::NodeState::active, metadata::NodeHealthState::unavailable),
        node("c", metadata::NodeState::draining), node("d", metadata::NodeState::active,
            metadata::NodeHealthState::suspect), node("e", metadata::NodeState::active,
            metadata::NodeHealthState::healthy, false)}));
    ASSERT_EQ(result.plans.size(), 2U); EXPECT_TRUE(result.plans[0].target_node.empty());
    EXPECT_EQ(result.plans[0].reason, RepairReason::no_eligible_target);
}
TEST(RepairPlanner, DetectsMembershipHealthPlacementAndCatalogStaleness) {
    const auto original_membership = membership({node("a"), node("b", metadata::NodeState::active,
        metadata::NodeHealthState::unavailable), node("c"), node("d")});
    const auto original_placement = placement({"a", "b", "c"});
    const auto catalog = catalog_snapshot(original_placement);
    const auto value = RepairPlanner{}.plan(
        original_placement, original_membership, catalog).plans.front();
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, original_membership,
        catalog), PlanValidation::current);
    auto changed_version = original_membership; changed_version.version++;
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, changed_version,
        catalog), PlanValidation::stale_membership);
    auto changed_health = original_membership; changed_health.nodes[0].health = metadata::NodeHealthState::suspect;
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, changed_health,
        catalog), PlanValidation::stale_health);
    auto changed_placement = original_placement; changed_placement.object_id = "other";
    EXPECT_EQ(RepairPlanner{}.validate(value, changed_placement, original_membership,
        catalog), PlanValidation::stale_placement);
    auto changed_catalog = catalog; changed_catalog.catalog_version++;
    EXPECT_EQ(RepairPlanner{}.validate(value, original_placement, original_membership,
        changed_catalog), PlanValidation::stale_catalog);
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

TEST(RepairExecutor, RequiresExplicitApprovalAndHonorsCancellation) {
    TempRoot root; ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    RepairExecutor executor(store, catalog, {source, target});
    EXPECT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::not_approved);
    EXPECT_EQ(target->repairs, 0U);
    ASSERT_EQ(executor.cancel(plan.plan_id), RepairStoreError::none);
    EXPECT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::cancelled);
}

TEST(RepairExecutor, ApprovedPlanCopiesVerifiesCommitsAndIsIdempotent) {
    TempRoot root; ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    RepairExecutor executor(store, catalog, {source, target});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    const auto result = executor.execute(plan.plan_id, fixture.placement, fixture.members);
    EXPECT_EQ(result.error, RepairExecutionError::none);
    EXPECT_EQ(result.state, RepairPlanState::completed); EXPECT_EQ(target->repairs, 1U);
    EXPECT_NE(std::find(fixture.placement.chunks[0].replica_node_ids.begin(),
                       fixture.placement.chunks[0].replica_node_ids.end(), "d"),
              fixture.placement.chunks[0].replica_node_ids.end());
    const auto durable = store.get(plan.plan_id); ASSERT_TRUE(durable);
    EXPECT_TRUE(durable->transfer_succeeded); EXPECT_TRUE(durable->verification_succeeded);
    EXPECT_TRUE(durable->publication_succeeded); EXPECT_TRUE(durable->metadata_committed);
    const auto current = catalog.get(fixture.placement.chunks[0].identity); ASSERT_TRUE(current);
    EXPECT_EQ(current->catalog_version, 2U);
    EXPECT_EQ(current->replicas.back().node_id, "d");
    EXPECT_EQ(decode_repair_plan(encode_repair_plan(*durable)), durable);
    EXPECT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::already_completed);
    EXPECT_EQ(target->repairs, 1U);
}

TEST(RepairExecutor, ExecutionNeverRewritesHistoricalOperationRecord) {
    TempRoot root; ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    metadata::CoordinatorOperationStore operations(root.path());
    const auto historical = metadata::make_coordinator_operation(
        fixture.placement, "historical-op", 77);
    ASSERT_EQ(operations.create(historical), metadata::CoordinatorRecordError::none);
    const auto before = metadata::encode_coordinator_operation(*operations.get("historical-op"));
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    RepairExecutor executor(store, catalog, {source, target});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    ASSERT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::none);
    EXPECT_EQ(metadata::encode_coordinator_operation(*operations.get("historical-op")), before);
}

TEST(RepairExecutor, RevalidationPreventsCopyWhenEvidenceChanges) {
    TempRoot root; ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    auto target = std::make_shared<FakeRepairNode>("d");
    RepairExecutor executor(store, catalog, {source, target});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    fixture.members.version++;
    EXPECT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::stale);
    EXPECT_EQ(target->repairs, 0U); EXPECT_EQ(store.get(plan.plan_id)->state, RepairPlanState::stale);
}

TEST(RepairExecutor, SourceIntegrityAndTransferFailuresDoNotCommitMetadata) {
    for (const bool corrupt_source : {true, false}) {
        TempRoot root; ExecutionFixture fixture;
        metadata::DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
        const auto plan = execution_plan(fixture, catalog);
        RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
        auto source = std::make_shared<FakeRepairNode>("a");
        auto bad_data = fixture.data; if (corrupt_source) bad_data[0] = std::byte{7};
        source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
            fixture.manifest.chunks[0], bad_data};
        auto target = std::make_shared<FakeRepairNode>("d");
        if (!corrupt_source) target->repair_error = storage::StoreError::filesystem_error;
        RepairExecutor executor(store, catalog, {source, target});
        ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
        const auto before = fixture.placement;
        const auto result = executor.execute(plan.plan_id, fixture.placement, fixture.members);
        EXPECT_EQ(result.error, corrupt_source ? RepairExecutionError::integrity_failure
                                               : RepairExecutionError::target_publication_failure);
        EXPECT_EQ(fixture.placement, before);
    }
}

TEST(RepairExecutor, MetadataCommitFailureLeavesPublishedChunkVisibleForInspection) {
    TempRoot root; ExecutionFixture fixture;
    bool fail_catalog = false;
    metadata::DurableReplicaCatalog catalog(root.path(), [&](auto point) {
        return fail_catalog &&
            point == metadata::ReplicaCatalogFaultPoint::record_write;
    });
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    RepairExecutor executor(store, catalog, {source, target});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    fail_catalog = true;
    const auto before = fixture.placement;
    EXPECT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::metadata_commit_failure);
    EXPECT_EQ(fixture.placement, before); EXPECT_TRUE(target->chunk_.has_value());
    EXPECT_EQ(store.get(plan.plan_id)->state, RepairPlanState::failed);
}

TEST(RepairExecutor, CatalogConflictAfterPublicationNeverClaimsCompletion) {
    TempRoot root; ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    const auto initial = catalog.get(fixture.placement.chunks[0].identity); ASSERT_TRUE(initial);
    auto competing_replicas = initial->replicas;
    competing_replicas.push_back({"e", metadata::CatalogReplicaState::present,
                                  fixture.checksum, fixture.members.version});
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    metadata::ReplicaCatalogCasStatus competing_status{
        metadata::ReplicaCatalogCasStatus::invalid_request};
    target->after_repair = [&] {
        competing_status = catalog.compare_exchange({initial->identity,
            initial->catalog_version, initial->replicas, competing_replicas}).status;
    };
    RepairExecutor executor(store, catalog, {source, target});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    const auto before = fixture.placement;

    const auto result = executor.execute(plan.plan_id, fixture.placement, fixture.members);
    EXPECT_EQ(competing_status, metadata::ReplicaCatalogCasStatus::committed);
    EXPECT_EQ(result.error, RepairExecutionError::conflicting_repair);
    EXPECT_EQ(result.state, RepairPlanState::stale);
    EXPECT_EQ(fixture.placement, before);
    EXPECT_TRUE(target->chunk_.has_value());
    const auto durable = store.get(plan.plan_id); ASSERT_TRUE(durable);
    EXPECT_FALSE(durable->metadata_committed);
    EXPECT_EQ(durable->last_error, RepairExecutionError::conflicting_repair);
    const auto current = catalog.get(initial->identity); ASSERT_TRUE(current);
    EXPECT_EQ(current->catalog_version, 2U);
    EXPECT_EQ(current->replicas, competing_replicas);
}

TEST(RepairExecutor, CatalogDurabilityUnknownNeverClaimsCompletion) {
    TempRoot root; ExecutionFixture fixture;
    bool fail_catalog = false;
    metadata::DurableReplicaCatalog catalog(root.path(), [&](auto point) {
        return fail_catalog &&
            point == metadata::ReplicaCatalogFaultPoint::directory_sync;
    });
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    RepairExecutor executor(store, catalog, {source, target});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    const auto before = fixture.placement;
    fail_catalog = true;

    const auto result = executor.execute(plan.plan_id, fixture.placement, fixture.members);
    EXPECT_EQ(result.error, RepairExecutionError::catalog_durability_unknown);
    EXPECT_EQ(result.state, RepairPlanState::failed);
    EXPECT_EQ(fixture.placement, before);
    EXPECT_TRUE(catalog.durability_unknown());
    const auto durable = store.get(plan.plan_id); ASSERT_TRUE(durable);
    EXPECT_FALSE(durable->metadata_committed);
    EXPECT_EQ(durable->last_error,
              RepairExecutionError::catalog_durability_unknown);
}

TEST(RepairExecutor, RestartRetainsCompletedCatalogAndPlanEvidence) {
    TempRoot root; ExecutionFixture fixture;
    std::string plan_id;
    {
        metadata::DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(seed_catalog(catalog, fixture.placement),
                  metadata::ReplicaCatalogError::none);
        const auto plan = execution_plan(fixture, catalog); plan_id = plan.plan_id;
        RepairPlanStore store(root.path()); ASSERT_EQ(store.create(plan), RepairStoreError::none);
        auto source = std::make_shared<FakeRepairNode>("a");
        source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
            fixture.manifest.chunks[0], fixture.data};
        auto target = std::make_shared<FakeRepairNode>("d");
        RepairExecutor executor(store, catalog, {source, target});
        ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
        ASSERT_EQ(executor.execute(plan.plan_id, fixture.placement, fixture.members).error,
                  RepairExecutionError::none);
    }

    metadata::DurableReplicaCatalog restarted_catalog(root.path());
    RepairPlanStore restarted_plans(root.path());
    const auto record = restarted_catalog.get(fixture.placement.chunks[0].identity);
    const auto plan = restarted_plans.get(plan_id);
    ASSERT_TRUE(record); ASSERT_TRUE(plan);
    EXPECT_EQ(record->catalog_version, 2U);
    EXPECT_EQ(record->replicas.back().node_id, "d");
    EXPECT_EQ(plan->catalog_version, 1U);
    EXPECT_EQ(plan->state, RepairPlanState::completed);
    EXPECT_TRUE(plan->metadata_committed);
    EXPECT_EQ(plan->observed_catalog_replicas.size(), 3U);
}

TEST(RepairExecutor, ReconcilesCrashAfterExecutingPlanWasPersisted) {
    TempRoot root; ExecutionFixture fixture; std::string plan_id;
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    {
        metadata::DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
        const auto plan = execution_plan(fixture, catalog); plan_id = plan.plan_id;
        RepairPlanStore plans(root.path()); ASSERT_EQ(plans.create(plan), RepairStoreError::none);
        bool interrupted = false;
        RepairExecutor executor(plans, catalog, {source, target}, [&](auto point) {
            return point == RepairExecutionFaultPoint::after_executing_persist &&
                !std::exchange(interrupted, true);
        });
        ASSERT_EQ(executor.approve(plan_id), RepairStoreError::none);
        EXPECT_EQ(executor.execute(plan_id, fixture.placement, fixture.members).error,
                  RepairExecutionError::recovery_interrupted);
        EXPECT_EQ(plans.get(plan_id)->state, RepairPlanState::executing);
        EXPECT_EQ(target->repairs, 0U);
    }
    metadata::DurableReplicaCatalog catalog(root.path());
    RepairPlanStore plans(root.path());
    RepairExecutor executor(plans, catalog, {source, target});
    EXPECT_EQ(executor.reconcile(plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::none);
    EXPECT_EQ(plans.get(plan_id)->state, RepairPlanState::completed);
    EXPECT_EQ(target->repairs, 1U);
}

TEST(RepairExecutor, ReconcilesCrashAfterPublicationWithoutRepublishing) {
    TempRoot root; ExecutionFixture fixture; std::string plan_id;
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    {
        metadata::DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
        const auto plan = execution_plan(fixture, catalog); plan_id = plan.plan_id;
        RepairPlanStore plans(root.path()); ASSERT_EQ(plans.create(plan), RepairStoreError::none);
        RepairExecutor executor(plans, catalog, {source, target}, [](auto point) {
            return point == RepairExecutionFaultPoint::after_publication_persist;
        });
        ASSERT_EQ(executor.approve(plan_id), RepairStoreError::none);
        EXPECT_EQ(executor.execute(plan_id, fixture.placement, fixture.members).error,
                  RepairExecutionError::recovery_interrupted);
        const auto durable = plans.get(plan_id); ASSERT_TRUE(durable);
        EXPECT_TRUE(durable->publication_succeeded);
        EXPECT_TRUE(durable->verification_succeeded);
        EXPECT_FALSE(durable->metadata_committed);
        EXPECT_EQ(target->repairs, 1U);
    }
    metadata::DurableReplicaCatalog catalog(root.path());
    RepairPlanStore plans(root.path());
    RepairExecutor executor(plans, catalog, {source, target});
    EXPECT_EQ(executor.reconcile(plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::none);
    EXPECT_EQ(target->repairs, 1U);
    EXPECT_EQ(catalog.version(), 2U);
}

TEST(RepairExecutor, ReconcilesCrashAfterCatalogCommitWithoutRecopying) {
    TempRoot root; ExecutionFixture fixture; std::string plan_id;
    const auto original_placement = fixture.placement;
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
        fixture.manifest.chunks[0], fixture.data};
    auto target = std::make_shared<FakeRepairNode>("d");
    {
        metadata::DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
        const auto plan = execution_plan(fixture, catalog); plan_id = plan.plan_id;
        RepairPlanStore plans(root.path()); ASSERT_EQ(plans.create(plan), RepairStoreError::none);
        RepairExecutor executor(plans, catalog, {source, target}, [](auto point) {
            return point == RepairExecutionFaultPoint::after_catalog_commit;
        });
        ASSERT_EQ(executor.approve(plan_id), RepairStoreError::none);
        EXPECT_EQ(executor.execute(plan_id, fixture.placement, fixture.members).error,
                  RepairExecutionError::recovery_interrupted);
        EXPECT_EQ(catalog.version(), 2U);
        EXPECT_EQ(fixture.placement, original_placement);
        EXPECT_EQ(plans.get(plan_id)->state, RepairPlanState::executing);
    }
    metadata::DurableReplicaCatalog catalog(root.path());
    RepairPlanStore plans(root.path());
    RepairExecutor executor(plans, catalog, {source, target});
    EXPECT_EQ(executor.reconcile(plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::none);
    EXPECT_EQ(plans.get(plan_id)->state, RepairPlanState::completed);
    EXPECT_EQ(target->repairs, 1U);
    EXPECT_EQ(fixture.placement.chunks[0].replica_node_ids,
              (std::vector<std::string>{"a", "c", "d"}));
}

TEST(RepairExecutor, ReconciliationRejectsConcurrentCatalogAndMembershipChanges) {
    for (const bool change_membership : {false, true}) {
        TempRoot root; ExecutionFixture fixture;
        metadata::DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
        const auto plan = execution_plan(fixture, catalog);
        RepairPlanStore plans(root.path()); ASSERT_EQ(plans.create(plan), RepairStoreError::none);
        auto source = std::make_shared<FakeRepairNode>("a");
        source->chunk_ = storage::ChunkResult{storage::StoreError::none, fixture.manifest,
            fixture.manifest.chunks[0], fixture.data};
        auto target = std::make_shared<FakeRepairNode>("d");
        RepairExecutor interrupted(plans, catalog, {source, target}, [](auto point) {
            return point == RepairExecutionFaultPoint::after_publication_persist;
        });
        ASSERT_EQ(interrupted.approve(plan.plan_id), RepairStoreError::none);
        ASSERT_EQ(interrupted.execute(plan.plan_id, fixture.placement, fixture.members).error,
                  RepairExecutionError::recovery_interrupted);
        const auto repairs = target->repairs;
        if (change_membership) {
            ++fixture.members.version;
        } else {
            const auto current = catalog.get(fixture.placement.chunks[0].identity);
            ASSERT_TRUE(current);
            auto competing = current->replicas;
            competing.push_back({"e", metadata::CatalogReplicaState::present,
                                 fixture.checksum, fixture.members.version});
            ASSERT_EQ(catalog.compare_exchange({current->identity,
                current->catalog_version, current->replicas, competing}).status,
                metadata::ReplicaCatalogCasStatus::committed);
        }
        RepairExecutor recovering(plans, catalog, {source, target});
        EXPECT_EQ(recovering.reconcile(plan.plan_id, fixture.placement, fixture.members).error,
                  RepairExecutionError::stale);
        EXPECT_EQ(plans.get(plan.plan_id)->state, RepairPlanState::stale);
        EXPECT_EQ(target->repairs, repairs);
    }
}

TEST(RepairExecutor, ReconciliationRejectsPlansWithoutAmbiguousDurableState) {
    TempRoot root; ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement), metadata::ReplicaCatalogError::none);
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore plans(root.path()); ASSERT_EQ(plans.create(plan), RepairStoreError::none);
    RepairExecutor executor(plans, catalog, {});
    EXPECT_EQ(executor.reconcile(plan.plan_id, fixture.placement, fixture.members).error,
              RepairExecutionError::not_reconcilable);
    EXPECT_EQ(plans.get(plan.plan_id)->state, RepairPlanState::planned);
}

TEST(RepairExecutor, RemotePublicationThenCatalogCasCompletesFourNodeRepair) {
    TempRoot root;
    ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement),
              metadata::ReplicaCatalogError::none);
    metadata::CoordinatorOperationStore operations(root.path());
    const auto historical = metadata::make_coordinator_operation(
        fixture.placement, "historical-remote", 77);
    ASSERT_EQ(operations.create(historical), metadata::CoordinatorRecordError::none);
    const auto history_before = metadata::encode_coordinator_operation(
        *operations.get("historical-remote"));

    auto source = std::make_shared<replication::LocalStorageNode>(
        "a", root.path() / "source", 1024, 1);
    ASSERT_EQ(source->publish_repair("seed-plan", "seed-execution", "a", "object",
        fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    auto replica_b = std::make_shared<replication::LocalStorageNode>(
        "b", root.path() / "replica-b", 1024, 1);
    auto replica_c = std::make_shared<replication::LocalStorageNode>(
        "c", root.path() / "replica-c", 1024, 1);
    ASSERT_EQ(replica_b->publish_repair("seed-plan", "seed-execution-b", "b", "object",
        fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    ASSERT_EQ(replica_c->publish_repair("seed-plan", "seed-execution-c", "c", "object",
        fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    auto target_local = std::make_shared<replication::LocalStorageNode>(
        "d", root.path() / "target", 1024, 1);
    replication::StorageNodeServer server(
        target_local, root.path() / "target-journal");
    auto target_remote = std::make_shared<replication::RemoteStorageNodeClient>(
        "d", server.port());

    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore plans(root.path());
    ASSERT_EQ(plans.create(plan), RepairStoreError::none);
    RepairExecutor executor(plans, catalog, {source, target_remote});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    std::thread server_thread([&] { server.serve(); });
    const auto result = executor.execute(
        plan.plan_id, fixture.placement, fixture.members);
    server.stop();
    server_thread.join();

    EXPECT_EQ(result.error, RepairExecutionError::none);
    EXPECT_EQ(result.state, RepairPlanState::completed);
    EXPECT_EQ(target_local->get_chunk("object", "generation-1", 0).data,
              fixture.data);
    const auto current = catalog.get(fixture.placement.chunks[0].identity);
    ASSERT_TRUE(current);
    std::vector<std::string> current_nodes;
    for (const auto& replica : current->replicas)
        current_nodes.push_back(replica.node_id);
    EXPECT_EQ(current_nodes, (std::vector<std::string>{"a", "c", "d"}));
    EXPECT_EQ(metadata::encode_coordinator_operation(
        *operations.get("historical-remote")), history_before);

}

TEST(RepairExecutor, LostRemoteResponseLeavesPhysicalReplicaNonAuthoritative) {
    TempRoot root;
    ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement),
              metadata::ReplicaCatalogError::none);
    auto source = std::make_shared<replication::LocalStorageNode>(
        "a", root.path() / "source", 1024, 1);
    ASSERT_EQ(source->publish_repair("seed-plan", "seed-execution", "a", "object",
        fixture.manifest, fixture.manifest.chunks[0], fixture.data).status,
        replication::RepairPublishStatus::published);
    auto target_local = std::make_shared<replication::LocalStorageNode>(
        "d", root.path() / "target", 1024, 1);
    replication::StorageNodeServer server(
        target_local, root.path() / "target-journal", 0, false, true);
    auto target_remote = std::make_shared<replication::RemoteStorageNodeClient>(
        "d", server.port());
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore plans(root.path());
    ASSERT_EQ(plans.create(plan), RepairStoreError::none);
    RepairExecutor executor(plans, catalog, {source, target_remote});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    std::thread server_thread([&] { try { server.serve(); } catch (...) {} });

    const auto before = catalog.snapshot();
    const auto result = executor.execute(
        plan.plan_id, fixture.placement, fixture.members);
    server_thread.join();

    EXPECT_EQ(result.error, RepairExecutionError::remote_result_unknown);
    EXPECT_EQ(result.state, RepairPlanState::failed);
    EXPECT_EQ(catalog.snapshot(), before);
    EXPECT_EQ(target_local->get_chunk("object", "generation-1", 0).data,
              fixture.data);
    const auto durable = plans.get(plan.plan_id);
    ASSERT_TRUE(durable);
    EXPECT_FALSE(durable->transfer_succeeded);
    EXPECT_FALSE(durable->publication_succeeded);
    EXPECT_FALSE(durable->metadata_committed);
    RepairPlanStore restarted_plans(root.path());
    EXPECT_EQ(restarted_plans.get(plan.plan_id)->last_error,
              RepairExecutionError::remote_result_unknown);

    metadata::DurableReplicaCatalog restarted_catalog(root.path());
    replication::StorageNodeServer restarted_server(
        target_local, root.path() / "target-journal");
    auto restarted_remote = std::make_shared<replication::RemoteStorageNodeClient>(
        "d", restarted_server.port());
    RepairExecutor restarted_executor(
        restarted_plans, restarted_catalog, {source, restarted_remote});
    std::thread restarted_thread([&] { restarted_server.serve(); });
    const auto reconciled = restarted_executor.reconcile(
        plan.plan_id, fixture.placement, fixture.members);
    restarted_server.stop();
    restarted_thread.join();

    EXPECT_EQ(reconciled.error, RepairExecutionError::none);
    EXPECT_EQ(reconciled.state, RepairPlanState::completed);
    EXPECT_EQ(restarted_catalog.version(), 2U);
    EXPECT_TRUE(restarted_plans.get(plan.plan_id)->metadata_committed);
}

TEST(RepairExecutor, RemoteReplicaRemainsPhysicalWhenCatalogCasConflicts) {
    TempRoot root;
    ExecutionFixture fixture;
    metadata::DurableReplicaCatalog catalog(root.path());
    ASSERT_EQ(seed_catalog(catalog, fixture.placement),
              metadata::ReplicaCatalogError::none);
    const auto initial = catalog.get(fixture.placement.chunks[0].identity);
    ASSERT_TRUE(initial);
    auto competing = initial->replicas;
    competing.push_back({"e", metadata::CatalogReplicaState::present,
                         fixture.checksum, fixture.members.version});
    auto source = std::make_shared<FakeRepairNode>("a");
    source->chunk_ = storage::ChunkResult{storage::StoreError::none,
        fixture.manifest, fixture.manifest.chunks[0], fixture.data};
    auto target_local = std::make_shared<FakeRepairNode>("d");
    auto competing_status = metadata::ReplicaCatalogCasStatus::invalid_request;
    target_local->after_repair = [&] {
        competing_status = catalog.compare_exchange({initial->identity,
            initial->catalog_version, initial->replicas, competing}).status;
    };
    replication::StorageNodeServer server(
        target_local, root.path() / "target-journal");
    auto target_remote = std::make_shared<replication::RemoteStorageNodeClient>(
        "d", server.port());
    const auto plan = execution_plan(fixture, catalog);
    RepairPlanStore plans(root.path());
    ASSERT_EQ(plans.create(plan), RepairStoreError::none);
    RepairExecutor executor(plans, catalog, {source, target_remote});
    ASSERT_EQ(executor.approve(plan.plan_id), RepairStoreError::none);
    std::thread server_thread([&] { server.serve(); });

    const auto result = executor.execute(
        plan.plan_id, fixture.placement, fixture.members);
    server.stop();
    server_thread.join();

    EXPECT_EQ(result.error, RepairExecutionError::conflicting_repair);
    EXPECT_EQ(result.state, RepairPlanState::stale);
    EXPECT_EQ(competing_status, metadata::ReplicaCatalogCasStatus::committed);
    EXPECT_TRUE(target_local->chunk_.has_value());
    EXPECT_EQ(catalog.get(initial->identity)->replicas, competing);
    const auto durable = plans.get(plan.plan_id);
    ASSERT_TRUE(durable);
    EXPECT_TRUE(durable->publication_succeeded);
    EXPECT_FALSE(durable->metadata_committed);
}
}  // namespace
}  // namespace atlasfs::repair
