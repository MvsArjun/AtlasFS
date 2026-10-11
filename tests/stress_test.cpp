#include "../benchmarks/scenario.hpp"
#include <gtest/gtest.h>
#include <fstream>
#include <future>
#include <map>

namespace atlasfs::bench {
namespace {
std::string contents(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
std::size_t temporary_files(const std::filesystem::path& root) {
    std::size_t count{};
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        const auto name = entry.path().filename().string();
        count += entry.is_regular_file() && (name.ends_with(".tmp") || name.starts_with(".atlasfs-tmp-"));
    }
    return count;
}
TEST(StressStorage, SeededOverwriteReadVerifyCyclesAndRestart) {
    test::Certificates temporary;
    std::map<std::string, std::vector<std::byte>> expected;
    {
        storage::ObjectStore store(temporary.root() / "data", 16384, 257);
        for (std::size_t n = 0; n < 128; ++n) {
            SCOPED_TRACE("seed=42 iteration=" + std::to_string(n));
            const auto id = "object-" + std::to_string(n % 16);
            auto data = payload(1 + n * 97 % 8192, 42 + n);
            ASSERT_EQ(store.put(id, data, integrity::crc32c(data)), storage::StoreError::none);
            expected[id] = data;
            const auto found = store.get(id);
            ASSERT_EQ(found.error, storage::StoreError::none);
            EXPECT_EQ(found.data, data); EXPECT_EQ(found.checksum, integrity::crc32c(data));
            EXPECT_EQ(temporary_files(temporary.root()), 0U);
        }
    }
    storage::ObjectStore reopened(temporary.root() / "data", 16384, 257);
    for (const auto& [id, data] : expected) EXPECT_EQ(reopened.get(id).data, data);
    EXPECT_TRUE(std::filesystem::is_empty(reopened.transactions_directory()));
}
TEST(StressReplication, ManyObjectsSurviveRepeatedNodeRestartAndUnavailability) {
    Cluster cluster(3, 257);
    std::vector<metadata::PlacementPlan> plans;
    const auto data = payload(2048, 73);
    {
        replication::ReplicaManager writer(cluster.clients());
        for (std::size_t n = 0; n < 12; ++n) {
            auto plan = placement("object-" + std::to_string(n), manifest(data, 257, "gen-" + std::to_string(n)), {"node-0", "node-1"});
            ASSERT_EQ(writer.put(plan, data).status, replication::ReplicationStatus::success);
            plans.push_back(std::move(plan));
        }
    }
    for (unsigned cycle = 0; cycle < 3; ++cycle) {
        SCOPED_TRACE(cycle); cluster.stop(0);
        { replication::ReplicaManager degraded({cluster.nodes[1].client});
          for (const auto& plan : plans) EXPECT_EQ(degraded.get(plan).data, data); }
        cluster.nodes[0].local = std::make_shared<replication::LocalStorageNode>("node-0", cluster.nodes[0].data_root, 16384, 257);
        cluster.start(0);
        { replication::ReplicaManager recovered(cluster.clients());
          for (const auto& plan : plans) {
              const auto result = recovered.get(plan);
              EXPECT_EQ(result.status, replication::ReplicationStatus::success); EXPECT_EQ(result.data, data);
              EXPECT_EQ(result.checksum, integrity::crc32c(data));
          } }
    }
}
TEST(StressRepair, RepeatedLostResponsesRestartAndByteEquivalentReplay) {
    Cluster cluster(1, 4096);
    const auto data = payload(1024, 99);
    for (unsigned n = 0; n < 8; ++n) {
        SCOPED_TRACE("epoch=benchmark/1 execution=execution-" + std::to_string(n));
        cluster.restart(0, 4096, true);
        const auto value = manifest(data, 4096, "gen-" + std::to_string(n));
        const auto id = "execution-" + std::to_string(n), object = "object-" + std::to_string(n);
        ASSERT_EQ(cluster.nodes[0].client->publish_repair("plan", id, "node-0", object, value, value.chunks[0], data).status,
                  replication::RepairPublishStatus::result_unknown);
        const auto record = contents(cluster.nodes[0].journal / "repairs" / id);
        ASSERT_FALSE(record.empty()); cluster.restart(0, 4096);
        for (unsigned replay = 0; replay < 3; ++replay) {
            const auto result = cluster.nodes[0].client->publish_repair("plan", id, "node-0", object, value, value.chunks[0], data);
            EXPECT_TRUE(result.status == replication::RepairPublishStatus::published || result.status == replication::RepairPublishStatus::already_present_valid);
            EXPECT_EQ(contents(cluster.nodes[0].journal / "repairs" / id), record);
            EXPECT_EQ(cluster.nodes[0].client->get_chunk(object, value.generation_id, 0).data, data);
        }
    }
}
TEST(StressPublication, RepeatedFailedWritesLeaveNoTemporaryFilesOrVisiblePartialChunks) {
    const auto data = payload(1024, 12);
    for (auto point : {storage::PutFaultPoint::manifest_write, storage::PutFaultPoint::manifest_sync,
                       storage::PutFaultPoint::chunk_write, storage::PutFaultPoint::chunk_sync,
                       storage::PutFaultPoint::generation_rename, storage::PutFaultPoint::commit_rename}) {
        test::Certificates temporary;
        storage::ObjectStore store(temporary.root() / "data", 16384, 4096, [=](auto at) { return at == point; });
        for (unsigned n = 0; n < 8; ++n) {
            SCOPED_TRACE("point=" + std::to_string(static_cast<int>(point)) + " iteration=" + std::to_string(n));
            const auto value = manifest(data, 4096, "gen-" + std::to_string(n));
            EXPECT_NE(store.publish_repair_chunk("object-" + std::to_string(n), value, value.chunks[0], data), storage::StoreError::none);
            EXPECT_EQ(store.get_replica_chunk("object-" + std::to_string(n), value.generation_id, 0).error, storage::StoreError::not_found);
            EXPECT_EQ(temporary_files(temporary.root()), 0U);
        }
    }
}
class StressCrash : public ::testing::TestWithParam<repair::RepairExecutionFaultPoint> {};
TEST_P(StressCrash, RepeatedRecoveryCrashesPreserveIdentityAuthorityAndHistory) {
    Cluster cluster(1, 4096);
    auto source = std::make_shared<replication::LocalStorageNode>("a", cluster.root() / "source", 16384, 4096);
    const auto data = payload(1024, 19);
    auto catalog = std::make_unique<metadata::DurableReplicaCatalog>(cluster.root(), cluster.authority, cluster.token);
    auto plans = std::make_unique<repair::RepairPlanStore>(cluster.root());
    const metadata::MembershipSnapshot members{10, {member("a"), member("b", false), member("node-0")}};
    metadata::CoordinatorOperationStore history(cluster.root());
    for (unsigned n = 0; n < 4; ++n) {
        SCOPED_TRACE("seed=19 window=" + std::to_string(static_cast<int>(GetParam())) + " object=" + std::to_string(n));
        auto plan = placement("object-" + std::to_string(n), manifest(data, 4096, "gen-" + std::to_string(n)), {"a", "b"});
        auto seed = plan; seed.replication_factor = 1; seed.chunks[0].replica_node_ids = {"a"};
        replication::ReplicaManager writer({source}); ASSERT_EQ(writer.put(seed, data).status, replication::ReplicationStatus::success);
        seed_catalog(*catalog, plan);
        const auto history_id = "history-" + std::to_string(n);
        ASSERT_EQ(history.create(metadata::make_coordinator_operation(plan, history_id, n)), metadata::CoordinatorRecordError::none);
        const auto historical = metadata::encode_coordinator_operation(*history.get(history_id));
        const auto planned = repair::RepairPlanner{}.plan(plan, members, catalog->snapshot()); ASSERT_EQ(planned.plans.size(), 1U);
        const auto id = planned.plans[0].plan_id; ASSERT_EQ(plans->create(planned.plans[0]), repair::RepairStoreError::none);
        {
            repair::RepairExecutor interrupted(*plans, *catalog, {source, cluster.nodes[0].client}, [&](auto at) { return at == GetParam(); });
            ASSERT_EQ(interrupted.approve(id), repair::RepairStoreError::none);
            ASSERT_EQ(interrupted.execute(id, plan, members).error, repair::RepairExecutionError::recovery_interrupted);
        }
        const auto execution = plans->get(id)->execution_id;
        const auto version = catalog->version();
        for (unsigned crash = 0; crash < 3; ++crash) {
            plans = std::make_unique<repair::RepairPlanStore>(cluster.root());
            catalog = std::make_unique<metadata::DurableReplicaCatalog>(cluster.root(), cluster.authority, cluster.token);
            repair::RepairExecutor again(*plans, *catalog, {source, cluster.nodes[0].client}, [](auto at) {
                return at == repair::RepairExecutionFaultPoint::after_executing_persist;
            });
            // The post-CAS branch completes directly; pre-CAS recovery can be repeatedly interrupted.
            const auto result = again.reconcile(id, plan, members);
            if (GetParam() == repair::RepairExecutionFaultPoint::after_catalog_commit) {
                EXPECT_TRUE(result.state == repair::RepairPlanState::completed); break;
            }
            EXPECT_EQ(result.error, repair::RepairExecutionError::recovery_interrupted);
            EXPECT_EQ(catalog->version(), version);
            EXPECT_EQ(plans->get(id)->execution_id, execution);
        }
        repair::RepairExecutor finish(*plans, *catalog, {source, cluster.nodes[0].client});
        const auto result = finish.reconcile(id, plan, members);
        EXPECT_TRUE(result.state == repair::RepairPlanState::completed);
        EXPECT_EQ(plans->get(id)->execution_id, execution); EXPECT_EQ(plans->get(id)->execution_epoch, cluster.token);
        EXPECT_EQ(metadata::encode_coordinator_operation(*history.get(history_id)), historical);
        EXPECT_EQ(cluster.nodes[0].client->get_chunk(plan.object_id, plan.generation_id, 0).data, data);
    }
}
INSTANTIATE_TEST_SUITE_P(Windows, StressCrash, ::testing::Values(repair::RepairExecutionFaultPoint::after_executing_persist,
    repair::RepairExecutionFaultPoint::after_publication_persist, repair::RepairExecutionFaultPoint::after_catalog_commit));
TEST(StressCatalog, CompetingCasHasOneWinnerAndRepeatedStalePlansNeverPublish) {
    Cluster cluster(1, 4096);
    metadata::DurableReplicaCatalog catalog(cluster.root(), cluster.authority, cluster.token);
    const auto data = payload(1024, 9);
    auto source = std::make_shared<replication::LocalStorageNode>("a", cluster.root() / "source", 16384, 4096);
    const metadata::MembershipSnapshot members{10, {member("a"), member("b", false), member("node-0")}};
    repair::RepairPlanStore plans(cluster.root());
    for (unsigned n = 0; n < 8; ++n) {
        auto plan = placement("object-" + std::to_string(n), manifest(data, 4096, "gen-" + std::to_string(n)), {"a", "b"});
        seed_catalog(catalog, plan);
        const auto planned = repair::RepairPlanner{}.plan(plan, members, catalog.snapshot()); ASSERT_EQ(planned.plans.size(), 1U);
        ASSERT_EQ(plans.create(planned.plans[0]), repair::RepairStoreError::none);
        repair::RepairExecutor executor(plans, catalog, {source, cluster.nodes[0].client});
        ASSERT_EQ(executor.approve(planned.plans[0].plan_id), repair::RepairStoreError::none);
        const auto record = catalog.get(plan.chunks[0].identity); ASSERT_TRUE(record);
        auto desired = record->replicas; desired[0].evidence_version += 1;
        const metadata::ReplicaCatalogCompareExchange cas{record->identity, catalog.version(), record->replicas, desired};
        auto first = std::async(std::launch::async, [&] { return catalog.compare_exchange(cas).status; });
        auto second = std::async(std::launch::async, [&] { return catalog.compare_exchange(cas).status; });
        const auto a = first.get(), b = second.get();
        EXPECT_TRUE((a == metadata::ReplicaCatalogCasStatus::committed && b == metadata::ReplicaCatalogCasStatus::conflict) ||
                    (b == metadata::ReplicaCatalogCasStatus::committed && a == metadata::ReplicaCatalogCasStatus::conflict));
        EXPECT_EQ(executor.execute(planned.plans[0].plan_id, plan, members).state, repair::RepairPlanState::stale);
        EXPECT_EQ(cluster.nodes[0].local->get_chunk(plan.object_id, plan.generation_id, 0).error, storage::StoreError::not_found);
    }
}
TEST(StressJournals, RepeatedCorruptAndMissingRecordsNeverResetEpochOrAdoptRepair) {
    for (unsigned n = 0; n < 8; ++n) {
        test::Certificates temporary;
        const epoch::Token token{"stress", 1, "coordinator", "activation"};
        epoch::Store::provision(temporary.root() / "epoch", token);
        auto store = std::make_unique<epoch::Store>(temporary.root() / "epoch", token);
        if (n % 2) std::filesystem::remove(temporary.root() / "epoch" / "record");
        else { std::ofstream output(temporary.root() / "epoch" / "record", std::ios::trunc); output << "corrupt"; }
        EXPECT_THROW(store->authorize(token), epoch::Error);
        EXPECT_ANY_THROW(epoch::Store(temporary.root() / "epoch", token));
        EXPECT_THROW(epoch::Store::provision(temporary.root() / "epoch", token), epoch::Error);
        std::filesystem::create_directories(temporary.root() / "coordinator" / "repairs");
        { std::ofstream output(temporary.root() / "coordinator" / "repairs" / "plan"); output << "corrupt"; }
        EXPECT_ANY_THROW(repair::RepairPlanStore(temporary.root()));
    }
}
TEST(StressRepair, ConcurrentRequestsToOneExecutorHaveOneCompletionAndNoDuplicateCas) {
    Cluster cluster(1, 4096);
    auto source = std::make_shared<replication::LocalStorageNode>("a", cluster.root() / "source", 16384, 4096);
    const auto data = payload(1024, 88);
    metadata::DurableReplicaCatalog catalog(cluster.root(), cluster.authority, cluster.token);
    repair::RepairPlanStore plans(cluster.root());
    const metadata::MembershipSnapshot members{10, {member("a"), member("b", false), member("node-0")}};
    for (unsigned n = 0; n < 4; ++n) {
        auto plan = placement("object-" + std::to_string(n), manifest(data, 4096, "gen-" + std::to_string(n)), {"a", "b"});
        auto seed = plan; seed.replication_factor = 1; seed.chunks[0].replica_node_ids = {"a"};
        replication::ReplicaManager writer({source}); ASSERT_EQ(writer.put(seed, data).status, replication::ReplicationStatus::success);
        seed_catalog(catalog, plan); const auto before = catalog.version();
        const auto planned = repair::RepairPlanner{}.plan(plan, members, catalog.snapshot()); ASSERT_EQ(planned.plans.size(), 1U);
        const auto id = planned.plans[0].plan_id; ASSERT_EQ(plans.create(planned.plans[0]), repair::RepairStoreError::none);
        repair::RepairExecutor executor(plans, catalog, {source, cluster.nodes[0].client});
        ASSERT_EQ(executor.approve(id), repair::RepairStoreError::none);
        auto first = std::async(std::launch::async, [&] { return executor.execute(id, plan, members); });
        auto second = std::async(std::launch::async, [&] { return executor.execute(id, plan, members); });
        const auto a = first.get(), b = second.get();
        EXPECT_TRUE((a.error == repair::RepairExecutionError::none && b.error == repair::RepairExecutionError::already_completed) ||
                    (b.error == repair::RepairExecutionError::none && a.error == repair::RepairExecutionError::already_completed));
        EXPECT_EQ(catalog.version(), before + 1);
        EXPECT_EQ(plans.get(id)->state, repair::RepairPlanState::completed);
    }
}
class Blocked final : public epoch::Participant {
public:
    explicit Blocked(std::shared_ptr<replication::RemoteStorageNodeClient> node) : node(std::move(node)) {}
    std::string_view id() const noexcept override { return node->id(); }
    epoch::Result activate_epoch(const epoch::Token& previous, const epoch::Token& desired) override {
        return blocked ? epoch::Result{} : node->activate_epoch(previous, desired);
    }
    epoch::Result query_epoch() const override { return blocked ? epoch::Result{} : node->query_epoch(); }
    std::shared_ptr<replication::RemoteStorageNodeClient> node;
    bool blocked{};
};
TEST(StressEpoch, RepeatedPartialActivationsRejectOldSessionsDuringAndAfterResume) {
    Cluster cluster(2, 4096);
    auto blocked = std::make_shared<Blocked>(cluster.nodes[1].client);
    epoch::Coordinator activate(*cluster.authority, {cluster.nodes[0].client, blocked});
    metadata::DurableReplicaCatalog old(cluster.root(), cluster.authority, cluster.token);
    const auto data = payload(1024, 101); const auto value = manifest(data, 4096, "generation");
    for (unsigned n = 0; n < 8; ++n) {
        SCOPED_TRACE("epoch successor=" + std::to_string(n + 2));
        blocked->blocked = true;
        ASSERT_EQ(activate.activate("coordinator", "next-" + std::to_string(n)).status, epoch::Status::unresolved);
        EXPECT_EQ(cluster.nodes[0].client->publish_repair("plan", "execution-" + std::to_string(n), "node-0", "object", value,
            value.chunks[0], data).status, replication::RepairPublishStatus::stale_epoch);
        EXPECT_EQ(old.upsert({"object", "generation", 0}, value.chunks[0], {{"node-0", metadata::CatalogReplicaState::present, value.object_checksum, 10}}),
            metadata::ReplicaCatalogError::epoch_rejected);
        EXPECT_EQ(activate.reconcile().status, epoch::Status::unresolved);
        blocked->blocked = false;
        ASSERT_EQ(activate.resume_activation().status, epoch::Status::accepted);
        EXPECT_EQ(cluster.nodes[1].client->remove("object", "generation"), storage::StoreError::stale_epoch);
        EXPECT_TRUE(std::filesystem::is_empty(cluster.nodes[0].journal / "repairs"));
        EXPECT_EQ(old.version(), 0U);
    }
}
} // namespace
} // namespace atlasfs::bench
