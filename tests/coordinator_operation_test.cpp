#include "atlasfs/checksum.hpp"
#include "atlasfs/coordinator_operation.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <unistd.h>

namespace atlasfs::metadata {
namespace {

class TempRoot {
public:
    TempRoot() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() /
            ("atlasfs-coordinator-test-" +
             std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
             std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }
    ~TempRoot() { std::error_code error; std::filesystem::remove_all(path_, error); }
    const std::filesystem::path& path() const { return path_; }
private:
    std::filesystem::path path_;
};

CoordinatorOperationRecord record(std::string operation_id = "op-1") {
    return {std::move(operation_id), CoordinatorOperationType::replicated_put, 77,
        "object", "generation-1", 99, 42, 2, CoordinatorOperationState::unresolved,
        {{"node-a", {0, 1}, replication::RecoveryParticipantState::committed,
          storage::StoreError::none},
         {"node-b", {0, 1}, replication::RecoveryParticipantState::unavailable,
          storage::StoreError::filesystem_error}}};
}

void rewrite_crc(std::vector<std::byte>& bytes) {
    const auto crc = integrity::crc32c(std::span(bytes).first(bytes.size() - 4));
    for (unsigned i = 0; i < 4; ++i)
        bytes[bytes.size() - 4 + i] = static_cast<std::byte>((crc >> ((3U - i) * 8U)) & 0xffU);
}

TEST(CoordinatorOperationRecord, RoundTripsStrictVersionedRepresentation) {
    const auto expected = record();
    EXPECT_EQ(decode_coordinator_operation(encode_coordinator_operation(expected)), expected);
}

TEST(CoordinatorOperationRecord, RejectsTruncationTrailingBytesAndBadCrc) {
    auto bytes = encode_coordinator_operation(record());
    auto truncated = bytes; truncated.pop_back(); EXPECT_FALSE(decode_coordinator_operation(truncated));
    auto trailing = bytes; trailing.push_back(std::byte{0}); EXPECT_FALSE(decode_coordinator_operation(trailing));
    bytes[12] ^= std::byte{1}; EXPECT_FALSE(decode_coordinator_operation(bytes));
}

TEST(CoordinatorOperationRecord, RejectsMagicVersionReservedAndLifecycle) {
    for (const std::pair<std::size_t, std::byte> mutation : {
             std::pair<std::size_t, std::byte>{0, std::byte{0}},
             {5, std::byte{2}}, {6, std::byte{1}}, {9, std::byte{99}}}) {
        auto bytes = encode_coordinator_operation(record()); bytes[mutation.first] = mutation.second;
        rewrite_crc(bytes); EXPECT_FALSE(decode_coordinator_operation(bytes));
    }
}

TEST(CoordinatorOperationRecord, RejectsUnsafeAndOversizedIdentityAndParticipants) {
    auto invalid = record("../operation");
    EXPECT_THROW(encode_coordinator_operation(invalid), std::invalid_argument);
    invalid = record(); invalid.participants[0].node_id = "../node";
    EXPECT_THROW(encode_coordinator_operation(invalid), std::invalid_argument);
    invalid = record(); invalid.participants.clear();
    EXPECT_THROW(encode_coordinator_operation(invalid), std::invalid_argument);
}

TEST(CoordinatorOperationStore, CreatesReloadsUpdatesAndOrdersRecords) {
    TempRoot root;
    {
        CoordinatorOperationStore store(root.path());
        ASSERT_EQ(store.create(record("op-b")), CoordinatorRecordError::none);
        ASSERT_EQ(store.create(record("op-a")), CoordinatorRecordError::none);
        auto updated = *store.get("op-a"); updated.state = CoordinatorOperationState::committed;
        ASSERT_EQ(store.update(updated), CoordinatorRecordError::none);
    }
    CoordinatorOperationStore restarted(root.path());
    const auto records = restarted.list(); ASSERT_EQ(records.size(), 2U);
    EXPECT_EQ(records[0].operation_id, "op-a"); EXPECT_EQ(records[0].state,
        CoordinatorOperationState::committed); EXPECT_EQ(records[1].operation_id, "op-b");
}

TEST(CoordinatorOperationStore, EnforcesOperationIdentityConflicts) {
    TempRoot root; CoordinatorOperationStore store(root.path());
    const auto original = record(); ASSERT_EQ(store.create(original), CoordinatorRecordError::none);
    EXPECT_EQ(store.create(original), CoordinatorRecordError::none);
    auto conflict = original; conflict.fingerprint++;
    EXPECT_EQ(store.create(conflict), CoordinatorRecordError::conflict);
    EXPECT_EQ(store.create(record("op-2")), CoordinatorRecordError::none);
}

TEST(CoordinatorOperationStore, CommittedLifecycleCannotRegress) {
    TempRoot root; CoordinatorOperationStore store(root.path());
    auto value = record(); value.state = CoordinatorOperationState::committed;
    ASSERT_EQ(store.create(value), CoordinatorRecordError::none);
    value.state = CoordinatorOperationState::aborted;
    EXPECT_EQ(store.update(value), CoordinatorRecordError::conflict);
    ASSERT_TRUE(store.get("op-1"));
    EXPECT_EQ(store.get("op-1")->state, CoordinatorOperationState::committed);
}

TEST(CoordinatorOperationStore, FailedPublicationNeverEntersTheIndex) {
    for (const auto point : {CoordinatorRecordFaultPoint::record_creation,
         CoordinatorRecordFaultPoint::record_write, CoordinatorRecordFaultPoint::record_sync,
         CoordinatorRecordFaultPoint::record_rename, CoordinatorRecordFaultPoint::directory_sync}) {
        TempRoot root;
        CoordinatorOperationStore store(root.path(), [point](auto current) { return current == point; });
        EXPECT_EQ(store.create(record()), CoordinatorRecordError::filesystem_error);
        EXPECT_FALSE(store.get("op-1"));
    }
}

TEST(CoordinatorOperationStore, CorruptPublishedRecordFailsStartupWithoutDeletion) {
    TempRoot root; std::filesystem::path path;
    { CoordinatorOperationStore store(root.path()); ASSERT_EQ(store.create(record()), CoordinatorRecordError::none);
      path = store.operations_directory() / "op-1"; }
    std::ofstream(path, std::ios::binary | std::ios::trunc).put('x');
    EXPECT_THROW(CoordinatorOperationStore(root.path()), std::runtime_error);
    EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(CoordinatorOperationRecord, PreservesHistoricalPlacementWithoutRecomputation) {
    PlacementPlan plan{"object", "generation-1", 8, 4, 10, 50, 2, false,
        {{{"object", "generation-1", 0}, {0, 4, 1}, {"node-a", "node-c"}},
         {{"object", "generation-1", 1}, {1, 4, 2}, {"node-b", "node-c"}}}};
    const auto value = make_coordinator_operation(plan, "op-history", 77);
    ASSERT_EQ(value.participants.size(), 3U);
    EXPECT_EQ(value.membership_version, 50U);
    EXPECT_EQ(value.participants[2].node_id, "node-c");
    EXPECT_EQ(value.participants[2].chunk_indices, (std::vector<std::uint32_t>{0, 1}));
}

class QueryNode final : public replication::StorageNode {
public:
    QueryNode(std::string id, replication::OperationState state,
              storage::StoreError commit_error = storage::StoreError::none)
        : id_(std::move(id)), state_(state), commit_error_(commit_error) {}
    std::string_view id() const noexcept override { return id_; }
    storage::StoreError prepare(std::string_view, std::string_view, const storage::ObjectManifest&,
        std::span<const std::uint32_t>, std::span<const std::byte>) override {
        state_ = replication::OperationState::prepared; return storage::StoreError::none;
    }
    storage::StoreError verify_prepared(std::string_view, std::string_view, std::string_view,
        std::span<const std::uint32_t>) const override { return storage::StoreError::none; }
    storage::StoreError commit(std::string_view, std::string_view, std::string_view) override {
        if (commit_error_ == storage::StoreError::none)
            state_ = replication::OperationState::committed;
        return commit_error_;
    }
    storage::StoreError abort(std::string_view, std::string_view, std::string_view) override { state_ = replication::OperationState::aborted; return storage::StoreError::none; }
    storage::StoreError finalize(std::string_view, std::string_view, std::string_view) override { return storage::StoreError::none; }
    storage::ChunkResult get_chunk(std::string_view, std::string_view, std::uint32_t) const override { return {}; }
    storage::StoreError remove(std::string_view, std::string_view) override { return storage::StoreError::none; }
    replication::OperationQueryResult query_operation(std::string_view op) const override {
        ++queries_; return {storage::StoreError::none, std::string(op), state_, 77};
    }
    mutable unsigned queries_{};
private:
    std::string id_;
    mutable replication::OperationState state_;
    storage::StoreError commit_error_;
};

TEST(DurableReplicaManager, AmbiguousCommitIsPersistedBeforeReturning) {
    TempRoot root;
    auto a = std::make_shared<QueryNode>("node-a", replication::OperationState::unknown);
    auto b = std::make_shared<QueryNode>("node-b", replication::OperationState::unknown,
                                        storage::StoreError::result_unknown);
    replication::ReplicaManager replicas({a, b});
    CoordinatorOperationStore store(root.path());
    DurableReplicaManager durable(store, replicas);
    const std::vector<std::byte> data{std::byte{42}};
    const auto checksum = integrity::crc32c(data);
    PlacementPlan plan{"object", "generation-1", 1, checksum, 1, 50, 2, false,
        {{{"object", "generation-1", 0}, {0, 1, checksum}, {"node-a", "node-b"}}}};

    const auto result = durable.put(plan, data, "op-ambiguous", 77);
    EXPECT_EQ(result.status, replication::ReplicationStatus::commit_result_unknown);
    const auto persisted = store.get("op-ambiguous"); ASSERT_TRUE(persisted);
    EXPECT_EQ(persisted->state, CoordinatorOperationState::unresolved);
    EXPECT_EQ(persisted->participants[0].last_state,
              replication::RecoveryParticipantState::committed);
    EXPECT_EQ(persisted->participants[1].last_state,
              replication::RecoveryParticipantState::unavailable);
}

TEST(DurableRecoveryCoordinator, RestartDoesNotActAndFreshInspectionReplacesStaleState) {
    TempRoot root;
    { CoordinatorOperationStore store(root.path()); ASSERT_EQ(store.create(record()), CoordinatorRecordError::none); }
    CoordinatorOperationStore restarted(root.path());
    auto a = std::make_shared<QueryNode>("node-a", replication::OperationState::committed);
    auto b = std::make_shared<QueryNode>("node-b", replication::OperationState::committed);
    EXPECT_EQ(a->queries_, 0U); EXPECT_EQ(b->queries_, 0U);
    replication::RecoveryCoordinator recovery({a, b});
    DurableRecoveryCoordinator durable(restarted, recovery);
    const auto report = durable.inspect("op-1"); ASSERT_TRUE(report);
    EXPECT_EQ(report->classification, replication::RecoveryClassification::all_committed);
    const auto updated = restarted.get("op-1"); ASSERT_TRUE(updated);
    EXPECT_EQ(updated->state, CoordinatorOperationState::committed);
    EXPECT_EQ(updated->participants[1].last_state,
              replication::RecoveryParticipantState::committed);
}

}  // namespace
}  // namespace atlasfs::metadata
