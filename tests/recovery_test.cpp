#include "atlasfs/recovery.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace atlasfs::replication {
namespace {

class FakeNode final : public StorageNode {
public:
    FakeNode(std::string id, OperationState state, std::uint32_t fingerprint = 77)
        : id_(std::move(id)), state_(state), fingerprint_(fingerprint) {}
    std::string_view id() const noexcept override { return id_; }
    storage::StoreError prepare(std::string_view, std::string_view,
        const storage::ObjectManifest&, std::span<const std::uint32_t>,
        std::span<const std::byte>) override { return storage::StoreError::none; }
    storage::StoreError verify_prepared(std::string_view, std::string_view,
        std::string_view, std::span<const std::uint32_t>) const override {
        return storage::StoreError::none;
    }
    storage::StoreError commit(std::string_view, std::string_view,
                               std::string_view) override {
        if (commit_error_ != storage::StoreError::none) return commit_error_;
        if (state_ == OperationState::committed) return storage::StoreError::none;
        if (state_ != OperationState::prepared)
            return storage::StoreError::invalid_operation_state;
        state_ = OperationState::committed; return storage::StoreError::none;
    }
    storage::StoreError abort(std::string_view, std::string_view,
                              std::string_view) override {
        if (abort_error_ != storage::StoreError::none) return abort_error_;
        if (state_ == OperationState::aborted) return storage::StoreError::none;
        if (state_ != OperationState::prepared)
            return storage::StoreError::invalid_operation_state;
        state_ = OperationState::aborted; return storage::StoreError::none;
    }
    storage::StoreError finalize(std::string_view, std::string_view,
                                 std::string_view) override { return storage::StoreError::none; }
    storage::ChunkResult get_chunk(std::string_view, std::string_view,
                                   std::uint32_t) const override { return {}; }
    storage::StoreError remove(std::string_view, std::string_view) override {
        return storage::StoreError::none;
    }
    OperationQueryResult query_operation(std::string_view operation_id) const override {
        if (query_error_ != storage::StoreError::none)
            return {query_error_, std::string(operation_id), OperationState::unknown, 0};
        return {storage::StoreError::none,
                returned_id_.empty() ? std::string(operation_id) : returned_id_,
                state_, fingerprint_};
    }
    storage::StoreError query_error_{storage::StoreError::none};
    storage::StoreError commit_error_{storage::StoreError::none};
    storage::StoreError abort_error_{storage::StoreError::none};
    std::string returned_id_;
private:
    std::string id_;
    OperationState state_;
    std::uint32_t fingerprint_;
};

using Nodes = std::vector<std::shared_ptr<FakeNode>>;

RecoveryRequest request(std::vector<std::string> ids = {"a", "b", "c"}) {
    return {"op-1", 77, "object", "generation-1", std::move(ids)};
}

RecoveryCoordinator coordinator(const Nodes& nodes) {
    return RecoveryCoordinator(
        std::vector<std::shared_ptr<StorageNode>>(nodes.begin(), nodes.end()));
}

TEST(RecoveryCoordinator, ClassifiesAllCommitted) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::committed),
                std::make_shared<FakeNode>("b", OperationState::committed),
                std::make_shared<FakeNode>("c", OperationState::committed)};
    EXPECT_EQ(coordinator(nodes).inspect(request()).classification,
              RecoveryClassification::all_committed);
}

TEST(RecoveryCoordinator, ClassifiesAllAborted) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::aborted),
                std::make_shared<FakeNode>("b", OperationState::aborted),
                std::make_shared<FakeNode>("c", OperationState::aborted)};
    EXPECT_EQ(coordinator(nodes).inspect(request()).classification,
              RecoveryClassification::all_aborted);
}

TEST(RecoveryCoordinator, ClassifiesAllPrepared) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::prepared),
                std::make_shared<FakeNode>("b", OperationState::prepared),
                std::make_shared<FakeNode>("c", OperationState::prepared)};
    const auto report = coordinator(nodes).inspect(request());
    EXPECT_EQ(report.classification, RecoveryClassification::all_prepared);
    EXPECT_TRUE(report.completion_allowed); EXPECT_TRUE(report.rollback_allowed);
}

TEST(RecoveryCoordinator, ClassifiesMixedPreparedCommitted) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::committed),
                std::make_shared<FakeNode>("b", OperationState::prepared)};
    EXPECT_EQ(coordinator(nodes).inspect(request({"a", "b"})).classification,
              RecoveryClassification::mixed_prepared_committed);
}

TEST(RecoveryCoordinator, ClassifiesMixedPreparedAborted) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::aborted),
                std::make_shared<FakeNode>("b", OperationState::prepared)};
    EXPECT_EQ(coordinator(nodes).inspect(request({"a", "b"})).classification,
              RecoveryClassification::mixed_prepared_aborted);
}

TEST(RecoveryCoordinator, RejectsMixedTerminalState) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::committed),
                std::make_shared<FakeNode>("b", OperationState::aborted)};
    auto recovery = coordinator(nodes); const auto report = recovery.inspect(request({"a", "b"}));
    EXPECT_EQ(report.classification, RecoveryClassification::mixed_terminal);
    EXPECT_FALSE(report.completion_allowed); EXPECT_FALSE(report.rollback_allowed);
}

TEST(RecoveryCoordinator, PreservesUnavailableParticipantUncertainty) {
    auto unavailable = std::make_shared<FakeNode>("b", OperationState::prepared);
    unavailable->query_error_ = storage::StoreError::filesystem_error;
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::committed), unavailable};
    EXPECT_EQ(coordinator(nodes).inspect(request({"a", "b"})).classification,
              RecoveryClassification::unavailable_participant);
}

TEST(RecoveryCoordinator, ClassifiesAllUnavailableWithoutGuessing) {
    auto a = std::make_shared<FakeNode>("a", OperationState::committed);
    auto b = std::make_shared<FakeNode>("b", OperationState::aborted);
    a->query_error_ = storage::StoreError::filesystem_error;
    b->query_error_ = storage::StoreError::result_unknown;
    Nodes nodes{a, b};
    const auto report = coordinator(nodes).inspect(request({"a", "b"}));
    EXPECT_EQ(report.classification, RecoveryClassification::unavailable_participant);
    EXPECT_FALSE(report.completion_allowed); EXPECT_FALSE(report.rollback_allowed);
}

TEST(RecoveryCoordinator, DetectsFingerprintAndOperationIdentityConflicts) {
    auto wrong_id = std::make_shared<FakeNode>("a", OperationState::prepared);
    wrong_id->returned_id_ = "op-other";
    Nodes nodes{wrong_id};
    EXPECT_EQ(coordinator(nodes).inspect(request({"a"})).classification,
              RecoveryClassification::identity_conflict);
    Nodes wrong_fingerprint{std::make_shared<FakeNode>("a", OperationState::prepared, 88)};
    EXPECT_EQ(coordinator(wrong_fingerprint).inspect(request({"a"})).classification,
              RecoveryClassification::identity_conflict);
}

TEST(RecoveryCoordinator, ExplicitCompletionCommitsOnlyPreparedParticipants) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::committed),
                std::make_shared<FakeNode>("b", OperationState::prepared)};
    auto recovery = coordinator(nodes); const auto result = recovery.complete(request({"b", "a"}));
    EXPECT_EQ(result.status, RecoveryActionStatus::success);
    EXPECT_EQ(result.after.classification, RecoveryClassification::all_committed);
    EXPECT_EQ(result.acted_nodes, std::vector<std::string>{"b"});
}

TEST(RecoveryCoordinator, RepeatedCompletionIsIdempotent) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::prepared)};
    auto recovery = coordinator(nodes);
    EXPECT_EQ(recovery.complete(request({"a"})).status, RecoveryActionStatus::success);
    EXPECT_EQ(recovery.complete(request({"a"})).status, RecoveryActionStatus::success);
}

TEST(RecoveryCoordinator, ExplicitRollbackAbortsPreparedAndNeverCommitted) {
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::prepared),
                std::make_shared<FakeNode>("b", OperationState::aborted)};
    auto recovery = coordinator(nodes);
    EXPECT_EQ(recovery.rollback(request({"a", "b"})).status,
              RecoveryActionStatus::success);
    Nodes committed{std::make_shared<FakeNode>("a", OperationState::committed)};
    auto unsafe = coordinator(committed).rollback(request({"a"}));
    EXPECT_EQ(unsafe.status, RecoveryActionStatus::rejected);
}

TEST(RecoveryCoordinator, ReportsPartialCompletionWithoutRetrying) {
    auto failed = std::make_shared<FakeNode>("b", OperationState::prepared);
    failed->commit_error_ = storage::StoreError::result_unknown;
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::prepared), failed};
    auto recovery = coordinator(nodes); const auto result = recovery.complete(request({"a", "b"}));
    EXPECT_EQ(result.status, RecoveryActionStatus::partial);
    EXPECT_EQ(result.after.classification,
              RecoveryClassification::mixed_prepared_committed);
    EXPECT_EQ(result.failed_nodes, std::vector<std::string>{"b"});
}

TEST(RecoveryCoordinator, RejectsCompletionWhileAnyParticipantIsUnavailable) {
    auto unavailable = std::make_shared<FakeNode>("b", OperationState::prepared);
    unavailable->query_error_ = storage::StoreError::filesystem_error;
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::prepared), unavailable};
    auto recovery = coordinator(nodes); const auto result = recovery.complete(request({"a", "b"}));
    EXPECT_EQ(result.status, RecoveryActionStatus::rejected);
    EXPECT_TRUE(result.acted_nodes.empty());
}

TEST(RecoveryCoordinator, ReportsPartialRollbackWithoutRetrying) {
    auto failed = std::make_shared<FakeNode>("b", OperationState::prepared);
    failed->abort_error_ = storage::StoreError::filesystem_error;
    Nodes nodes{std::make_shared<FakeNode>("a", OperationState::prepared), failed};
    auto recovery = coordinator(nodes); const auto result = recovery.rollback(request({"a", "b"}));
    EXPECT_EQ(result.status, RecoveryActionStatus::partial);
    EXPECT_EQ(result.after.classification, RecoveryClassification::mixed_prepared_aborted);
    EXPECT_EQ(result.failed_nodes, std::vector<std::string>{"b"});
}

TEST(RecoveryCoordinator, OrdersObservationsByNodeIdNotInputOrder) {
    Nodes nodes{std::make_shared<FakeNode>("c", OperationState::prepared),
                std::make_shared<FakeNode>("a", OperationState::prepared),
                std::make_shared<FakeNode>("b", OperationState::prepared)};
    const auto report = coordinator(nodes).inspect(request({"c", "a", "b"}));
    ASSERT_EQ(report.observations.size(), 3U);
    EXPECT_EQ(report.observations[0].node_id, "a");
    EXPECT_EQ(report.observations[1].node_id, "b");
    EXPECT_EQ(report.observations[2].node_id, "c");
}

}  // namespace
}  // namespace atlasfs::replication
