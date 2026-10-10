#pragma once

#include "atlasfs/coordinator.hpp"
#include "atlasfs/object_store.hpp"
#include "atlasfs/operation.hpp"

#include <filesystem>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::replication {

enum class ReplicaState { present, missing, corrupt, unavailable };

enum class ReplicationStatus {
    success,
    under_replicated,
    no_eligible_nodes,
    replica_write_failed,
    rollback_failed,
    cleanup_failed,
    all_replicas_unavailable,
    all_replicas_corrupt,
    partial_delete,
    invalid_plan,
    commit_result_unknown,
};

struct ReplicaObservation {
    std::string node_id;
    ReplicaState state{ReplicaState::unavailable};
    storage::StoreError error{storage::StoreError::none};
};

struct ReplicatedPutResult {
    ReplicationStatus status{ReplicationStatus::success};
    std::uint32_t requested_replicas{};
    std::vector<std::string> successful_nodes;
    std::vector<std::string> failed_nodes;
    std::vector<std::string> rollback_failed_nodes;
};

struct ReplicatedGetResult {
    ReplicationStatus status{ReplicationStatus::success};
    std::vector<std::byte> data;
    std::uint32_t checksum{};
    bool under_replicated{};
    std::vector<ReplicaObservation> observations;
};

struct ReplicatedDeleteResult {
    ReplicationStatus status{ReplicationStatus::success};
    std::vector<std::string> deleted_or_missing_nodes;
    std::vector<std::string> failed_nodes;
};

enum class RepairPublishStatus {
    published,
    already_present_valid,
    connection_failure,
    timeout,
    malformed_response,
    identity_mismatch,
    target_rejected,
    invalid_manifest,
    checksum_mismatch,
    publication_failure,
    result_unknown,
};

struct RepairPublishResult {
    RepairPublishStatus status{RepairPublishStatus::target_rejected};
    storage::StoreError storage_error{storage::StoreError::none};
};

class StorageNode {
public:
    virtual ~StorageNode() = default;
    [[nodiscard]] virtual std::string_view id() const noexcept = 0;
    [[nodiscard]] virtual storage::StoreError prepare(
        std::string_view operation_id, std::string_view object_id,
        const storage::ObjectManifest& manifest,
        std::span<const std::uint32_t> chunk_indices,
        std::span<const std::byte> object_data) = 0;
    [[nodiscard]] virtual storage::StoreError verify_prepared(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id,
        std::span<const std::uint32_t> chunk_indices) const = 0;
    [[nodiscard]] virtual storage::StoreError commit(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id) = 0;
    [[nodiscard]] virtual storage::StoreError abort(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id) = 0;
    [[nodiscard]] virtual storage::StoreError finalize(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id) = 0;
    [[nodiscard]] virtual storage::ChunkResult get_chunk(
        std::string_view object_id, std::string_view generation_id,
        std::uint32_t chunk_index) const = 0;
    [[nodiscard]] virtual storage::StoreError remove(std::string_view object_id,
                                                     std::string_view generation_id) = 0;
    [[nodiscard]] virtual OperationQueryResult query_operation(
        std::string_view operation_id) const = 0;
    [[nodiscard]] virtual RepairPublishResult publish_repair(
        std::string_view, std::string_view, std::string_view,
        std::string_view, const storage::ObjectManifest&,
        const storage::ChunkMetadata&, std::span<const std::byte>) {
        return {RepairPublishStatus::target_rejected,
                storage::StoreError::invalid_operation_state};
    }
};

class LocalStorageNode final : public StorageNode {
public:
    LocalStorageNode(std::string node_id, std::filesystem::path root,
                     std::size_t max_object_size = object_protocol::kMaxObjectDataSize,
                     std::uint32_t chunk_size = storage::kDefaultChunkSize,
                     storage::PutFaultInjector fault_injector = {});

    [[nodiscard]] std::string_view id() const noexcept override;
    [[nodiscard]] storage::StoreError prepare(
        std::string_view operation_id, std::string_view object_id,
        const storage::ObjectManifest& manifest,
        std::span<const std::uint32_t> chunk_indices,
        std::span<const std::byte> object_data) override;
    [[nodiscard]] storage::StoreError verify_prepared(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id,
        std::span<const std::uint32_t> chunk_indices) const override;
    [[nodiscard]] storage::StoreError commit(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id) override;
    [[nodiscard]] storage::StoreError abort(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id) override;
    [[nodiscard]] storage::StoreError finalize(
        std::string_view operation_id, std::string_view object_id,
        std::string_view generation_id) override;
    [[nodiscard]] storage::ChunkResult get_chunk(
        std::string_view object_id, std::string_view generation_id,
        std::uint32_t chunk_index) const override;
    [[nodiscard]] storage::StoreError remove(std::string_view object_id,
                                             std::string_view generation_id) override;
    [[nodiscard]] OperationQueryResult query_operation(
        std::string_view operation_id) const override;
    [[nodiscard]] RepairPublishResult publish_repair(
        std::string_view plan_id, std::string_view execution_id,
        std::string_view target_node_id, std::string_view object_id,
        const storage::ObjectManifest& manifest,
        const storage::ChunkMetadata& chunk,
        std::span<const std::byte> data) override;
    [[nodiscard]] const storage::ObjectStore& store() const noexcept;

private:
    std::string node_id_;
    storage::ObjectStore store_;
    struct LocalOperation {
        OperationState state{OperationState::unknown};
        std::uint32_t fingerprint{};
    };
    std::map<std::string, LocalOperation, std::less<>> operations_;
};

class ReplicaManager {
public:
    explicit ReplicaManager(std::vector<std::shared_ptr<StorageNode>> nodes);

    [[nodiscard]] ReplicatedPutResult put(
        const metadata::PlacementPlan& plan,
        std::span<const std::byte> object_data);
    [[nodiscard]] ReplicatedPutResult put(
        const metadata::PlacementPlan& plan, std::span<const std::byte> object_data,
        std::string_view operation_id);
    [[nodiscard]] ReplicatedGetResult get(const metadata::PlacementPlan& plan) const;
    [[nodiscard]] ReplicatedDeleteResult remove(const metadata::PlacementPlan& plan);

private:
    std::map<std::string, std::shared_ptr<StorageNode>, std::less<>> nodes_;
};

}  // namespace atlasfs::replication
