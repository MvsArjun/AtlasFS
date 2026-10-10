#include "atlasfs/replication.hpp"

#include "atlasfs/checksum.hpp"
#include "atlasfs/storage_node_protocol.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

namespace atlasfs::replication {
namespace {

using Assignments = std::map<std::string, std::vector<std::uint32_t>, std::less<>>;

std::optional<storage::ObjectManifest> manifest_from_plan(
    const metadata::PlacementPlan& plan) {
    if (plan.replication_factor == 0 ||
        plan.replication_factor > metadata::kMaxReplicationFactor) {
        return std::nullopt;
    }
    storage::ObjectManifest manifest{plan.object_size, plan.chunk_size,
                                     plan.object_checksum, plan.generation_id, {}};
    manifest.chunks.reserve(plan.chunks.size());
    for (std::size_t index = 0; index < plan.chunks.size(); ++index) {
        const auto& placement = plan.chunks[index];
        if (placement.identity.object_id != plan.object_id ||
            placement.identity.generation_id != plan.generation_id ||
            placement.identity.chunk_index != index || placement.chunk.index != index ||
            placement.replica_node_ids.empty() ||
            placement.replica_node_ids.size() > plan.replication_factor ||
            (!plan.under_replicated &&
             placement.replica_node_ids.size() != plan.replication_factor)) {
            return std::nullopt;
        }
        std::set<std::string> unique(placement.replica_node_ids.begin(),
                                    placement.replica_node_ids.end());
        if (unique.size() != placement.replica_node_ids.size()) { return std::nullopt; }
        manifest.chunks.push_back(placement.chunk);
    }
    try { static_cast<void>(storage::encode_manifest(manifest)); }
    catch (const std::invalid_argument&) { return std::nullopt; }
    return manifest;
}

Assignments assignments_from_plan(const metadata::PlacementPlan& plan) {
    Assignments assignments;
    for (const auto& chunk : plan.chunks) {
        for (const auto& node_id : chunk.replica_node_ids) {
            assignments[node_id].push_back(chunk.chunk.index);
        }
    }
    return assignments;
}

ReplicaState state_for(storage::StoreError error) {
    if (error == storage::StoreError::not_found) { return ReplicaState::missing; }
    if (error == storage::StoreError::corrupt_object ||
        error == storage::StoreError::checksum_mismatch) {
        return ReplicaState::corrupt;
    }
    return ReplicaState::unavailable;
}

}  // namespace

LocalStorageNode::LocalStorageNode(std::string node_id, std::filesystem::path root,
                                   std::size_t max_object_size, std::uint32_t chunk_size,
                                   storage::PutFaultInjector fault_injector)
    : node_id_(std::move(node_id)),
      store_(std::move(root), max_object_size, chunk_size, std::move(fault_injector)) {
    if (!metadata::NodeRegistry::valid_node_id(node_id_)) {
        throw std::invalid_argument("storage node requires a valid id");
    }
}

std::string_view LocalStorageNode::id() const noexcept { return node_id_; }

storage::StoreError LocalStorageNode::prepare(
    std::string_view operation_id, std::string_view object_id,
    const storage::ObjectManifest& manifest,
    std::span<const std::uint32_t> chunk_indices,
    std::span<const std::byte> object_data) {
    std::uint32_t fingerprint{};
    try {
        fingerprint = integrity::crc32c(storage_node_protocol::encode_prepare(
            {std::string(operation_id), std::string(object_id), manifest,
             {chunk_indices.begin(), chunk_indices.end()},
             {object_data.begin(), object_data.end()}}));
    } catch (const std::invalid_argument&) {
        return storage::StoreError::operation_conflict;
    }
    if (const auto found = operations_.find(operation_id); found != operations_.end()) {
        if (found->second.fingerprint != fingerprint)
            return storage::StoreError::operation_conflict;
        return found->second.state == OperationState::prepared
            ? storage::StoreError::none : storage::StoreError::invalid_operation_state;
    }
    const auto error = store_.prepare_replica(object_id, manifest, chunk_indices, object_data);
    if (error == storage::StoreError::none)
        operations_.emplace(std::string(operation_id),
                            LocalOperation{OperationState::prepared, fingerprint});
    return error;
}

storage::StoreError LocalStorageNode::verify_prepared(
    std::string_view, std::string_view object_id, std::string_view generation_id,
    std::span<const std::uint32_t> chunk_indices) const {
    return store_.verify_prepared_replica(object_id, generation_id, chunk_indices);
}

storage::StoreError LocalStorageNode::commit(std::string_view operation_id,
                                             std::string_view object_id,
                                             std::string_view generation_id) {
    const auto found = operations_.find(operation_id);
    if (found == operations_.end()) return storage::StoreError::operation_conflict;
    if (found->second.state == OperationState::committed) return storage::StoreError::none;
    if (found->second.state != OperationState::prepared)
        return storage::StoreError::invalid_operation_state;
    const auto error = store_.commit_replica(object_id, generation_id);
    if (error == storage::StoreError::none) found->second.state = OperationState::committed;
    return error;
}

storage::StoreError LocalStorageNode::abort(std::string_view operation_id,
                                            std::string_view object_id,
                                            std::string_view generation_id) {
    const auto found = operations_.find(operation_id);
    if (found == operations_.end()) return storage::StoreError::operation_conflict;
    if (found->second.state == OperationState::aborted) return storage::StoreError::none;
    if (found->second.state != OperationState::prepared &&
        found->second.state != OperationState::committed)
        return storage::StoreError::invalid_operation_state;
    const auto error = store_.abort_replica(object_id, generation_id);
    if (error == storage::StoreError::none) found->second.state = OperationState::aborted;
    return error;
}

storage::StoreError LocalStorageNode::finalize(std::string_view, std::string_view object_id,
                                               std::string_view generation_id) {
    return store_.finalize_replica(object_id, generation_id);
}

storage::ChunkResult LocalStorageNode::get_chunk(
    std::string_view object_id, std::string_view generation_id,
    std::uint32_t chunk_index) const {
    return store_.get_replica_chunk(object_id, generation_id, chunk_index);
}

storage::StoreError LocalStorageNode::remove(std::string_view object_id,
                                             std::string_view generation_id) {
    return store_.remove_generation(object_id, generation_id);
}

OperationQueryResult LocalStorageNode::query_operation(
    std::string_view operation_id) const {
    const auto found = operations_.find(operation_id);
    if (found == operations_.end())
        return {storage::StoreError::none, std::string(operation_id),
                OperationState::unknown, 0};
    return {storage::StoreError::none, std::string(operation_id),
            found->second.state, found->second.fingerprint};
}

RepairPublishResult LocalStorageNode::publish_repair(
    std::string_view plan_id, std::string_view execution_id,
    std::string_view target_node_id, std::string_view object_id,
    const storage::ObjectManifest& manifest, const storage::ChunkMetadata& chunk,
    std::span<const std::byte> data) {
    if (!storage_node_protocol::valid_operation_id(plan_id) ||
        !storage_node_protocol::valid_operation_id(execution_id) ||
        target_node_id != node_id_) {
        return {RepairPublishStatus::identity_mismatch,
                storage::StoreError::operation_conflict};
    }
    if (!storage::ObjectStore::valid_object_id(object_id) ||
        chunk.index >= manifest.chunks.size() || manifest.chunks[chunk.index] != chunk) {
        return {RepairPublishStatus::invalid_manifest,
                storage::StoreError::corrupt_object};
    }
    if (data.size() != chunk.length || integrity::crc32c(data) != chunk.checksum) {
        return {RepairPublishStatus::checksum_mismatch,
                storage::StoreError::checksum_mismatch};
    }
    const auto existing = store_.get_replica_chunk(
        object_id, manifest.generation_id, chunk.index);
    if (existing.error == storage::StoreError::none) {
        if (existing.manifest == manifest && existing.chunk == chunk &&
            existing.data.size() == data.size() &&
            std::equal(existing.data.begin(), existing.data.end(), data.begin())) {
            return {RepairPublishStatus::already_present_valid,
                    storage::StoreError::none};
        }
        return {RepairPublishStatus::target_rejected,
                storage::StoreError::operation_conflict};
    }
    if (existing.error != storage::StoreError::not_found) {
        return {RepairPublishStatus::target_rejected, existing.error};
    }
    const auto error = store_.publish_repair_chunk(object_id, manifest, chunk, data);
    switch (error) {
        case storage::StoreError::none:
            return {RepairPublishStatus::published, error};
        case storage::StoreError::result_unknown:
            return {RepairPublishStatus::result_unknown, error};
        case storage::StoreError::checksum_mismatch:
            return {RepairPublishStatus::checksum_mismatch, error};
        case storage::StoreError::invalid_object_id:
        case storage::StoreError::operation_conflict:
            return {RepairPublishStatus::invalid_manifest, error};
        case storage::StoreError::not_found:
        case storage::StoreError::corrupt_object:
        case storage::StoreError::filesystem_error:
        case storage::StoreError::payload_too_large:
        case storage::StoreError::invalid_operation_state:
            return {RepairPublishStatus::publication_failure, error};
    }
    return {RepairPublishStatus::publication_failure, error};
}

const storage::ObjectStore& LocalStorageNode::store() const noexcept { return store_; }

ReplicaManager::ReplicaManager(std::vector<std::shared_ptr<StorageNode>> nodes) {
    for (auto& node : nodes) {
        if (!node || node->id().empty() ||
            !nodes_.emplace(std::string(node->id()), node).second) {
            throw std::invalid_argument("replica manager requires distinct storage nodes");
        }
    }
}

ReplicatedPutResult ReplicaManager::put(const metadata::PlacementPlan& plan,
                                        std::span<const std::byte> object_data) {
    return put(plan, object_data, "op-" + plan.generation_id);
}

ReplicatedPutResult ReplicaManager::put(const metadata::PlacementPlan& plan,
                                        std::span<const std::byte> object_data,
                                        std::string_view operation_id) {
    ReplicatedPutResult result;
    result.requested_replicas = plan.replication_factor;
    if (!storage_node_protocol::valid_operation_id(operation_id)) {
        result.status = ReplicationStatus::invalid_plan;
        return result;
    }
    const auto manifest = manifest_from_plan(plan);
    if (!manifest || object_data.size() != plan.object_size ||
        integrity::crc32c(object_data) != plan.object_checksum) {
        result.status = ReplicationStatus::invalid_plan;
        return result;
    }
    if (plan.under_replicated) {
        result.status = ReplicationStatus::under_replicated;
        return result;
    }
    const auto assignments = assignments_from_plan(plan);
    if (!plan.chunks.empty() && assignments.empty()) {
        result.status = ReplicationStatus::no_eligible_nodes;
        return result;
    }
    if (!assignments.empty() &&
        std::none_of(assignments.begin(), assignments.end(), [&](const auto& entry) {
            return nodes_.contains(entry.first);
        })) {
        result.status = ReplicationStatus::no_eligible_nodes;
        for (const auto& [node_id, unused] : assignments) {
            static_cast<void>(unused);
            result.failed_nodes.push_back(node_id);
        }
        return result;
    }
    std::vector<std::string> prepared;
    for (const auto& [node_id, chunk_indices] : assignments) {
        const auto found = nodes_.find(node_id);
        storage::StoreError error = storage::StoreError::not_found;
        if (found != nodes_.end()) {
            error = found->second->prepare(operation_id, plan.object_id, *manifest,
                                           chunk_indices, object_data);
            if (error == storage::StoreError::none) {
                prepared.push_back(node_id);
                error = found->second->verify_prepared(
                    operation_id, plan.object_id, plan.generation_id, chunk_indices);
            }
        }
        if (error == storage::StoreError::none) {
            result.successful_nodes.push_back(node_id);
        } else {
            result.failed_nodes.push_back(node_id);
        }
    }
    auto rollback = [&] {
        for (const auto& node_id : prepared) {
            if (nodes_.at(node_id)->abort(operation_id, plan.object_id,
                                          plan.generation_id) !=
                storage::StoreError::none) {
                result.rollback_failed_nodes.push_back(node_id);
            }
        }
    };
    if (!result.failed_nodes.empty()) {
        rollback();
        result.status = result.rollback_failed_nodes.empty()
            ? ReplicationStatus::replica_write_failed : ReplicationStatus::rollback_failed;
        return result;
    }
    for (const auto& node_id : prepared) {
        const auto commit_error = nodes_.at(node_id)->commit(
            operation_id, plan.object_id, plan.generation_id);
        if (commit_error == storage::StoreError::result_unknown) {
            result.failed_nodes.push_back(node_id);
            result.status = ReplicationStatus::commit_result_unknown;
            return result;
        }
        if (commit_error != storage::StoreError::none) {
            result.failed_nodes.push_back(node_id);
            rollback();
            result.status = result.rollback_failed_nodes.empty()
                ? ReplicationStatus::replica_write_failed : ReplicationStatus::rollback_failed;
            return result;
        }
    }
    for (const auto& node_id : prepared) {
        if (nodes_.at(node_id)->finalize(operation_id, plan.object_id,
                                         plan.generation_id) !=
            storage::StoreError::none) {
            result.failed_nodes.push_back(node_id);
        }
    }
    result.status = result.failed_nodes.empty() ? ReplicationStatus::success
                                                : ReplicationStatus::cleanup_failed;
    return result;
}

ReplicatedGetResult ReplicaManager::get(const metadata::PlacementPlan& plan) const {
    ReplicatedGetResult result;
    const auto expected = manifest_from_plan(plan);
    if (!expected) { result.status = ReplicationStatus::invalid_plan; return result; }
    result.under_replicated = plan.under_replicated;
    result.data.reserve(static_cast<std::size_t>(plan.object_size));
    bool observed_corruption = false;
    for (const auto& placement : plan.chunks) {
        bool found_valid = false;
        for (const auto& node_id : placement.replica_node_ids) {
            const auto found = nodes_.find(node_id);
            if (found == nodes_.end()) {
                result.under_replicated = true;
                result.observations.push_back(
                    {node_id, ReplicaState::unavailable, storage::StoreError::not_found});
                continue;
            }
            auto chunk = found->second->get_chunk(
                plan.object_id, plan.generation_id, placement.chunk.index);
            if (chunk.error != storage::StoreError::none) {
                const auto state = state_for(chunk.error);
                result.under_replicated = true;
                observed_corruption = observed_corruption || state == ReplicaState::corrupt;
                result.observations.push_back({node_id, state, chunk.error});
                continue;
            }
            if (chunk.manifest != *expected || chunk.chunk != placement.chunk ||
                chunk.data.size() != placement.chunk.length ||
                integrity::crc32c(chunk.data) != placement.chunk.checksum) {
                observed_corruption = true;
                result.under_replicated = true;
                result.observations.push_back(
                    {node_id, ReplicaState::corrupt, storage::StoreError::corrupt_object});
                continue;
            }
            result.observations.push_back(
                {node_id, ReplicaState::present, storage::StoreError::none});
            if (!found_valid) {
                result.data.insert(result.data.end(), chunk.data.begin(), chunk.data.end());
                found_valid = true;
            }
        }
        if (!found_valid) {
            result.data.clear();
            result.status = observed_corruption ? ReplicationStatus::all_replicas_corrupt
                                                : ReplicationStatus::all_replicas_unavailable;
            return result;
        }
    }
    if (result.data.size() != plan.object_size ||
        integrity::crc32c(result.data) != plan.object_checksum) {
        result.data.clear();
        result.status = ReplicationStatus::all_replicas_corrupt;
        return result;
    }
    result.checksum = plan.object_checksum;
    result.status = ReplicationStatus::success;
    return result;
}

ReplicatedDeleteResult ReplicaManager::remove(const metadata::PlacementPlan& plan) {
    ReplicatedDeleteResult result;
    if (!manifest_from_plan(plan)) {
        result.status = ReplicationStatus::invalid_plan;
        return result;
    }
    const auto assignments = assignments_from_plan(plan);
    for (const auto& [node_id, unused] : assignments) {
        static_cast<void>(unused);
        const auto found = nodes_.find(node_id);
        const auto error = found == nodes_.end() ? storage::StoreError::filesystem_error
                                                 : found->second->remove(plan.object_id,
                                                                         plan.generation_id);
        if (error == storage::StoreError::none || error == storage::StoreError::not_found) {
            result.deleted_or_missing_nodes.push_back(node_id);
        } else {
            result.failed_nodes.push_back(node_id);
        }
    }
    result.status = result.failed_nodes.empty() ? ReplicationStatus::success
                                                : ReplicationStatus::partial_delete;
    return result;
}

}  // namespace atlasfs::replication
