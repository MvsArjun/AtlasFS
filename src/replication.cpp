#include "atlasfs/replication.hpp"

#include "atlasfs/checksum.hpp"

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
    if (node_id_.empty()) { throw std::invalid_argument("storage node requires an id"); }
}

std::string_view LocalStorageNode::id() const noexcept { return node_id_; }

storage::StoreError LocalStorageNode::prepare(
    std::string_view object_id, const storage::ObjectManifest& manifest,
    std::span<const std::uint32_t> chunk_indices,
    std::span<const std::byte> object_data) {
    return store_.prepare_replica(object_id, manifest, chunk_indices, object_data);
}

storage::StoreError LocalStorageNode::verify_prepared(
    std::string_view object_id, std::string_view generation_id,
    std::span<const std::uint32_t> chunk_indices) const {
    return store_.verify_prepared_replica(object_id, generation_id, chunk_indices);
}

storage::StoreError LocalStorageNode::commit(std::string_view object_id,
                                             std::string_view generation_id) {
    return store_.commit_replica(object_id, generation_id);
}

storage::StoreError LocalStorageNode::abort(std::string_view object_id,
                                            std::string_view generation_id) {
    return store_.abort_replica(object_id, generation_id);
}

storage::StoreError LocalStorageNode::finalize(std::string_view object_id,
                                               std::string_view generation_id) {
    return store_.finalize_replica(object_id, generation_id);
}

storage::ChunkResult LocalStorageNode::get_chunk(
    std::string_view object_id, std::string_view generation_id,
    std::uint32_t chunk_index) const {
    return store_.get_replica_chunk(object_id, generation_id, chunk_index);
}

storage::StoreError LocalStorageNode::remove(std::string_view object_id) {
    return store_.remove(object_id);
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
    ReplicatedPutResult result;
    result.requested_replicas = plan.replication_factor;
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
            error = found->second->prepare(plan.object_id, *manifest, chunk_indices, object_data);
            if (error == storage::StoreError::none) {
                prepared.push_back(node_id);
                error = found->second->verify_prepared(
                    plan.object_id, plan.generation_id, chunk_indices);
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
            if (nodes_.at(node_id)->abort(plan.object_id, plan.generation_id) !=
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
        if (nodes_.at(node_id)->commit(plan.object_id, plan.generation_id) !=
            storage::StoreError::none) {
            result.failed_nodes.push_back(node_id);
            rollback();
            result.status = result.rollback_failed_nodes.empty()
                ? ReplicationStatus::replica_write_failed : ReplicationStatus::rollback_failed;
            return result;
        }
    }
    for (const auto& node_id : prepared) {
        if (nodes_.at(node_id)->finalize(plan.object_id, plan.generation_id) !=
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
                                                 : found->second->remove(plan.object_id);
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
