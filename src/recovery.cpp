#include "atlasfs/recovery.hpp"

#include "atlasfs/storage_node_protocol.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

namespace atlasfs::replication {
namespace {

RecoveryParticipantState participant_state(OperationState state) {
    switch (state) {
        case OperationState::unknown: return RecoveryParticipantState::unknown;
        case OperationState::prepared: return RecoveryParticipantState::prepared;
        case OperationState::committed: return RecoveryParticipantState::committed;
        case OperationState::aborted: return RecoveryParticipantState::aborted;
    }
    return RecoveryParticipantState::unknown;
}

RecoveryClassification classify(const std::vector<RecoveryObservation>& observations) {
    bool prepared = false, committed = false, aborted = false;
    bool unavailable = false, conflict = false, unknown = false;
    for (const auto& observation : observations) {
        switch (observation.state) {
            case RecoveryParticipantState::prepared: prepared = true; break;
            case RecoveryParticipantState::committed: committed = true; break;
            case RecoveryParticipantState::aborted: aborted = true; break;
            case RecoveryParticipantState::unavailable: unavailable = true; break;
            case RecoveryParticipantState::conflict: conflict = true; break;
            case RecoveryParticipantState::unknown: unknown = true; break;
        }
    }
    if (conflict) return RecoveryClassification::identity_conflict;
    if (unavailable) return RecoveryClassification::unavailable_participant;
    if (unknown || observations.empty()) return RecoveryClassification::unknown_participant;
    if (committed && aborted) return RecoveryClassification::mixed_terminal;
    if (committed && prepared) return RecoveryClassification::mixed_prepared_committed;
    if (prepared && aborted) return RecoveryClassification::mixed_prepared_aborted;
    if (committed) return RecoveryClassification::all_committed;
    if (aborted) return RecoveryClassification::all_aborted;
    return RecoveryClassification::all_prepared;
}

}  // namespace

RecoveryCoordinator::RecoveryCoordinator(std::vector<std::shared_ptr<StorageNode>> nodes) {
    for (auto& node : nodes) {
        if (!node || node->id().empty() ||
            !nodes_.emplace(std::string(node->id()), node).second)
            throw std::invalid_argument("recovery requires distinct storage nodes");
    }
}

RecoveryReport RecoveryCoordinator::inspect(const RecoveryRequest& request) const {
    RecoveryReport report;
    report.operation_id = request.operation_id;
    std::set<std::string> participants(request.participant_node_ids.begin(),
                                       request.participant_node_ids.end());
    if (!storage_node_protocol::valid_operation_id(request.operation_id)) {
        for (const auto& node_id : participants)
            report.observations.push_back({node_id, RecoveryParticipantState::conflict,
                storage::StoreError::operation_conflict, request.operation_id, 0});
        report.classification = RecoveryClassification::identity_conflict;
        return report;
    }
    for (const auto& node_id : participants) {
        const auto found = nodes_.find(node_id);
        if (found == nodes_.end()) {
            report.observations.push_back({node_id, RecoveryParticipantState::unavailable,
                storage::StoreError::filesystem_error, "", 0});
            continue;
        }
        const auto query = found->second->query_operation(request.operation_id);
        RecoveryObservation observation{node_id, RecoveryParticipantState::unknown,
                                        query.error, query.operation_id, query.fingerprint};
        if (query.error == storage::StoreError::operation_conflict ||
            (query.error == storage::StoreError::none &&
             query.operation_id != request.operation_id) ||
            (query.error == storage::StoreError::none &&
             query.state != OperationState::unknown &&
             query.fingerprint != request.fingerprint)) {
            observation.state = RecoveryParticipantState::conflict;
        } else if (query.error != storage::StoreError::none) {
            observation.state = RecoveryParticipantState::unavailable;
        } else {
            observation.state = participant_state(query.state);
        }
        report.observations.push_back(std::move(observation));
    }
    report.classification = classify(report.observations);
    report.completion_allowed =
        report.classification == RecoveryClassification::all_prepared ||
        report.classification == RecoveryClassification::mixed_prepared_committed ||
        report.classification == RecoveryClassification::all_committed;
    report.rollback_allowed =
        report.classification == RecoveryClassification::all_prepared ||
        report.classification == RecoveryClassification::mixed_prepared_aborted ||
        report.classification == RecoveryClassification::all_aborted;
    return report;
}

RecoveryActionResult RecoveryCoordinator::complete(const RecoveryRequest& request) {
    RecoveryActionResult result;
    result.before = inspect(request);
    if (!result.before.completion_allowed) { result.after = result.before; return result; }
    for (const auto& observation : result.before.observations) {
        if (observation.state != RecoveryParticipantState::prepared) continue;
        const auto error = nodes_.at(observation.node_id)->commit(
            request.operation_id, request.object_id, request.generation_id);
        if (error == storage::StoreError::none) {
            result.acted_nodes.push_back(observation.node_id);
            if (nodes_.at(observation.node_id)->finalize(
                    request.operation_id, request.object_id, request.generation_id) !=
                storage::StoreError::none)
                result.failed_nodes.push_back(observation.node_id);
        } else result.failed_nodes.push_back(observation.node_id);
    }
    result.after = inspect(request);
    result.status = result.after.classification == RecoveryClassification::all_committed
        ? RecoveryActionStatus::success : RecoveryActionStatus::partial;
    return result;
}

RecoveryActionResult RecoveryCoordinator::rollback(const RecoveryRequest& request) {
    RecoveryActionResult result;
    result.before = inspect(request);
    if (!result.before.rollback_allowed) { result.after = result.before; return result; }
    for (const auto& observation : result.before.observations) {
        if (observation.state != RecoveryParticipantState::prepared) continue;
        const auto error = nodes_.at(observation.node_id)->abort(
            request.operation_id, request.object_id, request.generation_id);
        if (error == storage::StoreError::none) result.acted_nodes.push_back(observation.node_id);
        else result.failed_nodes.push_back(observation.node_id);
    }
    result.after = inspect(request);
    result.status = result.after.classification == RecoveryClassification::all_aborted
        ? RecoveryActionStatus::success : RecoveryActionStatus::partial;
    return result;
}

}  // namespace atlasfs::replication
