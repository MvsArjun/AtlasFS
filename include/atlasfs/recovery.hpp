#pragma once

#include "atlasfs/replication.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::replication {

enum class RecoveryParticipantState {
    unknown, prepared, committed, aborted, unavailable, conflict
};

enum class RecoveryClassification {
    all_committed,
    all_aborted,
    all_prepared,
    mixed_prepared_committed,
    mixed_prepared_aborted,
    mixed_terminal,
    unavailable_participant,
    identity_conflict,
    unknown_participant,
};

struct RecoveryRequest {
    std::string operation_id;
    std::uint32_t fingerprint{};
    std::string object_id;
    std::string generation_id;
    std::vector<std::string> participant_node_ids;
};

struct RecoveryObservation {
    std::string node_id;
    RecoveryParticipantState state{RecoveryParticipantState::unknown};
    storage::StoreError error{storage::StoreError::none};
    std::string returned_operation_id;
    std::uint32_t fingerprint{};
};

struct RecoveryReport {
    std::string operation_id;
    RecoveryClassification classification{RecoveryClassification::unknown_participant};
    std::vector<RecoveryObservation> observations;
    bool completion_allowed{};
    bool rollback_allowed{};
};

enum class RecoveryActionStatus { success, partial, rejected };

struct RecoveryActionResult {
    RecoveryActionStatus status{RecoveryActionStatus::rejected};
    RecoveryReport before;
    RecoveryReport after;
    std::vector<std::string> acted_nodes;
    std::vector<std::string> failed_nodes;
};

class RecoveryCoordinator {
public:
    explicit RecoveryCoordinator(std::vector<std::shared_ptr<StorageNode>> nodes);
    [[nodiscard]] RecoveryReport inspect(const RecoveryRequest& request) const;
    [[nodiscard]] RecoveryActionResult complete(const RecoveryRequest& request);
    [[nodiscard]] RecoveryActionResult rollback(const RecoveryRequest& request);

private:
    std::map<std::string, std::shared_ptr<StorageNode>, std::less<>> nodes_;
};

}  // namespace atlasfs::replication
