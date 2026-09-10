#pragma once

#include "atlasfs/coordinator.hpp"
#include "atlasfs/recovery.hpp"

#include <cstdint>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::metadata {

enum class CoordinatorOperationType : std::uint8_t { replicated_put = 1, replicated_delete = 2 };
enum class CoordinatorOperationState : std::uint8_t {
    started = 1, preparing, prepared, committing, unresolved, committed, aborting, aborted
};

struct CoordinatorParticipantRecord {
    std::string node_id;
    std::vector<std::uint32_t> chunk_indices;
    replication::RecoveryParticipantState last_state{
        replication::RecoveryParticipantState::unknown};
    storage::StoreError last_error{storage::StoreError::none};
    bool operator==(const CoordinatorParticipantRecord&) const = default;
};

struct CoordinatorOperationRecord {
    std::string operation_id;
    CoordinatorOperationType type{CoordinatorOperationType::replicated_put};
    std::uint32_t fingerprint{};
    std::string object_id;
    std::string generation_id;
    std::uint32_t placement_plan_identity{};
    std::uint64_t membership_version{};
    std::uint32_t replication_factor{};
    CoordinatorOperationState state{CoordinatorOperationState::started};
    std::vector<CoordinatorParticipantRecord> participants;
    bool operator==(const CoordinatorOperationRecord&) const = default;
};

enum class CoordinatorRecordError {
    none, invalid_record, conflict, not_found, filesystem_error
};

enum class CoordinatorRecordFaultPoint {
    record_creation, record_write, record_sync, record_rename, directory_sync
};
using CoordinatorRecordFaultInjector = std::function<bool(CoordinatorRecordFaultPoint)>;

[[nodiscard]] std::vector<std::byte> encode_coordinator_operation(
    const CoordinatorOperationRecord& record);
[[nodiscard]] std::optional<CoordinatorOperationRecord> decode_coordinator_operation(
    std::span<const std::byte> encoded);

[[nodiscard]] CoordinatorOperationRecord make_coordinator_operation(
    const PlacementPlan& plan, std::string operation_id, std::uint32_t fingerprint,
    CoordinatorOperationType type = CoordinatorOperationType::replicated_put);

class CoordinatorOperationStore {
public:
    explicit CoordinatorOperationStore(std::filesystem::path root,
        CoordinatorRecordFaultInjector fault_injector = {});
    [[nodiscard]] CoordinatorRecordError create(const CoordinatorOperationRecord& record);
    [[nodiscard]] CoordinatorRecordError update(const CoordinatorOperationRecord& record);
    [[nodiscard]] std::optional<CoordinatorOperationRecord> get(
        std::string_view operation_id) const;
    [[nodiscard]] std::vector<CoordinatorOperationRecord> list() const;
    [[nodiscard]] const std::filesystem::path& operations_directory() const noexcept;
private:
    [[nodiscard]] CoordinatorRecordError persist(const CoordinatorOperationRecord& record);
    std::filesystem::path operations_directory_;
    CoordinatorRecordFaultInjector fault_injector_;
    std::map<std::string, CoordinatorOperationRecord, std::less<>> records_;
};

class DurableReplicaManager {
public:
    DurableReplicaManager(CoordinatorOperationStore& store,
                          replication::ReplicaManager& replicas);
    [[nodiscard]] replication::ReplicatedPutResult put(
        const PlacementPlan& plan, std::span<const std::byte> object_data,
        std::string operation_id, std::uint32_t fingerprint);
private:
    CoordinatorOperationStore& store_;
    replication::ReplicaManager& replicas_;
};

class DurableRecoveryCoordinator {
public:
    DurableRecoveryCoordinator(CoordinatorOperationStore& store,
                               replication::RecoveryCoordinator& recovery);
    [[nodiscard]] std::optional<replication::RecoveryReport> inspect(
        std::string_view operation_id);
    [[nodiscard]] std::optional<replication::RecoveryActionResult> complete(
        std::string_view operation_id);
    [[nodiscard]] std::optional<replication::RecoveryActionResult> rollback(
        std::string_view operation_id);
private:
    [[nodiscard]] static replication::RecoveryRequest request_for(
        const CoordinatorOperationRecord& record);
    void save_report(CoordinatorOperationRecord& record,
                     const replication::RecoveryReport& report);
    CoordinatorOperationStore& store_;
    replication::RecoveryCoordinator& recovery_;
};

}  // namespace atlasfs::metadata
