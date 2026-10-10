#pragma once

#include "atlasfs/coordinator.hpp"
#include "atlasfs/membership.hpp"
#include "atlasfs/replica_catalog.hpp"
#include "atlasfs/replication.hpp"

#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::repair {

enum class RepairExecutionError : std::uint16_t {
    none, plan_not_found, not_approved, already_completed, cancelled, stale,
    source_unavailable, target_unavailable, source_chunk_missing,
    generation_mismatch, chunk_identity_mismatch, integrity_failure,
    target_already_contains_chunk, transfer_failure, verification_failure,
    target_publication_failure, metadata_commit_failure, conflicting_repair,
    persistence_failure, catalog_durability_unknown, remote_result_unknown,
    remote_identity_mismatch, remote_connection_failure, remote_timeout,
    remote_protocol_error, remote_target_rejected
};

enum class RepairPlanState : std::uint8_t {
    planned = 1, unrecoverable = 2, stale = 3, approved = 4,
    executing = 5, completed = 6, failed = 7, cancelled = 8
};
enum class RepairReason : std::uint8_t {
    replica_unavailable = 1, replica_missing = 2, source_unavailable = 3,
    no_eligible_target = 4
};

struct RepairPlan {
    std::string plan_id;
    std::string object_id;
    std::string generation_id;
    std::uint32_t chunk_index{};
    std::string source_node;
    std::string target_node;
    std::uint64_t membership_version{};
    std::uint32_t health_evidence{};
    std::uint32_t replication_factor{};
    std::vector<std::string> observed_replicas;
    RepairReason reason{RepairReason::replica_unavailable};
    RepairPlanState state{RepairPlanState::planned};
    std::string execution_id;
    bool transfer_succeeded{};
    bool verification_succeeded{};
    bool publication_succeeded{};
    bool metadata_committed{};
    RepairExecutionError last_error{RepairExecutionError::none};
    std::uint64_t catalog_version{};
    std::vector<metadata::CatalogReplica> observed_catalog_replicas;
    bool operator==(const RepairPlan&) const = default;
};

enum class PlanningError { none, invalid_plan, invalid_membership, invalid_catalog };
struct PlanningResult {
    PlanningError error{PlanningError::none};
    std::vector<RepairPlan> plans;
};
enum class PlanValidation {
    current, stale_membership, stale_health, stale_placement, stale_catalog
};

class RepairPlanner {
public:
    [[nodiscard]] PlanningResult plan(const metadata::PlacementPlan& placement,
                                      const metadata::MembershipSnapshot& membership,
                                      const metadata::ReplicaCatalogSnapshot& catalog) const;
    [[nodiscard]] PlanValidation validate(const RepairPlan& plan,
                                          const metadata::PlacementPlan& placement,
                                          const metadata::MembershipSnapshot& membership,
                                          const metadata::ReplicaCatalogSnapshot& catalog) const;
};

enum class RepairStoreError { none, invalid_record, conflict, filesystem_error };
enum class RepairFaultPoint { record_creation, record_write, record_sync, record_rename,
                              directory_sync };
using RepairFaultInjector = std::function<bool(RepairFaultPoint)>;

[[nodiscard]] std::vector<std::byte> encode_repair_plan(const RepairPlan& plan);
[[nodiscard]] std::optional<RepairPlan> decode_repair_plan(std::span<const std::byte> bytes);

class RepairPlanStore {
public:
    explicit RepairPlanStore(std::filesystem::path root, RepairFaultInjector fault = {});
    [[nodiscard]] RepairStoreError create(const RepairPlan& plan);
    [[nodiscard]] RepairStoreError update(const RepairPlan& plan);
    [[nodiscard]] std::optional<RepairPlan> get(std::string_view plan_id) const;
    [[nodiscard]] std::vector<RepairPlan> list() const;
    [[nodiscard]] const std::filesystem::path& plans_directory() const noexcept;
private:
    [[nodiscard]] RepairStoreError persist(const RepairPlan& plan);
    std::filesystem::path directory_;
    RepairFaultInjector fault_;
    std::map<std::string, RepairPlan, std::less<>> plans_;
};

struct RepairExecutionResult {
    RepairExecutionError error{RepairExecutionError::none};
    RepairPlanState state{RepairPlanState::planned};
};

class RepairExecutor {
public:
    RepairExecutor(RepairPlanStore& plans, metadata::DurableReplicaCatalog& catalog,
        std::vector<std::shared_ptr<replication::StorageNode>> nodes);
    [[nodiscard]] RepairStoreError approve(std::string_view plan_id);
    [[nodiscard]] RepairStoreError cancel(std::string_view plan_id);
    [[nodiscard]] RepairExecutionResult execute(
        std::string_view plan_id, metadata::PlacementPlan& placement,
        const metadata::MembershipSnapshot& membership);
private:
    RepairPlanStore& plans_;
    metadata::DurableReplicaCatalog& catalog_;
    std::map<std::string, std::shared_ptr<replication::StorageNode>, std::less<>> nodes_;
    RepairPlanner planner_;
    std::mutex execution_mutex_;
};

}  // namespace atlasfs::repair
