#include "atlasfs/coordinator_operation.hpp"

#include "atlasfs/checksum.hpp"
#include "atlasfs/storage_node_protocol.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <sys/stat.h>
#include <unistd.h>

namespace atlasfs::metadata {
namespace {

constexpr std::uint32_t kMagic = 0x41434f50U; // ACOP
constexpr std::uint16_t kVersion = 1;
constexpr std::uint32_t kMaxParticipants = 1024;
constexpr std::size_t kMaxRecordSize = 8U * 1024U * 1024U;

template<class T> void append(std::vector<std::byte>& out, T value) {
    static_assert(std::is_unsigned_v<T>);
    for (std::size_t i = sizeof(T); i > 0; --i)
        out.push_back(static_cast<std::byte>((value >> ((i - 1U) * 8U)) & 0xffU));
}
template<class T> bool take(std::span<const std::byte> in, std::size_t& at, T& value) {
    static_assert(std::is_unsigned_v<T>);
    if (at > in.size() || in.size() - at < sizeof(T)) return false;
    value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i)
        value = static_cast<T>((value << 8U) | std::to_integer<unsigned>(in[at++]));
    return true;
}
void append_string(std::vector<std::byte>& out, std::string_view value) {
    append(out, static_cast<std::uint16_t>(value.size()));
    const auto* begin = reinterpret_cast<const std::byte*>(value.data());
    out.insert(out.end(), begin, begin + value.size());
}
bool take_string(std::span<const std::byte> in, std::size_t& at, std::size_t limit,
                 std::string& value) {
    std::uint16_t size{};
    if (!take(in, at, size) || size == 0 || size > limit || in.size() - at < size) return false;
    value.assign(reinterpret_cast<const char*>(in.data() + at), size); at += size; return true;
}
bool valid_generation(std::string_view value) {
    if (value.empty() || value.size() > storage::kMaxGenerationIdSize) return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
               (c >= '0' && c <= '9') || c == '-';
    });
}
bool valid_type(CoordinatorOperationType value) {
    return value == CoordinatorOperationType::replicated_put ||
           value == CoordinatorOperationType::replicated_delete;
}
bool valid_state(CoordinatorOperationState value) {
    return value >= CoordinatorOperationState::started && value <= CoordinatorOperationState::aborted;
}
bool valid_participant_state(replication::RecoveryParticipantState value) {
    return value >= replication::RecoveryParticipantState::unknown &&
           value <= replication::RecoveryParticipantState::conflict;
}
bool valid_transition(CoordinatorOperationState from, CoordinatorOperationState to) {
    return from != CoordinatorOperationState::committed ||
           to == CoordinatorOperationState::committed;
}
bool valid_record(const CoordinatorOperationRecord& record) {
    if (!storage_node_protocol::valid_operation_id(record.operation_id) ||
        !object_protocol::valid_object_id(record.object_id) ||
        !valid_generation(record.generation_id) || !valid_type(record.type) ||
        !valid_state(record.state) || record.replication_factor == 0 ||
        record.replication_factor > kMaxReplicationFactor || record.participants.empty() ||
        record.participants.size() > kMaxParticipants) return false;
    std::string previous;
    std::size_t total_chunks = 0;
    for (const auto& participant : record.participants) {
        if (!NodeRegistry::valid_node_id(participant.node_id) ||
            (!previous.empty() && participant.node_id <= previous) ||
            !valid_participant_state(participant.last_state) ||
            participant.last_error > storage::StoreError::result_unknown ||
            participant.chunk_indices.size() > storage::kMaxChunkCount ||
            !std::is_sorted(participant.chunk_indices.begin(), participant.chunk_indices.end()) ||
            std::adjacent_find(participant.chunk_indices.begin(), participant.chunk_indices.end()) !=
                participant.chunk_indices.end()) return false;
        previous = participant.node_id;
        total_chunks += participant.chunk_indices.size();
        if (total_chunks > static_cast<std::size_t>(storage::kMaxChunkCount) *
                               kMaxReplicationFactor) return false;
        for (const auto index : participant.chunk_indices)
            if (index >= storage::kMaxChunkCount) return false;
    }
    return true;
}
bool sync_fd(int fd) { int rc; do { rc = ::fsync(fd); } while (rc < 0 && errno == EINTR); return rc == 0; }

}  // namespace

std::vector<std::byte> encode_coordinator_operation(const CoordinatorOperationRecord& record) {
    if (!valid_record(record)) throw std::invalid_argument("invalid coordinator operation record");
    std::vector<std::byte> out; out.reserve(256);
    append(out, kMagic); append(out, kVersion); append(out, std::uint16_t{0});
    append(out, static_cast<std::uint8_t>(record.type));
    append(out, static_cast<std::uint8_t>(record.state)); append(out, std::uint16_t{0});
    append_string(out, record.operation_id); append_string(out, record.object_id);
    append_string(out, record.generation_id); append(out, record.fingerprint);
    append(out, record.placement_plan_identity); append(out, record.membership_version);
    append(out, record.replication_factor);
    append(out, static_cast<std::uint32_t>(record.participants.size()));
    for (const auto& participant : record.participants) {
        append_string(out, participant.node_id);
        append(out, static_cast<std::uint8_t>(participant.last_state));
        append(out, static_cast<std::uint16_t>(participant.last_error));
        append(out, static_cast<std::uint32_t>(participant.chunk_indices.size()));
        for (const auto index : participant.chunk_indices) append(out, index);
    }
    append(out, integrity::crc32c(out));
    if (out.size() > kMaxRecordSize) throw std::length_error("coordinator record too large");
    return out;
}

std::optional<CoordinatorOperationRecord> decode_coordinator_operation(
    std::span<const std::byte> encoded) {
    if (encoded.size() < 32 || encoded.size() > kMaxRecordSize) return std::nullopt;
    const auto body = encoded.first(encoded.size() - 4); std::size_t crc_at = encoded.size() - 4;
    std::uint32_t stored_crc{};
    if (!take(encoded, crc_at, stored_crc) || stored_crc != integrity::crc32c(body)) return std::nullopt;
    std::size_t at = 0; std::uint32_t magic{}, count{}; std::uint16_t version{}, reserved{};
    std::uint8_t type{}, state{};
    CoordinatorOperationRecord record;
    if (!take(body, at, magic) || magic != kMagic || !take(body, at, version) || version != kVersion ||
        !take(body, at, reserved) || reserved != 0 || !take(body, at, type) ||
        !take(body, at, state) || !take(body, at, reserved) || reserved != 0 ||
        !take_string(body, at, storage_node_protocol::kMaxOperationIdSize,
                     record.operation_id) ||
        !take_string(body, at, object_protocol::kMaxObjectIdSize, record.object_id) ||
        !take_string(body, at, storage::kMaxGenerationIdSize, record.generation_id) ||
        !take(body, at, record.fingerprint) || !take(body, at, record.placement_plan_identity) ||
        !take(body, at, record.membership_version) || !take(body, at, record.replication_factor) ||
        !take(body, at, count) || count == 0 || count > kMaxParticipants) return std::nullopt;
    record.type = static_cast<CoordinatorOperationType>(type);
    record.state = static_cast<CoordinatorOperationState>(state);
    record.participants.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        CoordinatorParticipantRecord participant; std::uint8_t pstate{}; std::uint16_t error{};
        std::uint32_t chunks{};
        if (!take_string(body, at, kMaxNodeIdSize, participant.node_id) ||
            !take(body, at, pstate) || !take(body, at, error) || !take(body, at, chunks) ||
            chunks > storage::kMaxChunkCount) return std::nullopt;
        participant.last_state = static_cast<replication::RecoveryParticipantState>(pstate);
        participant.last_error = static_cast<storage::StoreError>(error);
        participant.chunk_indices.reserve(chunks);
        for (std::uint32_t j = 0, index{}; j < chunks; ++j) {
            if (!take(body, at, index)) return std::nullopt;
            participant.chunk_indices.push_back(index);
        }
        record.participants.push_back(std::move(participant));
    }
    if (at != body.size() || !valid_record(record)) return std::nullopt;
    return record;
}

CoordinatorOperationRecord make_coordinator_operation(const PlacementPlan& plan,
    std::string operation_id, std::uint32_t fingerprint, CoordinatorOperationType type) {
    std::map<std::string, std::vector<std::uint32_t>, std::less<>> mapping;
    for (const auto& chunk : plan.chunks)
        for (const auto& node : chunk.replica_node_ids) mapping[node].push_back(chunk.chunk.index);
    CoordinatorOperationRecord record{std::move(operation_id), type, fingerprint, plan.object_id,
        plan.generation_id, 0, plan.membership_version, plan.replication_factor,
        CoordinatorOperationState::started, {}};
    std::vector<std::byte> identity;
    append_string(identity, plan.object_id); append_string(identity, plan.generation_id);
    append(identity, plan.membership_version); append(identity, plan.replication_factor);
    for (auto& [node, chunks] : mapping) {
        record.participants.push_back({node, chunks});
        append_string(identity, node); append(identity, static_cast<std::uint32_t>(chunks.size()));
        for (const auto index : chunks) append(identity, index);
    }
    record.placement_plan_identity = integrity::crc32c(identity);
    if (!valid_record(record)) throw std::invalid_argument("placement cannot form operation record");
    return record;
}

CoordinatorOperationStore::CoordinatorOperationStore(std::filesystem::path root,
    CoordinatorRecordFaultInjector fault_injector)
    : operations_directory_(std::move(root) / "coordinator" / "operations"),
      fault_injector_(std::move(fault_injector)) {
    std::filesystem::create_directories(operations_directory_);
    std::error_code error;
    for (const auto& entry : std::filesystem::directory_iterator(operations_directory_, error)) {
        const auto name = entry.path().filename().string();
        if (name.ends_with(".tmp")) continue;
        std::ifstream input(entry.path(), std::ios::binary);
        std::vector<std::byte> bytes;
        for (char value; input.get(value);) bytes.push_back(static_cast<std::byte>(value));
        const auto record = decode_coordinator_operation(bytes);
        if (!record || record->operation_id != name || !records_.emplace(name, *record).second)
            throw std::runtime_error("invalid durable coordinator operation record");
    }
    if (error) throw std::system_error(error, "scan coordinator operations");
}

CoordinatorRecordError CoordinatorOperationStore::create(const CoordinatorOperationRecord& record) {
    if (!valid_record(record)) return CoordinatorRecordError::invalid_record;
    const auto found = records_.find(record.operation_id);
    if (found != records_.end()) return found->second == record ? CoordinatorRecordError::none
                                                                : CoordinatorRecordError::conflict;
    return persist(record);
}
CoordinatorRecordError CoordinatorOperationStore::update(const CoordinatorOperationRecord& record) {
    if (!valid_record(record)) return CoordinatorRecordError::invalid_record;
    const auto found = records_.find(record.operation_id);
    if (found == records_.end()) return CoordinatorRecordError::not_found;
    if (!valid_transition(found->second.state, record.state))
        return CoordinatorRecordError::conflict;
    if (found->second.type != record.type ||
        found->second.fingerprint != record.fingerprint || found->second.object_id != record.object_id ||
        found->second.generation_id != record.generation_id ||
        found->second.placement_plan_identity != record.placement_plan_identity ||
        found->second.membership_version != record.membership_version ||
        found->second.replication_factor != record.replication_factor ||
        found->second.participants.size() != record.participants.size()) return CoordinatorRecordError::conflict;
    for (std::size_t i = 0; i < record.participants.size(); ++i)
        if (found->second.participants[i].node_id != record.participants[i].node_id ||
            found->second.participants[i].chunk_indices != record.participants[i].chunk_indices)
            return CoordinatorRecordError::conflict;
    return persist(record);
}
CoordinatorRecordError CoordinatorOperationStore::persist(const CoordinatorOperationRecord& record) {
    if (fault_injector_ && fault_injector_(CoordinatorRecordFaultPoint::record_creation))
        return CoordinatorRecordError::filesystem_error;
    std::vector<std::byte> bytes;
    try { bytes = encode_coordinator_operation(record); }
    catch (...) { return CoordinatorRecordError::invalid_record; }
    const auto temporary = operations_directory_ / (record.operation_id + ".tmp");
    const auto final = operations_directory_ / record.operation_id;
    int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return CoordinatorRecordError::filesystem_error;
    bool okay = !(fault_injector_ && fault_injector_(CoordinatorRecordFaultPoint::record_write));
    std::size_t offset = 0;
    while (okay && offset < bytes.size()) {
        const auto written = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written > 0) offset += static_cast<std::size_t>(written);
        else if (written < 0 && errno == EINTR) continue; else okay = false;
    }
    if (fault_injector_ && fault_injector_(CoordinatorRecordFaultPoint::record_sync)) okay = false;
    if (okay) okay = sync_fd(fd);
    if (::close(fd) != 0) okay = false;
    if (!okay) return CoordinatorRecordError::filesystem_error;
    if (fault_injector_ && fault_injector_(CoordinatorRecordFaultPoint::record_rename))
        return CoordinatorRecordError::filesystem_error;
    std::error_code error; std::filesystem::rename(temporary, final, error);
    if (error) return CoordinatorRecordError::filesystem_error;
    if (fault_injector_ && fault_injector_(CoordinatorRecordFaultPoint::directory_sync))
        return CoordinatorRecordError::filesystem_error;
    const int directory = ::open(operations_directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0) return CoordinatorRecordError::filesystem_error;
    const bool directory_synced = sync_fd(directory);
    const bool directory_closed = ::close(directory) == 0;
    if (!directory_synced || !directory_closed)
        return CoordinatorRecordError::filesystem_error;
    records_[record.operation_id] = record;
    return CoordinatorRecordError::none;
}
std::optional<CoordinatorOperationRecord> CoordinatorOperationStore::get(
    std::string_view operation_id) const {
    const auto found = records_.find(operation_id);
    return found == records_.end() ? std::optional<CoordinatorOperationRecord>{}
                                   : std::optional<CoordinatorOperationRecord>{found->second};
}
std::vector<CoordinatorOperationRecord> CoordinatorOperationStore::list() const {
    std::vector<CoordinatorOperationRecord> result; result.reserve(records_.size());
    for (const auto& [id, record] : records_) { static_cast<void>(id); result.push_back(record); }
    return result;
}
const std::filesystem::path& CoordinatorOperationStore::operations_directory() const noexcept {
    return operations_directory_;
}

DurableReplicaManager::DurableReplicaManager(CoordinatorOperationStore& store,
    replication::ReplicaManager& replicas) : store_(store), replicas_(replicas) {}
replication::ReplicatedPutResult DurableReplicaManager::put(const PlacementPlan& plan,
    std::span<const std::byte> object_data, std::string operation_id,
    std::uint32_t fingerprint) {
    auto record = make_coordinator_operation(plan, std::move(operation_id), fingerprint);
    if (store_.create(record) != CoordinatorRecordError::none)
        throw std::runtime_error("persist operation start");
    record.state = CoordinatorOperationState::preparing;
    if (store_.update(record) != CoordinatorRecordError::none)
        throw std::runtime_error("persist operation preparing");

    const auto result = replicas_.put(plan, object_data, record.operation_id);
    for (auto& participant : record.participants) {
        if (result.status == replication::ReplicationStatus::success ||
            result.status == replication::ReplicationStatus::cleanup_failed) {
            participant.last_state = replication::RecoveryParticipantState::committed;
        } else if (result.status == replication::ReplicationStatus::replica_write_failed ||
                   result.status == replication::ReplicationStatus::under_replicated ||
                   result.status == replication::ReplicationStatus::no_eligible_nodes ||
                   result.status == replication::ReplicationStatus::invalid_plan) {
            participant.last_state = replication::RecoveryParticipantState::aborted;
        } else if (std::find(result.failed_nodes.begin(), result.failed_nodes.end(),
                             participant.node_id) != result.failed_nodes.end()) {
            participant.last_state = replication::RecoveryParticipantState::unavailable;
            participant.last_error = storage::StoreError::result_unknown;
        } else if (std::find(result.successful_nodes.begin(), result.successful_nodes.end(),
                             participant.node_id) != result.successful_nodes.end()) {
            participant.last_state = result.status == replication::ReplicationStatus::commit_result_unknown &&
                    !result.failed_nodes.empty() && participant.node_id < result.failed_nodes.front()
                ? replication::RecoveryParticipantState::committed
                : replication::RecoveryParticipantState::prepared;
        }
    }
    if (result.status == replication::ReplicationStatus::success ||
        result.status == replication::ReplicationStatus::cleanup_failed)
        record.state = CoordinatorOperationState::committed;
    else if (result.status == replication::ReplicationStatus::replica_write_failed ||
             result.status == replication::ReplicationStatus::under_replicated ||
             result.status == replication::ReplicationStatus::no_eligible_nodes ||
             result.status == replication::ReplicationStatus::invalid_plan)
        record.state = CoordinatorOperationState::aborted;
    else
        record.state = CoordinatorOperationState::unresolved;
    if (store_.update(record) != CoordinatorRecordError::none)
        throw std::runtime_error("persist operation result");
    return result;
}

DurableRecoveryCoordinator::DurableRecoveryCoordinator(CoordinatorOperationStore& store,
    replication::RecoveryCoordinator& recovery) : store_(store), recovery_(recovery) {}
replication::RecoveryRequest DurableRecoveryCoordinator::request_for(
    const CoordinatorOperationRecord& record) {
    replication::RecoveryRequest request{record.operation_id, record.fingerprint,
        record.object_id, record.generation_id, {}};
    for (const auto& participant : record.participants)
        request.participant_node_ids.push_back(participant.node_id);
    return request;
}
void DurableRecoveryCoordinator::save_report(CoordinatorOperationRecord& record,
    const replication::RecoveryReport& report) {
    for (const auto& observation : report.observations) {
        const auto found = std::find_if(record.participants.begin(), record.participants.end(),
            [&](const auto& participant) { return participant.node_id == observation.node_id; });
        if (found != record.participants.end()) {
            found->last_state = observation.state; found->last_error = observation.error;
        }
    }
    if (record.state == CoordinatorOperationState::committed) {
        // Participant observations are refreshed, but a durable commit is never reversed.
    } else if (report.classification == replication::RecoveryClassification::all_committed)
        record.state = CoordinatorOperationState::committed;
    else if (report.classification == replication::RecoveryClassification::all_aborted)
        record.state = CoordinatorOperationState::aborted;
    else if (report.classification == replication::RecoveryClassification::all_prepared)
        record.state = CoordinatorOperationState::prepared;
    else record.state = CoordinatorOperationState::unresolved;
    if (store_.update(record) != CoordinatorRecordError::none)
        throw std::runtime_error("persist recovery observation");
}
std::optional<replication::RecoveryReport> DurableRecoveryCoordinator::inspect(
    std::string_view operation_id) {
    auto record = store_.get(operation_id); if (!record) return std::nullopt;
    auto report = recovery_.inspect(request_for(*record)); save_report(*record, report); return report;
}
std::optional<replication::RecoveryActionResult> DurableRecoveryCoordinator::complete(
    std::string_view operation_id) {
    auto record = store_.get(operation_id); if (!record) return std::nullopt;
    record->state = CoordinatorOperationState::committing;
    if (store_.update(*record) != CoordinatorRecordError::none) throw std::runtime_error("persist committing");
    auto result = recovery_.complete(request_for(*record)); save_report(*record, result.after); return result;
}
std::optional<replication::RecoveryActionResult> DurableRecoveryCoordinator::rollback(
    std::string_view operation_id) {
    auto record = store_.get(operation_id); if (!record) return std::nullopt;
    if (record->state != CoordinatorOperationState::committed) {
        record->state = CoordinatorOperationState::aborting;
        if (store_.update(*record) != CoordinatorRecordError::none)
            throw std::runtime_error("persist aborting");
    }
    auto result = recovery_.rollback(request_for(*record)); save_report(*record, result.after); return result;
}

}  // namespace atlasfs::metadata
