#include "atlasfs/repair.hpp"
#include "atlasfs/metrics.hpp"

#include "atlasfs/checksum.hpp"
#include "atlasfs/object_protocol.hpp"
#include "atlasfs/storage_node_protocol.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <iomanip>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <sys/stat.h>
#include <utility>
#include <unistd.h>

namespace atlasfs::repair {
namespace {
constexpr std::uint32_t kMagic = 0x4152504cU; // ARPL
constexpr std::uint16_t kVersion = 3;
constexpr std::size_t kMaxRecordSize = 1024U * 1024U;

template<class T> void append(std::vector<std::byte>& out, T value) {
    static_assert(std::is_unsigned_v<T>);
    for (std::size_t i = sizeof(T); i > 0; --i)
        out.push_back(static_cast<std::byte>((value >> ((i - 1) * 8U)) & 0xffU));
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
bool take_string(std::span<const std::byte> in, std::size_t& at, std::size_t maximum,
                 std::string& value, bool empty = false) {
    std::uint16_t size{};
    if (!take(in, at, size) || (!empty && size == 0) || size > maximum || at > in.size() ||
        in.size() - at < size) return false;
    value.assign(reinterpret_cast<const char*>(in.data() + at), size); at += size; return true;
}
bool healthy(const metadata::StorageNode& node) {
    return node.state == metadata::NodeState::active &&
           node.health == metadata::NodeHealthState::healthy && node.health_current;
}
std::uint32_t evidence(const metadata::MembershipSnapshot& membership) {
    std::vector<std::byte> bytes; append(bytes, membership.version);
    for (const auto& node : membership.nodes) {
        append_string(bytes, node.node_id); append(bytes, static_cast<std::uint8_t>(node.state));
        append(bytes, static_cast<std::uint8_t>(node.health));
        append(bytes, static_cast<std::uint8_t>(node.health_current));
        append(bytes, node.consecutive_failures);
    }
    return atlasfs::integrity::crc32c(bytes);
}
bool valid_reason(RepairReason value) {
    return value >= RepairReason::replica_unavailable && value <= RepairReason::no_eligible_target;
}
bool valid_state(RepairPlanState value) {
    return value >= RepairPlanState::planned && value <= RepairPlanState::cancelled;
}
bool valid_catalog_state(metadata::CatalogReplicaState value) {
    return value == metadata::CatalogReplicaState::present ||
        value == metadata::CatalogReplicaState::missing ||
        value == metadata::CatalogReplicaState::corrupt ||
        value == metadata::CatalogReplicaState::unavailable;
}
bool valid_generation(std::string_view value) {
    return !value.empty() && value.size() <= atlasfs::storage::kMaxGenerationIdSize &&
        std::all_of(value.begin(), value.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                   (c >= '0' && c <= '9') || c == '-';
        });
}
bool valid_plan(const RepairPlan& value) {
    if (!atlasfs::storage_node_protocol::valid_operation_id(value.plan_id) ||
        !atlasfs::object_protocol::valid_object_id(value.object_id) ||
        !valid_generation(value.generation_id) ||
        value.chunk_index >= atlasfs::storage::kMaxChunkCount || value.replication_factor == 0 ||
        value.replication_factor > atlasfs::metadata::kMaxReplicationFactor ||
        value.observed_replicas.size() > atlasfs::metadata::kMaxMembershipNodes ||
        !valid_reason(value.reason) || !valid_state(value.state) ||
        value.execution_id.size() > atlasfs::storage_node_protocol::kMaxOperationIdSize ||
        (!value.execution_id.empty() &&
         !atlasfs::storage_node_protocol::valid_operation_id(value.execution_id))) return false;
    if (!value.source_node.empty() && !atlasfs::metadata::NodeRegistry::valid_node_id(value.source_node)) return false;
    if (!value.target_node.empty() && !atlasfs::metadata::NodeRegistry::valid_node_id(value.target_node)) return false;
    std::string previous;
    for (const auto& node : value.observed_replicas) {
        if (!atlasfs::metadata::NodeRegistry::valid_node_id(node) ||
            (!previous.empty() && node <= previous)) return false;
        previous = node;
    }
    if (value.catalog_version == 0) {
        if (!value.observed_catalog_replicas.empty()) return false;
    } else {
        if (value.observed_catalog_replicas.size() != value.observed_replicas.size())
            return false;
        previous.clear();
        for (std::size_t i = 0; i < value.observed_catalog_replicas.size(); ++i) {
            const auto& replica = value.observed_catalog_replicas[i];
            if (!metadata::NodeRegistry::valid_node_id(replica.node_id) ||
                !valid_catalog_state(replica.state) || replica.evidence_version == 0 ||
                (!previous.empty() && replica.node_id <= previous) ||
                replica.node_id != value.observed_replicas[i] ||
                ((replica.state == metadata::CatalogReplicaState::missing ||
                  replica.state == metadata::CatalogReplicaState::unavailable) &&
                 replica.integrity_checksum != 0)) return false;
            previous = replica.node_id;
        }
    }
    const bool any_result = value.transfer_succeeded || value.verification_succeeded ||
        value.publication_succeeded || value.metadata_committed;
    if ((value.state == RepairPlanState::planned || value.state == RepairPlanState::approved ||
         value.state == RepairPlanState::cancelled ||
         value.state == RepairPlanState::unrecoverable) &&
        (!value.execution_id.empty() || any_result)) return false;
    if ((value.state == RepairPlanState::executing || value.state == RepairPlanState::completed ||
         value.state == RepairPlanState::failed) && value.execution_id.empty()) return false;
    if (value.verification_succeeded && !value.transfer_succeeded) return false;
    if (value.publication_succeeded && !value.transfer_succeeded) return false;
    if (value.metadata_committed && (!value.verification_succeeded ||
                                     !value.publication_succeeded)) return false;
    if (value.state == RepairPlanState::completed && !value.metadata_committed) return false;
    if (value.last_error > RepairExecutionError::epoch_unresolved) return false;
    if (value.execution_epoch && (!epoch::valid_token(*value.execution_epoch) || value.execution_id.empty())) return false;
    if ((value.state == RepairPlanState::planned || value.state == RepairPlanState::approved ||
         value.state == RepairPlanState::executing || value.state == RepairPlanState::completed ||
         value.state == RepairPlanState::unrecoverable) &&
        value.last_error != RepairExecutionError::none) return false;
    if (value.state == RepairPlanState::failed && value.last_error == RepairExecutionError::none)
        return false;
    return true;
}
std::string plan_id(std::string_view object, std::string_view generation, std::uint32_t chunk,
                    std::uint64_t membership, std::uint64_t catalog,
                    std::uint32_t slot) {
    std::vector<std::byte> bytes; append_string(bytes, object); append_string(bytes, generation);
    append(bytes, chunk); append(bytes, membership); append(bytes, catalog); append(bytes, slot);
    std::ostringstream output; output << "repair-" << std::hex << std::setw(8) << std::setfill('0')
                                      << atlasfs::integrity::crc32c(bytes);
    return output.str();
}
std::string execution_id(std::string_view plan) {
    const auto* begin = reinterpret_cast<const std::byte*>(plan.data());
    const auto checksum = atlasfs::integrity::crc32c(
        std::span<const std::byte>(begin, plan.size()));
    std::ostringstream output;
    output << "execution-" << std::hex << std::setw(8) << std::setfill('0') << checksum;
    return output.str();
}
bool sync_fd(int fd) { int result; do { result = metrics::io::fsync(fd); } while (result < 0 && errno == EINTR); return result == 0; }
std::optional<std::vector<std::byte>> read_record(const std::filesystem::path& path) {
    const int fd = metrics::io::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) return std::nullopt;
    struct stat status{};
    if (::fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) || status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) > kMaxRecordSize) {
        metrics::io::close(fd); return std::nullopt;
    }
    std::vector<std::byte> bytes;
    try { bytes.resize(static_cast<std::size_t>(status.st_size)); }
    catch (...) { metrics::io::close(fd); throw; }
    std::size_t offset{}; bool okay = true;
    while (offset < bytes.size()) {
        const auto count = metrics::io::read(fd, bytes.data() + offset, bytes.size() - offset);
        if (count > 0) offset += static_cast<std::size_t>(count);
        else if (count < 0 && errno == EINTR) continue;
        else { okay = false; break; }
    }
    std::byte extra{}; ssize_t count;
    do { count = metrics::io::read(fd, &extra, 1); } while (count < 0 && errno == EINTR);
    if (count != 0) okay = false;
    if (metrics::io::close(fd) != 0) okay = false;
    if (!okay) return std::nullopt;
    return bytes;
}
}

PlanningResult RepairPlanner::plan(const metadata::PlacementPlan& placement,
    const metadata::MembershipSnapshot& membership,
    const metadata::ReplicaCatalogSnapshot& catalog) const {
    if (placement.replication_factor == 0 ||
        placement.replication_factor > metadata::kMaxReplicationFactor ||
        !object_protocol::valid_object_id(placement.object_id) ||
        !storage::valid_generation_id(placement.generation_id))
        return {PlanningError::invalid_plan, {}};
    try { static_cast<void>(metadata::encode_membership(membership)); }
    catch (...) { return {PlanningError::invalid_membership, {}}; }
    try { static_cast<void>(metadata::encode_replica_catalog(catalog)); }
    catch (...) { return {PlanningError::invalid_catalog, {}}; }
    std::map<std::string, metadata::StorageNode, std::less<>> nodes;
    for (const auto& node : membership.nodes)
        if (!metadata::NodeRegistry::valid_node_id(node.node_id) ||
            !nodes.emplace(node.node_id, node).second) return {PlanningError::invalid_membership, {}};
    PlanningResult result; const auto health_version = evidence(membership);
    std::vector<std::uint32_t> chunk_indices;
    for (const auto& chunk : placement.chunks) {
        if (chunk.identity.object_id != placement.object_id ||
            chunk.identity.generation_id != placement.generation_id ||
            chunk.identity.chunk_index != chunk.chunk.index ||
            chunk.chunk.index >= storage::kMaxChunkCount ||
            std::find(chunk_indices.begin(), chunk_indices.end(), chunk.chunk.index) !=
                chunk_indices.end()) return {PlanningError::invalid_plan, {}};
        chunk_indices.push_back(chunk.chunk.index);
        const auto catalog_record = std::find_if(catalog.records.begin(), catalog.records.end(),
            [&](const auto& record) { return record.identity == chunk.identity; });
        if (catalog_record == catalog.records.end() || catalog_record->chunk != chunk.chunk)
            return {PlanningError::invalid_catalog, {}};
        std::vector<std::string> replicas;
        replicas.reserve(catalog_record->replicas.size());
        for (const auto& replica : catalog_record->replicas)
            replicas.push_back(replica.node_id);
        std::vector<std::string> sources;
        for (const auto& replica : catalog_record->replicas) {
            const auto found = nodes.find(replica.node_id);
            if (replica.state == metadata::CatalogReplicaState::present &&
                found != nodes.end() && healthy(found->second))
                sources.push_back(replica.node_id);
        }
        std::vector<std::string> targets;
        for (const auto& [id, node] : nodes)
            if (healthy(node) && !std::binary_search(replicas.begin(), replicas.end(), id))
                targets.push_back(id);
        const auto healthy_count = static_cast<std::uint32_t>(sources.size());
        const auto deficit = placement.replication_factor > healthy_count
            ? placement.replication_factor - healthy_count : 0U;
        for (std::uint32_t slot = 0; slot < deficit; ++slot) {
            RepairPlan repair;
            repair.plan_id = plan_id(placement.object_id, placement.generation_id,
                chunk.chunk.index, membership.version, catalog.catalog_version, slot);
            repair.object_id = placement.object_id;
            repair.generation_id = placement.generation_id;
            repair.chunk_index = chunk.chunk.index;
            repair.membership_version = membership.version;
            repair.health_evidence = health_version;
            repair.replication_factor = placement.replication_factor;
            repair.observed_replicas = replicas;
            repair.reason = (replicas.size() < placement.replication_factor ||
                std::any_of(catalog_record->replicas.begin(), catalog_record->replicas.end(),
                    [](const auto& replica) {
                        return replica.state == metadata::CatalogReplicaState::missing ||
                            replica.state == metadata::CatalogReplicaState::corrupt;
                    })) ? RepairReason::replica_missing
                        : RepairReason::replica_unavailable;
            repair.catalog_version = catalog.catalog_version;
            repair.observed_catalog_replicas = catalog_record->replicas;
            if (sources.empty()) {
                repair.reason = RepairReason::source_unavailable;
                repair.state = RepairPlanState::unrecoverable;
            } else {
                repair.source_node = sources.front();
                if (slot < targets.size()) repair.target_node = targets[slot];
                else repair.reason = RepairReason::no_eligible_target;
            }
            result.plans.push_back(std::move(repair));
        }
    }
    return result;
}

PlanValidation RepairPlanner::validate(const RepairPlan& repair,
    const metadata::PlacementPlan& placement, const metadata::MembershipSnapshot& membership,
    const metadata::ReplicaCatalogSnapshot& catalog) const {
    if (repair.membership_version != membership.version) return PlanValidation::stale_membership;
    if (repair.health_evidence != evidence(membership)) return PlanValidation::stale_health;
    if (repair.object_id != placement.object_id || repair.generation_id != placement.generation_id)
        return PlanValidation::stale_placement;
    const auto found = std::find_if(placement.chunks.begin(), placement.chunks.end(),
        [&](const auto& chunk) { return chunk.chunk.index == repair.chunk_index; });
    if (found == placement.chunks.end()) return PlanValidation::stale_placement;
    const metadata::ChunkIdentity expected_identity{
        repair.object_id, repair.generation_id, repair.chunk_index};
    if (found->identity != expected_identity || found->chunk.index != repair.chunk_index)
        return PlanValidation::stale_placement;
    const auto catalog_record = std::find_if(catalog.records.begin(), catalog.records.end(),
        [&](const auto& record) { return record.identity == found->identity; });
    if (catalog_record == catalog.records.end() || found->chunk != catalog_record->chunk)
        return PlanValidation::stale_catalog;
    std::vector<std::string> replicas;
    replicas.reserve(catalog_record->replicas.size());
    for (const auto& replica : catalog_record->replicas) replicas.push_back(replica.node_id);
    return repair.catalog_version == catalog.catalog_version &&
            repair.observed_catalog_replicas == catalog_record->replicas &&
            repair.observed_replicas == replicas
        ? PlanValidation::current : PlanValidation::stale_catalog;
}

std::vector<std::byte> encode_repair_plan(const RepairPlan& value) {
    if (!valid_plan(value)) throw std::invalid_argument("invalid repair plan");
    std::vector<std::byte> out; append(out, kMagic);
    append(out, value.execution_epoch ? std::uint16_t{4} : kVersion); append(out, std::uint16_t{0});
    append(out, static_cast<std::uint8_t>(value.state)); append(out, static_cast<std::uint8_t>(value.reason));
    append(out, std::uint16_t{0}); append_string(out, value.plan_id); append_string(out, value.object_id);
    append_string(out, value.generation_id); append_string(out, value.source_node);
    append_string(out, value.target_node); append(out, value.chunk_index); append(out, value.membership_version);
    append(out, value.health_evidence); append(out, value.replication_factor);
    append_string(out, value.execution_id);
    std::uint8_t flags = static_cast<std::uint8_t>(value.transfer_succeeded) |
        static_cast<std::uint8_t>(value.verification_succeeded << 1U) |
        static_cast<std::uint8_t>(value.publication_succeeded << 2U) |
        static_cast<std::uint8_t>(value.metadata_committed << 3U);
    append(out, flags); append(out, std::uint8_t{0});
    append(out, static_cast<std::uint16_t>(value.last_error));
    append(out, static_cast<std::uint32_t>(value.observed_replicas.size()));
    for (const auto& node : value.observed_replicas) append_string(out, node);
    append(out, value.catalog_version);
    append(out, static_cast<std::uint32_t>(value.observed_catalog_replicas.size()));
    for (const auto& replica : value.observed_catalog_replicas) {
        append_string(out, replica.node_id);
        append(out, static_cast<std::uint8_t>(replica.state));
        append(out, std::uint8_t{0});
        append(out, std::uint16_t{0});
        append(out, replica.integrity_checksum);
        append(out, replica.evidence_version);
    }
    if (value.execution_epoch) {
        const auto token = epoch::encode_token(*value.execution_epoch);
        append(out, static_cast<std::uint16_t>(token.size()));
        out.insert(out.end(), token.begin(), token.end());
    }
    append(out, atlasfs::integrity::crc32c(out));
    if (out.size() > kMaxRecordSize) throw std::length_error("repair plan too large");
    return out;
}

std::optional<RepairPlan> decode_repair_plan(std::span<const std::byte> encoded) {
    if (encoded.size() < 40 || encoded.size() > kMaxRecordSize) return std::nullopt;
    const auto body = encoded.first(encoded.size() - 4); std::size_t crc_at = body.size(); std::uint32_t crc{};
    if (!take(encoded, crc_at, crc) || crc != atlasfs::integrity::crc32c(body)) return std::nullopt;
    std::size_t at = 0; std::uint32_t magic{}, count{}; std::uint16_t version{}, reserved{};
    std::uint8_t state{}, reason{}, flags{}, reserved8{}; std::uint16_t execution_error{};
    RepairPlan value;
    if (!take(body, at, magic) || magic != kMagic || !take(body, at, version) ||
        (version != 1 && version != 2 && version != kVersion && version != 4) ||
        !take(body, at, reserved) || reserved != 0 || !take(body, at, state) || !take(body, at, reason) ||
        !take(body, at, reserved) || reserved != 0 ||
        !take_string(body, at, storage_node_protocol::kMaxOperationIdSize, value.plan_id) ||
        !take_string(body, at, object_protocol::kMaxObjectIdSize, value.object_id) ||
        !take_string(body, at, storage::kMaxGenerationIdSize, value.generation_id) ||
        !take_string(body, at, metadata::kMaxNodeIdSize, value.source_node, true) ||
        !take_string(body, at, metadata::kMaxNodeIdSize, value.target_node, true) ||
        !take(body, at, value.chunk_index) || !take(body, at, value.membership_version) ||
        !take(body, at, value.health_evidence) || !take(body, at, value.replication_factor))
        return std::nullopt;
    if (version >= 2 && (!take_string(body, at, storage_node_protocol::kMaxOperationIdSize,
            value.execution_id, true) || !take(body, at, flags) || flags > 15 ||
            !take(body, at, reserved8) || reserved8 != 0 ||
            !take(body, at, execution_error))) return std::nullopt;
    if (!take(body, at, count) || count > metadata::kMaxMembershipNodes) return std::nullopt;
    value.state = static_cast<RepairPlanState>(state); value.reason = static_cast<RepairReason>(reason);
    value.transfer_succeeded = (flags & 1U) != 0;
    value.verification_succeeded = (flags & 2U) != 0;
    value.publication_succeeded = (flags & 4U) != 0;
    value.metadata_committed = (flags & 8U) != 0;
    value.last_error = static_cast<RepairExecutionError>(execution_error);
    for (std::uint32_t i = 0; i < count; ++i) { std::string node;
        if (!take_string(body, at, metadata::kMaxNodeIdSize, node)) return std::nullopt;
        value.observed_replicas.push_back(std::move(node)); }
    if (version >= 3) {
        std::uint32_t catalog_count{};
        if (!take(body, at, value.catalog_version) || !take(body, at, catalog_count) ||
            catalog_count > metadata::kMaxCatalogReplicasPerChunk)
            return std::nullopt;
        value.observed_catalog_replicas.reserve(catalog_count);
        for (std::uint32_t i = 0; i < catalog_count; ++i) {
            metadata::CatalogReplica replica;
            std::uint8_t catalog_state{}, catalog_reserved8{};
            std::uint16_t catalog_reserved16{};
            if (!take_string(body, at, metadata::kMaxNodeIdSize, replica.node_id) ||
                !take(body, at, catalog_state) ||
                !take(body, at, catalog_reserved8) || catalog_reserved8 != 0 ||
                !take(body, at, catalog_reserved16) || catalog_reserved16 != 0 ||
                !take(body, at, replica.integrity_checksum) ||
                !take(body, at, replica.evidence_version)) return std::nullopt;
            replica.state = static_cast<metadata::CatalogReplicaState>(catalog_state);
            value.observed_catalog_replicas.push_back(std::move(replica));
        }
    }
    if (version == 4) {
        std::uint16_t length{};
        if (!take(body, at, length) || length > 302 || body.size() - at < length) return std::nullopt;
        value.execution_epoch = epoch::decode_token(body.subspan(at, length)); at += length;
        if (!value.execution_epoch || !epoch::valid_token(*value.execution_epoch)) return std::nullopt;
    }
    if ((version == 1 && static_cast<unsigned>(state) > 3U) ||
        at != body.size() || !valid_plan(value)) return std::nullopt;
    return value;
}

RepairPlanStore::RepairPlanStore(std::filesystem::path root, RepairFaultInjector fault)
    : directory_(std::move(root) / "coordinator" / "repairs"), fault_(std::move(fault)) {
    metrics::Timer startup(metrics::Counter::startup_calls, metrics::Counter::startup_ns);
    std::filesystem::create_directories(directory_); std::error_code error;
    for (const auto& entry : std::filesystem::directory_iterator(directory_, error)) {
        metrics::add(metrics::Counter::startup_entries);
        const auto name = entry.path().filename().string(); if (name.ends_with(".tmp")) continue;
        const auto bytes = read_record(entry.path());
        const auto value = bytes ? decode_repair_plan(*bytes) : std::nullopt;
        if (!value || value->plan_id != name || !plans_.emplace(name, *value).second)
            throw std::runtime_error("invalid durable repair plan");
    }
    if (error) throw std::system_error(error, "scan repair plans");
}
RepairStoreError RepairPlanStore::create(const RepairPlan& value) {
    if (durability_unknown_) return RepairStoreError::filesystem_error;
    if (!valid_plan(value)) return RepairStoreError::invalid_record;
    if (value.catalog_version == 0) return RepairStoreError::invalid_record;
    if (value.state != RepairPlanState::planned &&
        value.state != RepairPlanState::unrecoverable) return RepairStoreError::invalid_record;
    const auto found = plans_.find(value.plan_id);
    if (found != plans_.end()) return found->second == value ? RepairStoreError::none : RepairStoreError::conflict;
    return persist(value);
}
RepairStoreError RepairPlanStore::update(const RepairPlan& value) {
    if (durability_unknown_) return RepairStoreError::filesystem_error;
    if (!valid_plan(value)) return RepairStoreError::invalid_record;
    const auto found = plans_.find(value.plan_id);
    if (found == plans_.end()) return RepairStoreError::conflict;
    const auto& old = found->second;
    if ((old.execution_epoch && old.execution_epoch != value.execution_epoch) ||
        (!old.execution_epoch && value.execution_epoch &&
         !(old.state == RepairPlanState::approved && value.state == RepairPlanState::executing)))
        return RepairStoreError::conflict;
    if (old.object_id != value.object_id || old.generation_id != value.generation_id ||
        old.chunk_index != value.chunk_index || old.source_node != value.source_node ||
        old.target_node != value.target_node || old.membership_version != value.membership_version ||
        old.health_evidence != value.health_evidence ||
        old.replication_factor != value.replication_factor ||
        old.observed_replicas != value.observed_replicas || old.reason != value.reason ||
        old.catalog_version != value.catalog_version ||
        old.observed_catalog_replicas != value.observed_catalog_replicas)
        return RepairStoreError::conflict;
    const bool monotonic_results = (!old.transfer_succeeded || value.transfer_succeeded) &&
        (!old.verification_succeeded || value.verification_succeeded) &&
        (!old.publication_succeeded || value.publication_succeeded) &&
        (!old.metadata_committed || value.metadata_committed) &&
        (old.execution_id.empty() || old.execution_id == value.execution_id);
    const bool transition = monotonic_results && (
        (old.state == RepairPlanState::planned &&
            (value.state == RepairPlanState::approved || value.state == RepairPlanState::cancelled ||
             value.state == RepairPlanState::stale)) ||
        (old.state == RepairPlanState::approved &&
            (value.state == RepairPlanState::executing || value.state == RepairPlanState::cancelled ||
             value.state == RepairPlanState::stale)) ||
        (old.state == RepairPlanState::executing &&
            (value.state == RepairPlanState::completed || value.state == RepairPlanState::failed ||
             value.state == RepairPlanState::stale)) ||
        (old.state == RepairPlanState::failed && value.state == RepairPlanState::executing &&
            (old.last_error == RepairExecutionError::remote_result_unknown ||
             old.last_error == RepairExecutionError::catalog_durability_unknown ||
             old.last_error == RepairExecutionError::metadata_commit_failure)) ||
        (old.state == value.state &&
         (old.state == RepairPlanState::executing || old == value)));
    return transition ? persist(value) : RepairStoreError::conflict;
}
RepairStoreError RepairPlanStore::persist(const RepairPlan& value) {
    if (fault_ && fault_(RepairFaultPoint::record_creation)) return RepairStoreError::filesystem_error;
    const auto bytes = encode_repair_plan(value); const auto temporary = directory_ / (value.plan_id + ".tmp");
    const auto final = directory_ / value.plan_id;
    int fd = metrics::io::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return RepairStoreError::filesystem_error;
    bool okay = !(fault_ && fault_(RepairFaultPoint::record_write)); std::size_t offset = 0;
    while (okay && offset < bytes.size()) { const auto written = metrics::io::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written > 0) offset += static_cast<std::size_t>(written); else if (written < 0 && errno == EINTR) continue; else okay = false; }
    if (fault_ && fault_(RepairFaultPoint::record_sync)) okay = false;
    if (okay) okay = sync_fd(fd);
    if (metrics::io::close(fd) != 0) okay = false;
    if (!okay) return RepairStoreError::filesystem_error;
    if (fault_ && fault_(RepairFaultPoint::record_rename)) return RepairStoreError::filesystem_error;
    std::error_code error; metrics::io::rename(temporary, final, error);
    if (error) return RepairStoreError::filesystem_error;
    durability_unknown_ = true;
    if (fault_ && fault_(RepairFaultPoint::directory_sync)) return RepairStoreError::filesystem_error;
    const int directory = metrics::io::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0) return RepairStoreError::filesystem_error;
    const bool synced = sync_fd(directory); const bool closed = metrics::io::close(directory) == 0;
    if (!synced || !closed) return RepairStoreError::filesystem_error;
    plans_[value.plan_id] = value;
    durability_unknown_ = false;
    return RepairStoreError::none;
}
std::optional<RepairPlan> RepairPlanStore::get(std::string_view id) const {
    const auto found = plans_.find(id);
    return found == plans_.end() ? std::optional<RepairPlan>{}
                                 : std::optional<RepairPlan>{found->second};
}
std::vector<RepairPlan> RepairPlanStore::list() const {
    std::vector<RepairPlan> result;
    for (const auto& [id, value] : plans_) {
        static_cast<void>(id);
        result.push_back(value);
    }
    return result;
}
bool RepairPlanStore::durability_unknown() const noexcept { return durability_unknown_; }
const std::filesystem::path& RepairPlanStore::plans_directory() const noexcept { return directory_; }

RepairExecutor::RepairExecutor(RepairPlanStore& plans,
    metadata::DurableReplicaCatalog& catalog,
    std::vector<std::shared_ptr<replication::StorageNode>> nodes,
    RepairExecutionFaultInjector fault)
    : plans_(plans), catalog_(catalog), fault_(std::move(fault)) {
    for (auto& node : nodes) {
        if (!node || !nodes_.emplace(std::string(node->id()), node).second)
            throw std::invalid_argument("repair executor requires distinct nodes");
    }
}

RepairStoreError RepairExecutor::approve(std::string_view plan_id_value) {
    const std::scoped_lock lock(execution_mutex_);
    if (plans_.durability_unknown()) return RepairStoreError::filesystem_error;
    std::unique_ptr<epoch::Guard> guard;
    try { guard = catalog_.authorize_epoch(); } catch (const epoch::Error&) { return RepairStoreError::conflict; }
    auto plan = plans_.get(plan_id_value);
    if (!plan || plan->state != RepairPlanState::planned || plan->source_node.empty() ||
        plan->target_node.empty() || plan->catalog_version == 0)
        return RepairStoreError::conflict;
    plan->state = RepairPlanState::approved;
    return plans_.update(*plan);
}

RepairStoreError RepairExecutor::cancel(std::string_view plan_id_value) {
    const std::scoped_lock lock(execution_mutex_);
    if (plans_.durability_unknown()) return RepairStoreError::filesystem_error;
    std::unique_ptr<epoch::Guard> guard;
    try { guard = catalog_.authorize_epoch(); } catch (const epoch::Error&) { return RepairStoreError::conflict; }
    auto plan = plans_.get(plan_id_value);
    if (!plan || (plan->state != RepairPlanState::planned &&
                  plan->state != RepairPlanState::approved)) return RepairStoreError::conflict;
    plan->state = RepairPlanState::cancelled;
    return plans_.update(*plan);
}

RepairExecutionResult RepairExecutor::execute(std::string_view plan_id_value,
    metadata::PlacementPlan& placement, const metadata::MembershipSnapshot& membership) {
    const std::scoped_lock lock(execution_mutex_);
    return execute_locked(plan_id_value, placement, membership, false);
}

RepairExecutionResult RepairExecutor::reconcile(std::string_view plan_id_value,
    metadata::PlacementPlan& placement, const metadata::MembershipSnapshot& membership) {
    const std::scoped_lock lock(execution_mutex_);
    return execute_locked(plan_id_value, placement, membership, true);
}

RepairExecutionResult RepairExecutor::execute_locked(std::string_view plan_id_value,
    metadata::PlacementPlan& placement, const metadata::MembershipSnapshot& membership,
    bool recovering) {
    auto plan = plans_.get(plan_id_value);
    if (plans_.durability_unknown())
        return {RepairExecutionError::persistence_failure, plan ? plan->state : RepairPlanState::planned};
    if (!plan) return {RepairExecutionError::plan_not_found, RepairPlanState::planned};
    std::unique_ptr<epoch::Guard> epoch_guard;
    try { epoch_guard = catalog_.authorize_epoch(); } catch (const epoch::Error& error) {
        return {error.status() == epoch::Status::unresolved ? RepairExecutionError::epoch_unresolved :
            RepairExecutionError::stale_epoch, plan->state};
    }
    if ((recovering && plan->execution_epoch != catalog_.epoch_session()) ||
        (plan->execution_epoch && plan->execution_epoch != catalog_.epoch_session()))
        return {RepairExecutionError::stale_epoch, plan->state};
    if (plan->state == RepairPlanState::completed)
        return {RepairExecutionError::already_completed, plan->state};
    if (plan->state == RepairPlanState::cancelled)
        return {RepairExecutionError::cancelled, plan->state};
    const bool recoverable_failure = plan->state == RepairPlanState::failed &&
        (plan->last_error == RepairExecutionError::remote_result_unknown ||
         plan->last_error == RepairExecutionError::catalog_durability_unknown ||
         plan->last_error == RepairExecutionError::metadata_commit_failure);
    if (recovering) {
        if (plan->state != RepairPlanState::executing && !recoverable_failure)
            return {RepairExecutionError::not_reconcilable, plan->state};
    } else if (plan->state != RepairPlanState::approved) {
        return {RepairExecutionError::not_approved, plan->state};
    }
    // A visible post-rename snapshot is not confirmed durable in a poisoned
    // catalog process. Only restart can establish which snapshot survived.
    if (catalog_.durability_unknown())
        return {RepairExecutionError::catalog_durability_unknown, plan->state};
    if (recoverable_failure) {
        plan->state = RepairPlanState::executing;
        plan->last_error = RepairExecutionError::none;
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure, RepairPlanState::failed};
    }

    const metadata::ChunkIdentity identity{plan->object_id, plan->generation_id,
                                           plan->chunk_index};
    const auto chunk_it = std::find_if(placement.chunks.begin(), placement.chunks.end(),
        [&](const auto& chunk) { return chunk.chunk.index == plan->chunk_index; });
    if (chunk_it == placement.chunks.end()) {
        plan->state = RepairPlanState::stale;
        plan->last_error = RepairExecutionError::stale;
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure, plan->state};
        return {RepairExecutionError::stale, plan->state};
    }
    const auto member = [&](std::string_view id) -> const metadata::StorageNode* {
        const auto found = std::find_if(membership.nodes.begin(), membership.nodes.end(),
            [&](const auto& node) { return node.node_id == id; });
        return found == membership.nodes.end() ? nullptr : &*found;
    };
    const auto eligible = [](const metadata::StorageNode* node) {
        return node && node->state == metadata::NodeState::active &&
            node->health == metadata::NodeHealthState::healthy && node->health_current;
    };
    auto desired_replicas = plan->observed_catalog_replicas;
    if (desired_replicas.size() >= plan->replication_factor) {
        const auto replace = std::find_if(desired_replicas.begin(), desired_replicas.end(),
            [&](const auto& replica) {
                return replica.state != metadata::CatalogReplicaState::present ||
                    !eligible(member(replica.node_id));
            });
        if (replace != desired_replicas.end()) desired_replicas.erase(replace);
    }
    desired_replicas.push_back({plan->target_node,
        metadata::CatalogReplicaState::present, chunk_it->chunk.checksum,
        membership.version});
    std::sort(desired_replicas.begin(), desired_replicas.end(),
        [](const auto& left, const auto& right) { return left.node_id < right.node_id; });

    auto catalog_snapshot = catalog_.snapshot();
    auto validation = planner_.validate(*plan, placement, membership, catalog_snapshot);
    if (recovering && validation == PlanValidation::stale_catalog) {
        auto evidence_snapshot = catalog_snapshot;
        evidence_snapshot.catalog_version = plan->catalog_version;
        const metadata::ChunkIdentity planned_identity{
            plan->object_id, plan->generation_id, plan->chunk_index};
        const auto record = std::find_if(evidence_snapshot.records.begin(),
            evidence_snapshot.records.end(), [&](auto& value) {
                return value.identity == planned_identity;
            });
        if (record != evidence_snapshot.records.end()) {
            record->catalog_version = plan->catalog_version;
            record->replicas = plan->observed_catalog_replicas;
            if (planner_.validate(*plan, placement, membership, evidence_snapshot) ==
                    PlanValidation::current) {
                const auto current = std::find_if(catalog_snapshot.records.begin(),
                    catalog_snapshot.records.end(), [&](const auto& value) {
                        return value.identity == identity;
                    });
                if (plan->catalog_version != std::numeric_limits<std::uint64_t>::max() &&
                    catalog_snapshot.catalog_version == plan->catalog_version + 1U &&
                    current != catalog_snapshot.records.end() &&
                    current->catalog_version == plan->catalog_version + 1U &&
                    current->replicas == desired_replicas) {
                    if (!plan->publication_succeeded || !plan->verification_succeeded)
                        return {RepairExecutionError::not_reconcilable, plan->state};
                    const auto target = nodes_.find(plan->target_node);
                    if (target == nodes_.end())
                        return {RepairExecutionError::target_unavailable, plan->state};
                    const auto visible = target->second->get_chunk(plan->object_id,
                        plan->generation_id, plan->chunk_index);
                    if (visible.error != storage::StoreError::none)
                        return {RepairExecutionError::target_unavailable, plan->state};
                    storage::ObjectManifest expected{placement.object_size,
                        placement.chunk_size, placement.object_checksum,
                        placement.generation_id, {}};
                    for (const auto& chunk : placement.chunks)
                        expected.chunks.push_back(chunk.chunk);
                    std::sort(expected.chunks.begin(), expected.chunks.end(),
                        [](const auto& left, const auto& right) {
                            return left.index < right.index;
                        });
                    if (visible.manifest != expected || visible.chunk != chunk_it->chunk ||
                        visible.data.size() != chunk_it->chunk.length ||
                        integrity::crc32c(visible.data) != chunk_it->chunk.checksum)
                        return {RepairExecutionError::verification_failure, plan->state};
                    if (catalog_.durability_unknown())
                        return {RepairExecutionError::catalog_durability_unknown, plan->state};
                    if (catalog_.snapshot() != catalog_snapshot)
                        return {RepairExecutionError::conflicting_repair, plan->state};
                    auto& placement_replicas = placement.chunks[static_cast<std::size_t>(
                        std::distance(placement.chunks.begin(), chunk_it))].replica_node_ids;
                    placement_replicas.clear();
                    for (const auto& replica : desired_replicas)
                        placement_replicas.push_back(replica.node_id);
                    plan->metadata_committed = true;
                    plan->state = RepairPlanState::completed;
                    plan->last_error = RepairExecutionError::none;
                    if (plans_.update(*plan) != RepairStoreError::none)
                        return {RepairExecutionError::persistence_failure,
                                RepairPlanState::executing};
                    return {RepairExecutionError::none, plan->state};
                }
            }
        }
    }
    if (validation != PlanValidation::current) {
        plan->state = RepairPlanState::stale;
        plan->last_error = RepairExecutionError::stale;
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure, RepairPlanState::approved};
        return {RepairExecutionError::stale, plan->state};
    }
    auto stale = [&](RepairExecutionError error) {
        plan->state = RepairPlanState::stale;
        plan->last_error = error;
        if (plans_.update(*plan) != RepairStoreError::none)
            return RepairExecutionResult{RepairExecutionError::persistence_failure,
                                         RepairPlanState::approved};
        return RepairExecutionResult{error, plan->state};
    };
    if (!eligible(member(plan->source_node)))
        return stale(RepairExecutionError::source_unavailable);
    if (!eligible(member(plan->target_node)))
        return stale(RepairExecutionError::target_unavailable);
    const auto source = nodes_.find(plan->source_node);
    const auto target = nodes_.find(plan->target_node);
    if (source == nodes_.end()) return stale(RepairExecutionError::source_unavailable);
    if (target == nodes_.end()) return stale(RepairExecutionError::target_unavailable);
    if (target->second->epoch_token() && target->second->epoch_token() != catalog_.epoch_session())
        return stale(RepairExecutionError::stale_epoch);

    const auto source_chunk = source->second->get_chunk(
        plan->object_id, plan->generation_id, plan->chunk_index);
    if (source_chunk.error == storage::StoreError::not_found)
        return stale(RepairExecutionError::source_chunk_missing);
    if (source_chunk.error != storage::StoreError::none)
        return stale(RepairExecutionError::source_unavailable);
    storage::ObjectManifest expected_manifest{placement.object_size, placement.chunk_size,
        placement.object_checksum, placement.generation_id, {}};
    expected_manifest.chunks.reserve(placement.chunks.size());
    for (const auto& chunk : placement.chunks) expected_manifest.chunks.push_back(chunk.chunk);
    std::sort(expected_manifest.chunks.begin(), expected_manifest.chunks.end(),
        [](const auto& left, const auto& right) { return left.index < right.index; });
    if (source_chunk.manifest != expected_manifest ||
        source_chunk.manifest.generation_id != plan->generation_id ||
        source_chunk.chunk != chunk_it->chunk ||
        source_chunk.chunk.index != plan->chunk_index)
        return stale(RepairExecutionError::chunk_identity_mismatch);
    if (source_chunk.data.size() != source_chunk.chunk.length ||
        integrity::crc32c(source_chunk.data) != source_chunk.chunk.checksum)
        return stale(RepairExecutionError::integrity_failure);

    const auto existing = target->second->get_chunk(
        plan->object_id, plan->generation_id, plan->chunk_index);
    if (existing.error == storage::StoreError::none) {
        if (existing.manifest != expected_manifest || existing.chunk != source_chunk.chunk ||
            existing.data != source_chunk.data)
            return stale(RepairExecutionError::integrity_failure);
    } else if (existing.error != storage::StoreError::not_found) {
        return stale(RepairExecutionError::target_unavailable);
    }

    if (!recovering) {
        plan->state = RepairPlanState::executing;
        plan->execution_id = execution_id(plan->plan_id);
        plan->execution_epoch = catalog_.epoch_session();
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure, RepairPlanState::approved};
    }
    if (fault_ && fault_(RepairExecutionFaultPoint::after_executing_persist))
        return {RepairExecutionError::recovery_interrupted, RepairPlanState::executing};

    // Final coordinator-local evidence check is intentionally adjacent to the
    // transfer. The mutex serializes competing executions in this executor.
    catalog_snapshot = catalog_.snapshot();
    if (planner_.validate(*plan, placement, membership, catalog_snapshot) !=
            PlanValidation::current) {
        plan->state = RepairPlanState::stale;
        plan->last_error = RepairExecutionError::stale;
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure, RepairPlanState::executing};
        return {RepairExecutionError::stale, plan->state};
    }

    // Replay every interrupted execution through the publisher, including when
    // bytes are visible. Presence cannot resolve an unknown durable journal.
    if (existing.error != storage::StoreError::none || recovering) {
        const auto published = [&] {
            metrics::Timer publication(metrics::Counter::publication_calls, metrics::Counter::publication_ns);
            return target->second->publish_repair(
            plan->plan_id, plan->execution_id, plan->target_node, plan->object_id,
            source_chunk.manifest, source_chunk.chunk, source_chunk.data);
        }();
        if (published.status != replication::RepairPublishStatus::published &&
            published.status != replication::RepairPublishStatus::already_present_valid) {
            plan->state = RepairPlanState::failed;
            switch (published.status) {
                case replication::RepairPublishStatus::result_unknown:
                    plan->last_error = RepairExecutionError::remote_result_unknown;
                    break;
                case replication::RepairPublishStatus::stale_epoch:
                    plan->last_error = RepairExecutionError::stale_epoch;
                    break;
                case replication::RepairPublishStatus::epoch_unresolved:
                    plan->last_error = RepairExecutionError::epoch_unresolved;
                    break;
                case replication::RepairPublishStatus::identity_mismatch:
                    plan->last_error = RepairExecutionError::remote_identity_mismatch;
                    break;
                case replication::RepairPublishStatus::connection_failure:
                    plan->last_error = RepairExecutionError::remote_connection_failure;
                    break;
                case replication::RepairPublishStatus::timeout:
                    plan->last_error = RepairExecutionError::remote_timeout;
                    break;
                case replication::RepairPublishStatus::malformed_response:
                    plan->last_error = RepairExecutionError::remote_protocol_error;
                    break;
                case replication::RepairPublishStatus::checksum_mismatch:
                    plan->last_error = RepairExecutionError::verification_failure;
                    break;
                case replication::RepairPublishStatus::publication_failure:
                    plan->last_error = RepairExecutionError::target_publication_failure;
                    break;
                case replication::RepairPublishStatus::target_rejected:
                case replication::RepairPublishStatus::invalid_manifest:
                    plan->last_error = RepairExecutionError::remote_target_rejected;
                    break;
                case replication::RepairPublishStatus::published:
                case replication::RepairPublishStatus::already_present_valid:
                    break;
            }
            const auto error = plan->last_error;
            if (plans_.update(*plan) != RepairStoreError::none)
                return {RepairExecutionError::persistence_failure, RepairPlanState::executing};
            return {error, plan->state};
        }
    }
    plan->transfer_succeeded = true;
    plan->publication_succeeded = true;
    std::optional<metrics::Timer> verification;
    verification.emplace(metrics::Counter::verification_calls, metrics::Counter::verification_ns);
    const auto verified = target->second->get_chunk(plan->object_id, plan->generation_id, plan->chunk_index);
    if (verified.error != storage::StoreError::none ||
        verified.manifest != expected_manifest || verified.chunk != source_chunk.chunk ||
        verified.data != source_chunk.data ||
        integrity::crc32c(verified.data) != source_chunk.chunk.checksum) {
        plan->state = RepairPlanState::failed;
        plan->last_error = RepairExecutionError::verification_failure;
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure, RepairPlanState::executing};
        return {RepairExecutionError::verification_failure, plan->state};
    }
    plan->verification_succeeded = true;
    verification.reset();
    if (plans_.update(*plan) != RepairStoreError::none)
        return {RepairExecutionError::persistence_failure, plan->state};
    if (fault_ && fault_(RepairExecutionFaultPoint::after_publication_persist))
        return {RepairExecutionError::recovery_interrupted, RepairPlanState::executing};

    const auto committed = [&] {
        metrics::Timer cas(metrics::Counter::cas_calls, metrics::Counter::cas_ns);
        return catalog_.compare_exchange({identity, plan->catalog_version,
            plan->observed_catalog_replicas, desired_replicas});
    }();
    if (committed.status != metadata::ReplicaCatalogCasStatus::committed) {
        if (committed.status == metadata::ReplicaCatalogCasStatus::conflict ||
            committed.status == metadata::ReplicaCatalogCasStatus::no_change ||
            committed.status == metadata::ReplicaCatalogCasStatus::not_found) {
            plan->state = RepairPlanState::stale;
            plan->last_error = RepairExecutionError::conflicting_repair;
        } else {
            plan->state = RepairPlanState::failed;
            plan->last_error = committed.status ==
                    metadata::ReplicaCatalogCasStatus::durability_unknown
                ? RepairExecutionError::catalog_durability_unknown
                : RepairExecutionError::metadata_commit_failure;
        }
        const auto error = plan->last_error;
        if (plans_.update(*plan) != RepairStoreError::none)
            return {RepairExecutionError::persistence_failure,
                    RepairPlanState::executing};
        return {error, plan->state};
    }
    if (fault_ && fault_(RepairExecutionFaultPoint::after_catalog_commit))
        return {RepairExecutionError::recovery_interrupted, RepairPlanState::executing};
    auto& placement_replicas = placement.chunks[static_cast<std::size_t>(
        std::distance(placement.chunks.begin(), chunk_it))].replica_node_ids;
    placement_replicas.clear();
    for (const auto& replica : desired_replicas)
        placement_replicas.push_back(replica.node_id);
    plan->metadata_committed = true;
    plan->state = RepairPlanState::completed;
    metrics::Timer completion(metrics::Counter::completion_calls, metrics::Counter::completion_ns);
    if (plans_.update(*plan) != RepairStoreError::none)
        return {RepairExecutionError::persistence_failure, RepairPlanState::executing};
    return {RepairExecutionError::none, plan->state};
}

}  // namespace atlasfs::repair
