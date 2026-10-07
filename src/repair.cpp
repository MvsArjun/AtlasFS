#include "atlasfs/repair.hpp"

#include "atlasfs/checksum.hpp"
#include "atlasfs/object_protocol.hpp"
#include "atlasfs/storage_node_protocol.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <unistd.h>

namespace atlasfs::repair {
namespace {
constexpr std::uint32_t kMagic = 0x4152504cU; // ARPL
constexpr std::uint16_t kVersion = 1;
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
    return value >= RepairPlanState::planned && value <= RepairPlanState::stale;
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
        !valid_reason(value.reason) || !valid_state(value.state)) return false;
    if (!value.source_node.empty() && !atlasfs::metadata::NodeRegistry::valid_node_id(value.source_node)) return false;
    if (!value.target_node.empty() && !atlasfs::metadata::NodeRegistry::valid_node_id(value.target_node)) return false;
    std::string previous;
    for (const auto& node : value.observed_replicas) {
        if (!atlasfs::metadata::NodeRegistry::valid_node_id(node) ||
            (!previous.empty() && node <= previous)) return false;
        previous = node;
    }
    return true;
}
std::string plan_id(std::string_view object, std::string_view generation, std::uint32_t chunk,
                    std::uint64_t membership, std::uint32_t slot) {
    std::vector<std::byte> bytes; append_string(bytes, object); append_string(bytes, generation);
    append(bytes, chunk); append(bytes, membership); append(bytes, slot);
    std::ostringstream output; output << "repair-" << std::hex << std::setw(8) << std::setfill('0')
                                      << atlasfs::integrity::crc32c(bytes);
    return output.str();
}
bool sync_fd(int fd) { int result; do { result = ::fsync(fd); } while (result < 0 && errno == EINTR); return result == 0; }
}

PlanningResult RepairPlanner::plan(const metadata::PlacementPlan& placement,
                                   const metadata::MembershipSnapshot& membership) const {
    if (placement.replication_factor == 0 ||
        placement.replication_factor > metadata::kMaxReplicationFactor ||
        !object_protocol::valid_object_id(placement.object_id) || placement.generation_id.empty())
        return {PlanningError::invalid_plan, {}};
    try { static_cast<void>(metadata::encode_membership(membership)); }
    catch (...) { return {PlanningError::invalid_membership, {}}; }
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
        auto replicas = chunk.replica_node_ids; std::sort(replicas.begin(), replicas.end());
        if (std::adjacent_find(replicas.begin(), replicas.end()) != replicas.end())
            return {PlanningError::invalid_plan, {}};
        std::vector<std::string> sources;
        for (const auto& replica : replicas) {
            const auto found = nodes.find(replica);
            if (found != nodes.end() && healthy(found->second)) sources.push_back(replica);
        }
        std::vector<std::string> targets;
        for (const auto& [id, node] : nodes)
            if (healthy(node) && !std::binary_search(replicas.begin(), replicas.end(), id))
                targets.push_back(id);
        const auto healthy_count = static_cast<std::uint32_t>(sources.size());
        const auto deficit = placement.replication_factor > healthy_count
            ? placement.replication_factor - healthy_count : 0U;
        for (std::uint32_t slot = 0; slot < deficit; ++slot) {
            RepairPlan repair{plan_id(placement.object_id, placement.generation_id,
                    chunk.chunk.index, membership.version, slot), placement.object_id,
                placement.generation_id, chunk.chunk.index, {}, {}, membership.version,
                health_version, placement.replication_factor, replicas,
                replicas.size() < placement.replication_factor ? RepairReason::replica_missing
                                                               : RepairReason::replica_unavailable,
                RepairPlanState::planned};
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
    const metadata::PlacementPlan& placement, const metadata::MembershipSnapshot& membership) const {
    if (repair.membership_version != membership.version) return PlanValidation::stale_membership;
    if (repair.health_evidence != evidence(membership)) return PlanValidation::stale_health;
    if (repair.object_id != placement.object_id || repair.generation_id != placement.generation_id)
        return PlanValidation::stale_placement;
    const auto found = std::find_if(placement.chunks.begin(), placement.chunks.end(),
        [&](const auto& chunk) { return chunk.chunk.index == repair.chunk_index; });
    if (found == placement.chunks.end()) return PlanValidation::stale_placement;
    auto replicas = found->replica_node_ids; std::sort(replicas.begin(), replicas.end());
    return replicas == repair.observed_replicas ? PlanValidation::current
                                                : PlanValidation::stale_placement;
}

std::vector<std::byte> encode_repair_plan(const RepairPlan& value) {
    if (!valid_plan(value)) throw std::invalid_argument("invalid repair plan");
    std::vector<std::byte> out; append(out, kMagic); append(out, kVersion); append(out, std::uint16_t{0});
    append(out, static_cast<std::uint8_t>(value.state)); append(out, static_cast<std::uint8_t>(value.reason));
    append(out, std::uint16_t{0}); append_string(out, value.plan_id); append_string(out, value.object_id);
    append_string(out, value.generation_id); append_string(out, value.source_node);
    append_string(out, value.target_node); append(out, value.chunk_index); append(out, value.membership_version);
    append(out, value.health_evidence); append(out, value.replication_factor);
    append(out, static_cast<std::uint32_t>(value.observed_replicas.size()));
    for (const auto& node : value.observed_replicas) append_string(out, node);
    append(out, atlasfs::integrity::crc32c(out));
    if (out.size() > kMaxRecordSize) throw std::length_error("repair plan too large");
    return out;
}

std::optional<RepairPlan> decode_repair_plan(std::span<const std::byte> encoded) {
    if (encoded.size() < 40 || encoded.size() > kMaxRecordSize) return std::nullopt;
    const auto body = encoded.first(encoded.size() - 4); std::size_t crc_at = body.size(); std::uint32_t crc{};
    if (!take(encoded, crc_at, crc) || crc != atlasfs::integrity::crc32c(body)) return std::nullopt;
    std::size_t at = 0; std::uint32_t magic{}, count{}; std::uint16_t version{}, reserved{};
    std::uint8_t state{}, reason{}; RepairPlan value;
    if (!take(body, at, magic) || magic != kMagic || !take(body, at, version) || version != kVersion ||
        !take(body, at, reserved) || reserved != 0 || !take(body, at, state) || !take(body, at, reason) ||
        !take(body, at, reserved) || reserved != 0 ||
        !take_string(body, at, storage_node_protocol::kMaxOperationIdSize, value.plan_id) ||
        !take_string(body, at, object_protocol::kMaxObjectIdSize, value.object_id) ||
        !take_string(body, at, storage::kMaxGenerationIdSize, value.generation_id) ||
        !take_string(body, at, metadata::kMaxNodeIdSize, value.source_node, true) ||
        !take_string(body, at, metadata::kMaxNodeIdSize, value.target_node, true) ||
        !take(body, at, value.chunk_index) || !take(body, at, value.membership_version) ||
        !take(body, at, value.health_evidence) || !take(body, at, value.replication_factor) ||
        !take(body, at, count) || count > metadata::kMaxMembershipNodes) return std::nullopt;
    value.state = static_cast<RepairPlanState>(state); value.reason = static_cast<RepairReason>(reason);
    for (std::uint32_t i = 0; i < count; ++i) { std::string node;
        if (!take_string(body, at, metadata::kMaxNodeIdSize, node)) return std::nullopt;
        value.observed_replicas.push_back(std::move(node)); }
    if (at != body.size() || !valid_plan(value)) return std::nullopt;
    return value;
}

RepairPlanStore::RepairPlanStore(std::filesystem::path root, RepairFaultInjector fault)
    : directory_(std::move(root) / "coordinator" / "repairs"), fault_(std::move(fault)) {
    std::filesystem::create_directories(directory_); std::error_code error;
    for (const auto& entry : std::filesystem::directory_iterator(directory_, error)) {
        const auto name = entry.path().filename().string(); if (name.ends_with(".tmp")) continue;
        std::ifstream input(entry.path(), std::ios::binary); std::vector<std::byte> bytes;
        for (char c; input.get(c);) bytes.push_back(static_cast<std::byte>(c));
        const auto value = decode_repair_plan(bytes);
        if (!value || value->plan_id != name || !plans_.emplace(name, *value).second)
            throw std::runtime_error("invalid durable repair plan");
    }
    if (error) throw std::system_error(error, "scan repair plans");
}
RepairStoreError RepairPlanStore::create(const RepairPlan& value) {
    if (!valid_plan(value)) return RepairStoreError::invalid_record;
    const auto found = plans_.find(value.plan_id);
    if (found != plans_.end()) return found->second == value ? RepairStoreError::none : RepairStoreError::conflict;
    if (fault_ && fault_(RepairFaultPoint::record_creation)) return RepairStoreError::filesystem_error;
    const auto bytes = encode_repair_plan(value); const auto temporary = directory_ / (value.plan_id + ".tmp");
    const auto final = directory_ / value.plan_id;
    int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return RepairStoreError::filesystem_error;
    bool okay = !(fault_ && fault_(RepairFaultPoint::record_write)); std::size_t offset = 0;
    while (okay && offset < bytes.size()) { const auto written = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written > 0) offset += static_cast<std::size_t>(written); else if (written < 0 && errno == EINTR) continue; else okay = false; }
    if (fault_ && fault_(RepairFaultPoint::record_sync)) okay = false;
    if (okay) okay = sync_fd(fd);
    if (::close(fd) != 0) okay = false;
    if (!okay) return RepairStoreError::filesystem_error;
    if (fault_ && fault_(RepairFaultPoint::record_rename)) return RepairStoreError::filesystem_error;
    std::error_code error; std::filesystem::rename(temporary, final, error);
    if (error) return RepairStoreError::filesystem_error;
    if (fault_ && fault_(RepairFaultPoint::directory_sync)) return RepairStoreError::filesystem_error;
    const int directory = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0) return RepairStoreError::filesystem_error;
    const bool synced = sync_fd(directory); const bool closed = ::close(directory) == 0;
    if (!synced || !closed) return RepairStoreError::filesystem_error;
    plans_.emplace(value.plan_id, value); return RepairStoreError::none;
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
const std::filesystem::path& RepairPlanStore::plans_directory() const noexcept { return directory_; }

}  // namespace atlasfs::repair
