#include "atlasfs/membership.hpp"
#include "atlasfs/metrics.hpp"

#include "atlasfs/checksum.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <unistd.h>

namespace atlasfs::metadata {
namespace {
constexpr std::uint32_t kMagic = 0x414d4252U; // AMBR
constexpr std::uint16_t kVersion = 1;
constexpr std::size_t kMaxRecordSize = 2U * 1024U * 1024U;

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
    const auto* bytes = reinterpret_cast<const std::byte*>(value.data());
    out.insert(out.end(), bytes, bytes + value.size());
}
bool take_string(std::span<const std::byte> in, std::size_t& at, std::size_t limit,
                 std::string& value) {
    std::uint16_t size{};
    if (!take(in, at, size) || size == 0 || size > limit || at > in.size() ||
        in.size() - at < size) return false;
    value.assign(reinterpret_cast<const char*>(in.data() + at), size); at += size; return true;
}
bool valid_state(NodeState state) {
    return state >= NodeState::active && state <= NodeState::unavailable;
}
bool valid_health(NodeHealthState state) {
    return state >= NodeHealthState::healthy && state <= NodeHealthState::unavailable;
}
bool valid_snapshot(const MembershipSnapshot& value) {
    if (value.nodes.size() > kMaxMembershipNodes) return false;
    std::string previous;
    for (const auto& node : value.nodes) {
        if (!NodeRegistry::valid_node_id(node.node_id) ||
            !NodeRegistry::valid_endpoint(node.endpoint) || !valid_state(node.state) ||
            !valid_health(node.health) || (!previous.empty() && node.node_id <= previous))
            return false;
        previous = node.node_id;
    }
    return true;
}
MembershipError map_error(RegistryError error) {
    switch (error) {
        case RegistryError::none: return MembershipError::none;
        case RegistryError::invalid_node: return MembershipError::invalid_node;
        case RegistryError::duplicate_node: return MembershipError::duplicate_node;
        case RegistryError::unknown_node: return MembershipError::unknown_node;
    }
    return MembershipError::invalid_node;
}
bool sync_fd(int fd) { int result; do { result = metrics::io::fsync(fd); } while (result < 0 && errno == EINTR); return result == 0; }
}

std::vector<std::byte> encode_membership(const MembershipSnapshot& snapshot) {
    if (!valid_snapshot(snapshot)) throw std::invalid_argument("invalid membership snapshot");
    std::vector<std::byte> out;
    append(out, kMagic); append(out, kVersion); append(out, std::uint16_t{0});
    append(out, snapshot.version); append(out, static_cast<std::uint32_t>(snapshot.nodes.size()));
    for (const auto& node : snapshot.nodes) {
        append_string(out, node.node_id); append_string(out, node.endpoint.host);
        append(out, node.endpoint.port); append(out, static_cast<std::uint8_t>(node.state));
        append(out, static_cast<std::uint8_t>(node.health)); append(out, std::uint16_t{0});
        append(out, node.consecutive_failures);
    }
    append(out, integrity::crc32c(out));
    if (out.size() > kMaxRecordSize) throw std::length_error("membership record too large");
    return out;
}

std::optional<MembershipSnapshot> decode_membership(std::span<const std::byte> encoded) {
    if (encoded.size() < 24 || encoded.size() > kMaxRecordSize) return std::nullopt;
    const auto body = encoded.first(encoded.size() - 4); std::size_t crc_at = body.size();
    std::uint32_t crc{};
    if (!take(encoded, crc_at, crc) || crc != integrity::crc32c(body)) return std::nullopt;
    std::size_t at = 0; std::uint32_t magic{}, count{}; std::uint16_t version{}, reserved{};
    MembershipSnapshot result;
    if (!take(body, at, magic) || magic != kMagic || !take(body, at, version) ||
        version != kVersion || !take(body, at, reserved) || reserved != 0 ||
        !take(body, at, result.version) || !take(body, at, count) ||
        count > kMaxMembershipNodes) return std::nullopt;
    result.nodes.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        StorageNode node; std::uint8_t state{}, health{}; std::uint32_t failures{};
        if (!take_string(body, at, kMaxNodeIdSize, node.node_id) ||
            !take_string(body, at, kMaxHostSize, node.endpoint.host) ||
            !take(body, at, node.endpoint.port) || !take(body, at, state) ||
            !take(body, at, health) || !take(body, at, reserved) || reserved != 0 ||
            !take(body, at, failures)) return std::nullopt;
        node.state = static_cast<NodeState>(state); node.health = static_cast<NodeHealthState>(health);
        node.consecutive_failures = failures; node.health_current = false;
        result.nodes.push_back(std::move(node));
    }
    if (at != body.size() || !valid_snapshot(result)) return std::nullopt;
    return result;
}

DurableMembership::DurableMembership(std::filesystem::path root, FailureDetectorConfig config,
    MembershipFaultInjector fault_injector)
    : directory_(std::move(root) / "coordinator" / "membership"),
      record_path_(directory_ / "registry"), config_(config),
      fault_injector_(std::move(fault_injector)) {
    if (config_.failure_threshold == 0 || config_.failure_threshold > kMaxFailureThreshold)
        throw std::invalid_argument("invalid failure threshold");
    std::filesystem::create_directories(directory_);
    if (!std::filesystem::exists(record_path_)) return;
    std::ifstream input(record_path_, std::ios::binary); std::vector<std::byte> bytes;
    for (char value; input.get(value);) bytes.push_back(static_cast<std::byte>(value));
    metrics::io::observed_read(bytes.size());
    const auto snapshot = decode_membership(bytes);
    if (!snapshot || registry_.restore(snapshot->nodes, snapshot->version) != RegistryError::none)
        throw std::runtime_error("invalid durable membership record");
}

MembershipError DurableMembership::publish(const NodeRegistry& candidate) {
    const MembershipSnapshot snapshot{candidate.version(), candidate.list()};
    std::vector<std::byte> bytes;
    try { bytes = encode_membership(snapshot); } catch (...) { return MembershipError::invalid_node; }
    if (fault_injector_ && fault_injector_(MembershipFaultPoint::record_creation))
        return MembershipError::persistence_error;
    const auto temporary = directory_ / "registry.tmp";
    int fd = metrics::io::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return MembershipError::persistence_error;
    bool okay = !(fault_injector_ && fault_injector_(MembershipFaultPoint::record_write));
    std::size_t offset = 0;
    while (okay && offset < bytes.size()) {
        const auto written = metrics::io::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written > 0) offset += static_cast<std::size_t>(written);
        else if (written < 0 && errno == EINTR) continue; else okay = false;
    }
    if (fault_injector_ && fault_injector_(MembershipFaultPoint::record_sync)) okay = false;
    if (okay) okay = sync_fd(fd);
    if (metrics::io::close(fd) != 0) okay = false;
    if (!okay) return MembershipError::persistence_error;
    if (fault_injector_ && fault_injector_(MembershipFaultPoint::record_rename))
        return MembershipError::persistence_error;
    std::error_code error; metrics::io::rename(temporary, record_path_, error);
    if (error) return MembershipError::persistence_error;
    if (fault_injector_ && fault_injector_(MembershipFaultPoint::directory_sync))
        return MembershipError::persistence_error;
    const int directory = metrics::io::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0) return MembershipError::persistence_error;
    const bool synced = sync_fd(directory); const bool closed = metrics::io::close(directory) == 0;
    return synced && closed ? MembershipError::none : MembershipError::persistence_error;
}

MembershipError DurableMembership::add(StorageNode node) {
    NodeRegistry candidate = registry_; const auto error = candidate.add(std::move(node));
    if (error != RegistryError::none) return map_error(error);
    const auto persisted = publish(candidate); if (persisted != MembershipError::none) return persisted;
    registry_ = std::move(candidate); return MembershipError::none;
}
MembershipError DurableMembership::remove(std::string_view node_id) {
    NodeRegistry candidate = registry_; const auto error = candidate.remove(node_id);
    if (error != RegistryError::none) return map_error(error);
    const auto persisted = publish(candidate); if (persisted != MembershipError::none) return persisted;
    registry_ = std::move(candidate); return MembershipError::none;
}
MembershipError DurableMembership::set_state(std::string_view node_id, NodeState state) {
    NodeRegistry candidate = registry_; const auto before = candidate.version();
    const auto error = candidate.set_state(node_id, state);
    if (error != RegistryError::none) return map_error(error);
    if (candidate.version() == before) return MembershipError::none;
    const auto persisted = publish(candidate); if (persisted != MembershipError::none) return persisted;
    registry_ = std::move(candidate); return MembershipError::none;
}
HeartbeatStatus DurableMembership::check_node(std::string_view node_id, HeartbeatClient& client) {
    const auto current = registry_.get(node_id);
    if (!current) return HeartbeatStatus::identity_mismatch;
    auto result = client.check(current->endpoint);
    if (result.status == HeartbeatStatus::success && result.responding_node_id != node_id)
        result.status = HeartbeatStatus::identity_mismatch;
    NodeRegistry candidate = registry_;
    if (result.status == HeartbeatStatus::success) {
        static_cast<void>(candidate.set_health(node_id, NodeHealthState::healthy, 0, true));
    } else {
        const auto maximum = std::numeric_limits<std::uint32_t>::max();
        const auto failures = current->consecutive_failures == maximum
            ? maximum : current->consecutive_failures + 1U;
        const auto health = failures >= config_.failure_threshold
            ? NodeHealthState::unavailable : NodeHealthState::suspect;
        static_cast<void>(candidate.set_health(node_id, health, failures, true));
    }
    if (publish(candidate) != MembershipError::none) return HeartbeatStatus::persistence_error;
    registry_ = std::move(candidate); return result.status;
}
std::optional<StorageNode> DurableMembership::get(std::string_view id) const { return registry_.get(id); }
std::vector<StorageNode> DurableMembership::list() const { return registry_.list(); }
std::vector<StorageNode> DurableMembership::eligible_nodes() const { return registry_.eligible_nodes(); }
std::uint64_t DurableMembership::version() const noexcept { return registry_.version(); }
const std::filesystem::path& DurableMembership::record_path() const noexcept { return record_path_; }

}  // namespace atlasfs::metadata
