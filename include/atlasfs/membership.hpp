#pragma once

#include "atlasfs/node_registry.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::metadata {

inline constexpr std::uint32_t kDefaultFailureThreshold = 3;
inline constexpr std::uint32_t kMaxFailureThreshold = 1000;
inline constexpr std::uint32_t kMaxMembershipNodes = 4096;

struct MembershipSnapshot {
    std::uint64_t version{};
    std::vector<StorageNode> nodes;
    bool operator==(const MembershipSnapshot&) const = default;
};

enum class MembershipError {
    none, invalid_node, duplicate_node, unknown_node, persistence_error
};
enum class MembershipFaultPoint { record_creation, record_write, record_sync, record_rename,
                                  directory_sync };
using MembershipFaultInjector = std::function<bool(MembershipFaultPoint)>;

enum class HeartbeatStatus {
    success, connection_failure, timeout, malformed_response, identity_mismatch, response_lost,
    persistence_error
};
struct HeartbeatResult {
    HeartbeatStatus status{HeartbeatStatus::connection_failure};
    std::string responding_node_id;
};
class HeartbeatClient {
public:
    virtual ~HeartbeatClient() = default;
    [[nodiscard]] virtual HeartbeatResult check(const NodeEndpoint& endpoint) = 0;
};

struct FailureDetectorConfig { std::uint32_t failure_threshold{kDefaultFailureThreshold}; };

[[nodiscard]] std::vector<std::byte> encode_membership(const MembershipSnapshot& snapshot);
[[nodiscard]] std::optional<MembershipSnapshot> decode_membership(
    std::span<const std::byte> encoded);

class DurableMembership {
public:
    DurableMembership(std::filesystem::path root,
                      FailureDetectorConfig config = {},
                      MembershipFaultInjector fault_injector = {});
    [[nodiscard]] MembershipError add(StorageNode node);
    [[nodiscard]] MembershipError remove(std::string_view node_id);
    [[nodiscard]] MembershipError set_state(std::string_view node_id, NodeState state);
    [[nodiscard]] HeartbeatStatus check_node(std::string_view node_id,
                                             HeartbeatClient& client);
    [[nodiscard]] std::optional<StorageNode> get(std::string_view node_id) const;
    [[nodiscard]] std::vector<StorageNode> list() const;
    [[nodiscard]] std::vector<StorageNode> eligible_nodes() const;
    [[nodiscard]] std::uint64_t version() const noexcept;
    [[nodiscard]] const std::filesystem::path& record_path() const noexcept;
private:
    [[nodiscard]] MembershipError publish(const NodeRegistry& candidate);
    NodeRegistry registry_;
    std::filesystem::path directory_;
    std::filesystem::path record_path_;
    FailureDetectorConfig config_;
    MembershipFaultInjector fault_injector_;
};

}  // namespace atlasfs::metadata
