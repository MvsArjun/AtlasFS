#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::metadata {

inline constexpr std::size_t kMaxNodeIdSize = 64;
inline constexpr std::size_t kMaxHostSize = 253;

enum class NodeState {
    active,
    draining,
    unavailable,
};

struct NodeEndpoint {
    std::string host;
    std::uint16_t port{};

    bool operator==(const NodeEndpoint&) const = default;
};

struct StorageNode {
    std::string node_id;
    NodeEndpoint endpoint;
    NodeState state{NodeState::active};

    bool operator==(const StorageNode&) const = default;
};

enum class RegistryError {
    none,
    invalid_node,
    duplicate_node,
    unknown_node,
};

class NodeRegistry {
public:
    [[nodiscard]] RegistryError add(StorageNode node);
    [[nodiscard]] RegistryError remove(std::string_view node_id);
    [[nodiscard]] RegistryError set_state(std::string_view node_id, NodeState state);
    [[nodiscard]] std::optional<StorageNode> get(std::string_view node_id) const;
    [[nodiscard]] std::vector<StorageNode> list() const;
    [[nodiscard]] std::vector<StorageNode> eligible_nodes() const;
    [[nodiscard]] std::uint64_t version() const noexcept;

    [[nodiscard]] static bool valid_node_id(std::string_view node_id) noexcept;
    [[nodiscard]] static bool valid_endpoint(const NodeEndpoint& endpoint) noexcept;

private:
    std::map<std::string, StorageNode, std::less<>> nodes_;
    std::uint64_t version_{0};
};

}  // namespace atlasfs::metadata
