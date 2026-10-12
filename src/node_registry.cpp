#include "atlasfs/node_registry.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace atlasfs::metadata {
namespace {

bool valid_state(NodeState state) noexcept {
    return state == NodeState::active || state == NodeState::draining ||
           state == NodeState::unavailable;
}
bool valid_health(NodeHealthState state) noexcept {
    return state == NodeHealthState::healthy || state == NodeHealthState::suspect ||
           state == NodeHealthState::unavailable;
}

}  // namespace

RegistryError NodeRegistry::add(StorageNode node) {
    if (!valid_node_id(node.node_id) || !valid_endpoint(node.endpoint) ||
        !valid_state(node.state) || !valid_health(node.health)) {
        return RegistryError::invalid_node;
    }
    if (version_ == std::numeric_limits<std::uint64_t>::max()) {
        return nodes_.contains(node.node_id) ? RegistryError::duplicate_node
                                             : RegistryError::version_exhausted;
    }
    auto node_id = node.node_id;
    const auto [iterator, inserted] = nodes_.emplace(std::move(node_id), std::move(node));
    static_cast<void>(iterator);
    if (!inserted) {
        return RegistryError::duplicate_node;
    }
    ++version_;
    return RegistryError::none;
}

RegistryError NodeRegistry::set_health(std::string_view node_id, NodeHealthState health,
                                       std::uint32_t consecutive_failures,
                                       bool health_current) {
    if (!valid_health(health)) return RegistryError::invalid_node;
    const auto iterator = nodes_.find(node_id);
    if (iterator == nodes_.end()) return RegistryError::unknown_node;
    auto& node = iterator->second;
    if (node.health != health || node.consecutive_failures != consecutive_failures ||
        node.health_current != health_current) {
        if (version_ == std::numeric_limits<std::uint64_t>::max())
            return RegistryError::version_exhausted;
        node.health = health;
        node.consecutive_failures = consecutive_failures;
        node.health_current = health_current;
        ++version_;
    }
    return RegistryError::none;
}

RegistryError NodeRegistry::restore(std::vector<StorageNode> nodes, std::uint64_t version) {
    std::map<std::string, StorageNode, std::less<>> restored;
    for (auto& node : nodes) {
        if (!valid_node_id(node.node_id) || !valid_endpoint(node.endpoint) ||
            !valid_state(node.state) || !valid_health(node.health) ||
            !restored.emplace(node.node_id, std::move(node)).second)
            return RegistryError::invalid_node;
    }
    nodes_ = std::move(restored);
    version_ = version;
    return RegistryError::none;
}

RegistryError NodeRegistry::remove(std::string_view node_id) {
    const auto iterator = nodes_.find(node_id);
    if (iterator == nodes_.end()) {
        return RegistryError::unknown_node;
    }
    if (version_ == std::numeric_limits<std::uint64_t>::max())
        return RegistryError::version_exhausted;
    nodes_.erase(iterator);
    ++version_;
    return RegistryError::none;
}

RegistryError NodeRegistry::set_state(std::string_view node_id, NodeState state) {
    if (!valid_state(state)) {
        return RegistryError::invalid_node;
    }
    const auto iterator = nodes_.find(node_id);
    if (iterator == nodes_.end()) {
        return RegistryError::unknown_node;
    }
    if (iterator->second.state != state) {
        if (version_ == std::numeric_limits<std::uint64_t>::max())
            return RegistryError::version_exhausted;
        iterator->second.state = state;
        ++version_;
    }
    return RegistryError::none;
}

std::optional<StorageNode> NodeRegistry::get(std::string_view node_id) const {
    const auto iterator = nodes_.find(node_id);
    return iterator == nodes_.end() ? std::nullopt
                                    : std::optional<StorageNode>(iterator->second);
}

std::vector<StorageNode> NodeRegistry::list() const {
    std::vector<StorageNode> result;
    result.reserve(nodes_.size());
    for (const auto& [id, node] : nodes_) {
        static_cast<void>(id);
        result.push_back(node);
    }
    return result;
}

std::vector<StorageNode> NodeRegistry::eligible_nodes() const {
    auto result = list();
    std::erase_if(result, [](const StorageNode& node) {
        return node.state != NodeState::active || node.health != NodeHealthState::healthy ||
               !node.health_current;
    });
    return result;
}

std::uint64_t NodeRegistry::version() const noexcept {
    return version_;
}

bool NodeRegistry::valid_node_id(std::string_view node_id) noexcept {
    if (node_id.empty() || node_id.size() > kMaxNodeIdSize) {
        return false;
    }
    return std::all_of(node_id.begin(), node_id.end(), [](unsigned char character) {
        return (character >= 'a' && character <= 'z') ||
               (character >= 'A' && character <= 'Z') ||
               (character >= '0' && character <= '9') || character == '-' ||
               character == '_';
    });
}

bool NodeRegistry::valid_endpoint(const NodeEndpoint& endpoint) noexcept {
    if (endpoint.host.empty() || endpoint.host.size() > kMaxHostSize || endpoint.port == 0) {
        return false;
    }
    const bool characters_valid =
        std::all_of(endpoint.host.begin(), endpoint.host.end(), [](unsigned char character) {
        return (character >= 'a' && character <= 'z') ||
               (character >= 'A' && character <= 'Z') ||
               (character >= '0' && character <= '9') || character == '.' ||
               character == '-' || character == ':';
    });
    const bool has_alphanumeric =
        std::any_of(endpoint.host.begin(), endpoint.host.end(), [](unsigned char character) {
            return (character >= 'a' && character <= 'z') ||
                   (character >= 'A' && character <= 'Z') ||
                   (character >= '0' && character <= '9');
        });
    return characters_valid && has_alphanumeric;
}

}  // namespace atlasfs::metadata
