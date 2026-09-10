#include "atlasfs/coordinator.hpp"

#include <algorithm>
#include <initializer_list>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace atlasfs::metadata {
namespace {

constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

void hash_byte(std::uint64_t& hash, std::uint8_t byte) noexcept {
    hash ^= byte;
    hash *= kFnvPrime;
}

void hash_u32(std::uint64_t& hash, std::uint32_t value) noexcept {
    for (unsigned shift : {24U, 16U, 8U, 0U}) {
        hash_byte(hash, static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

void hash_u64(std::uint64_t& hash, std::uint64_t value) noexcept {
    for (unsigned shift : {56U, 48U, 40U, 32U, 24U, 16U, 8U, 0U}) {
        hash_byte(hash, static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

void hash_string(std::uint64_t& hash, std::string_view value) noexcept {
    hash_u64(hash, static_cast<std::uint64_t>(value.size()));
    for (const unsigned char character : value) {
        hash_byte(hash, character);
    }
}

std::uint64_t score(const ChunkIdentity& chunk, std::string_view node_id) noexcept {
    std::uint64_t hash = stable_chunk_hash(chunk);
    hash_string(hash, node_id);
    return hash;
}

CoordinatorError map_error(RegistryError error) {
    switch (error) {
        case RegistryError::none: return CoordinatorError::none;
        case RegistryError::invalid_node: return CoordinatorError::invalid_node;
        case RegistryError::duplicate_node: return CoordinatorError::duplicate_node;
        case RegistryError::unknown_node: return CoordinatorError::unknown_node;
    }
    return CoordinatorError::invalid_node;
}

bool valid_generation_id(std::string_view generation_id) noexcept {
    if (generation_id.empty() || generation_id.size() > storage::kMaxGenerationIdSize) {
        return false;
    }
    for (const unsigned char character : generation_id) {
        if (!((character >= 'a' && character <= 'z') ||
              (character >= 'A' && character <= 'Z') ||
              (character >= '0' && character <= '9') || character == '-')) {
            return false;
        }
    }
    return true;
}

bool valid_chunk(const ChunkIdentity& chunk) noexcept {
    return object_protocol::valid_object_id(chunk.object_id) &&
           valid_generation_id(chunk.generation_id) && chunk.chunk_index < storage::kMaxChunkCount;
}

bool contains_node(std::span<const StorageNode> nodes, std::string_view node_id) {
    return std::any_of(nodes.begin(), nodes.end(), [node_id](const StorageNode& node) {
        return node.node_id == node_id;
    });
}

}  // namespace

std::uint64_t stable_chunk_hash(const ChunkIdentity& chunk) noexcept {
    std::uint64_t hash = kFnvOffset;
    hash_string(hash, chunk.object_id);
    hash_string(hash, chunk.generation_id);
    hash_u32(hash, chunk.chunk_index);
    return hash;
}

std::optional<std::string> RendezvousPlacement::select(
    const ChunkIdentity& chunk, std::span<const StorageNode> eligible_nodes) const {
    std::optional<std::string> selected;
    std::uint64_t best_score = 0;
    for (const auto& node : eligible_nodes) {
        const auto candidate_score = score(chunk, node.node_id);
        if (!selected || candidate_score > best_score ||
            (candidate_score == best_score && node.node_id < *selected)) {
            selected = node.node_id;
            best_score = candidate_score;
        }
    }
    return selected;
}

MetadataCoordinator::MetadataCoordinator(std::unique_ptr<PlacementPolicy> policy)
    : policy_(std::move(policy)) {
    if (!policy_) {
        throw std::invalid_argument("metadata coordinator requires a placement policy");
    }
}

CoordinatorError MetadataCoordinator::register_node(StorageNode node) {
    return map_error(registry_.add(std::move(node)));
}

CoordinatorError MetadataCoordinator::remove_node(std::string_view node_id) {
    return map_error(registry_.remove(node_id));
}

CoordinatorError MetadataCoordinator::set_node_state(std::string_view node_id,
                                                      NodeState state) {
    return map_error(registry_.set_state(node_id, state));
}

std::optional<StorageNode> MetadataCoordinator::get_node(std::string_view node_id) const {
    return registry_.get(node_id);
}

std::vector<StorageNode> MetadataCoordinator::list_nodes() const {
    return registry_.list();
}

std::uint64_t MetadataCoordinator::membership_version() const noexcept {
    return registry_.version();
}

PlacementResult MetadataCoordinator::place_chunk(const ChunkIdentity& chunk) const {
    if (!valid_generation_id(chunk.generation_id)) {
        return {CoordinatorError::invalid_generation, std::nullopt};
    }
    if (!valid_chunk(chunk)) {
        return {CoordinatorError::invalid_chunk, std::nullopt};
    }
    const auto eligible = registry_.eligible_nodes();
    if (eligible.empty()) {
        return {CoordinatorError::no_active_nodes, std::nullopt};
    }
    auto selected = policy_->select(chunk, eligible);
    if (selected && !contains_node(eligible, *selected)) {
        return {CoordinatorError::invalid_membership, std::nullopt};
    }
    return selected ? PlacementResult{CoordinatorError::none, std::move(selected)}
                    : PlacementResult{CoordinatorError::no_active_nodes, std::nullopt};
}

PlanResult MetadataCoordinator::place_object(const ObjectMetadata& object) const {
    if (!object_protocol::valid_object_id(object.object_id)) {
        return {CoordinatorError::invalid_object_metadata, std::nullopt};
    }
    if (!valid_generation_id(object.manifest.generation_id)) {
        return {CoordinatorError::invalid_generation, std::nullopt};
    }
    try {
        static_cast<void>(storage::encode_manifest(object.manifest));
    } catch (const std::invalid_argument&) {
        return {CoordinatorError::invalid_object_metadata, std::nullopt};
    }
    const auto eligible = registry_.eligible_nodes();
    if (!object.manifest.chunks.empty() && eligible.empty()) {
        return {CoordinatorError::no_active_nodes, std::nullopt};
    }

    PlacementPlan plan{object.object_id, object.manifest.generation_id,
                       object.manifest.object_size, object.manifest.chunk_size,
                       object.manifest.object_checksum, registry_.version(), {}};
    plan.chunks.reserve(object.manifest.chunks.size());
    for (const auto& chunk : object.manifest.chunks) {
        ChunkIdentity identity{object.object_id, object.manifest.generation_id, chunk.index};
        auto node_id = policy_->select(identity, eligible);
        if (!node_id) {
            return {CoordinatorError::no_active_nodes, std::nullopt};
        }
        if (!contains_node(eligible, *node_id)) {
            return {CoordinatorError::invalid_membership, std::nullopt};
        }
        plan.chunks.push_back({std::move(identity), chunk, std::move(*node_id)});
    }
    return {CoordinatorError::none, std::move(plan)};
}

}  // namespace atlasfs::metadata
