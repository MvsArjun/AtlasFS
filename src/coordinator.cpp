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
        case RegistryError::version_exhausted: return CoordinatorError::version_exhausted;
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

std::optional<std::string> PlacementPolicy::select(
    const ChunkIdentity& chunk, std::span<const StorageNode> eligible_nodes) const {
    auto ranked = rank(chunk, eligible_nodes);
    if (ranked.empty()) { return std::nullopt; }
    return std::move(ranked.front());
}

std::vector<std::string> RendezvousPlacement::rank(
    const ChunkIdentity& chunk, std::span<const StorageNode> eligible_nodes) const {
    struct RankedNode { std::uint64_t score{}; std::string node_id; };
    std::vector<RankedNode> ranked;
    ranked.reserve(eligible_nodes.size());
    for (const auto& node : eligible_nodes) {
        ranked.push_back({score(chunk, node.node_id), node.node_id});
    }
    std::sort(ranked.begin(), ranked.end(), [](const auto& left, const auto& right) {
        return left.score > right.score ||
               (left.score == right.score && left.node_id < right.node_id);
    });
    std::vector<std::string> result;
    result.reserve(ranked.size());
    for (auto& entry : ranked) { result.push_back(std::move(entry.node_id)); }
    return result;
}

MetadataCoordinator::MetadataCoordinator(std::unique_ptr<PlacementPolicy> policy,
                                         std::uint32_t replication_factor)
    : policy_(std::move(policy)), replication_factor_(replication_factor) {
    if (!policy_) {
        throw std::invalid_argument("metadata coordinator requires a placement policy");
    }
    if (replication_factor == 0 || replication_factor > kMaxReplicationFactor) {
        throw std::invalid_argument("invalid AtlasFS replication factor");
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
    auto replicas = place_chunk_replicas(chunk);
    if (replicas.error != CoordinatorError::none || replicas.node_ids.empty()) {
        return {replicas.error, std::nullopt};
    }
    return {CoordinatorError::none, std::move(replicas.node_ids.front())};
}

ReplicaPlacementResult MetadataCoordinator::place_chunk_replicas(
    const ChunkIdentity& chunk) const {
    if (!valid_generation_id(chunk.generation_id)) {
        return {CoordinatorError::invalid_generation, {}, false};
    }
    if (!valid_chunk(chunk)) {
        return {CoordinatorError::invalid_chunk, {}, false};
    }
    const auto eligible = registry_.eligible_nodes();
    if (eligible.empty()) {
        return {CoordinatorError::no_active_nodes, {}, false};
    }
    auto ranked = policy_->rank(chunk, eligible);
    if (ranked.empty()) {
        return {CoordinatorError::no_active_nodes, {}, false};
    }
    std::vector<std::string> selected;
    selected.reserve(std::min<std::size_t>(replication_factor_, eligible.size()));
    for (auto& node_id : ranked) {
        if (!contains_node(eligible, node_id) ||
            std::find(selected.begin(), selected.end(), node_id) != selected.end()) {
            return {CoordinatorError::invalid_membership, {}, false};
        }
        if (selected.size() < replication_factor_) { selected.push_back(std::move(node_id)); }
    }
    if (ranked.size() != eligible.size()) {
        return {CoordinatorError::invalid_membership, {}, false};
    }
    return {CoordinatorError::none, std::move(selected),
            eligible.size() < replication_factor_};
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
                       object.manifest.object_checksum, registry_.version(),
                       replication_factor_, false, {}};
    plan.chunks.reserve(object.manifest.chunks.size());
    for (const auto& chunk : object.manifest.chunks) {
        ChunkIdentity identity{object.object_id, object.manifest.generation_id, chunk.index};
        auto replicas = place_chunk_replicas(identity);
        if (replicas.error != CoordinatorError::none) {
            return {replicas.error, std::nullopt};
        }
        plan.under_replicated = plan.under_replicated || replicas.under_replicated;
        plan.chunks.push_back({std::move(identity), chunk, std::move(replicas.node_ids)});
    }
    return {CoordinatorError::none, std::move(plan)};
}

}  // namespace atlasfs::metadata
