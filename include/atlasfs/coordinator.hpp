#pragma once

#include "atlasfs/manifest.hpp"
#include "atlasfs/node_registry.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::metadata {

struct ChunkIdentity {
    std::string object_id;
    std::string generation_id;
    std::uint32_t chunk_index{};

    bool operator==(const ChunkIdentity&) const = default;
};

struct ObjectMetadata {
    std::string object_id;
    storage::ObjectManifest manifest;

    bool operator==(const ObjectMetadata&) const = default;
};

struct ChunkPlacement {
    ChunkIdentity identity;
    storage::ChunkMetadata chunk;
    std::string primary_node_id;

    bool operator==(const ChunkPlacement&) const = default;
};

struct PlacementPlan {
    std::string object_id;
    std::string generation_id;
    std::uint64_t object_size{};
    std::uint32_t chunk_size{};
    std::uint32_t object_checksum{};
    std::uint64_t membership_version{};
    std::vector<ChunkPlacement> chunks;

    bool operator==(const PlacementPlan&) const = default;
};

enum class CoordinatorError {
    none,
    invalid_node,
    duplicate_node,
    unknown_node,
    no_active_nodes,
    invalid_chunk,
    invalid_object_metadata,
    invalid_generation,
    invalid_membership,
};

struct PlacementResult {
    CoordinatorError error{CoordinatorError::none};
    std::optional<std::string> node_id;
};

struct PlanResult {
    CoordinatorError error{CoordinatorError::none};
    std::optional<PlacementPlan> plan;
};

class PlacementPolicy {
public:
    virtual ~PlacementPolicy() = default;
    [[nodiscard]] virtual std::optional<std::string> select(
        const ChunkIdentity& chunk, std::span<const StorageNode> eligible_nodes) const = 0;
};

class RendezvousPlacement final : public PlacementPolicy {
public:
    [[nodiscard]] std::optional<std::string> select(
        const ChunkIdentity& chunk,
        std::span<const StorageNode> eligible_nodes) const override;
};

[[nodiscard]] std::uint64_t stable_chunk_hash(const ChunkIdentity& chunk) noexcept;

class MetadataCoordinator {
public:
    explicit MetadataCoordinator(
        std::unique_ptr<PlacementPolicy> policy = std::make_unique<RendezvousPlacement>());

    [[nodiscard]] CoordinatorError register_node(StorageNode node);
    [[nodiscard]] CoordinatorError remove_node(std::string_view node_id);
    [[nodiscard]] CoordinatorError set_node_state(std::string_view node_id, NodeState state);
    [[nodiscard]] std::optional<StorageNode> get_node(std::string_view node_id) const;
    [[nodiscard]] std::vector<StorageNode> list_nodes() const;
    [[nodiscard]] std::uint64_t membership_version() const noexcept;

    [[nodiscard]] PlacementResult place_chunk(const ChunkIdentity& chunk) const;
    [[nodiscard]] PlanResult place_object(const ObjectMetadata& object) const;

private:
    NodeRegistry registry_;
    std::unique_ptr<PlacementPolicy> policy_;
};

}  // namespace atlasfs::metadata
