#pragma once

#include "atlasfs/coordinator.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::metadata {

inline constexpr std::uint32_t kMaxReplicaCatalogRecords = 65536;
inline constexpr std::uint32_t kMaxCatalogReplicasPerChunk = 4096;
inline constexpr std::size_t kMaxReplicaCatalogSize = 64U * 1024U * 1024U;

enum class CatalogReplicaState : std::uint8_t {
    present = 1,
    missing = 2,
    corrupt = 3,
    unavailable = 4,
};

struct CatalogReplica {
    std::string node_id;
    CatalogReplicaState state{CatalogReplicaState::unavailable};
    std::uint32_t integrity_checksum{};
    std::uint64_t evidence_version{};

    bool operator==(const CatalogReplica&) const = default;
};

struct ReplicaCatalogRecord {
    ChunkIdentity identity;
    storage::ChunkMetadata chunk;
    std::uint64_t catalog_version{};
    std::vector<CatalogReplica> replicas;

    bool operator==(const ReplicaCatalogRecord&) const = default;
};

struct ReplicaCatalogSnapshot {
    std::uint64_t catalog_version{};
    std::vector<ReplicaCatalogRecord> records;

    bool operator==(const ReplicaCatalogSnapshot&) const = default;
};

enum class ReplicaCatalogError {
    none,
    invalid_record,
    conflict,
    not_found,
    persistence_error,
    version_exhausted,
    durability_unknown,
};

enum class ReplicaCatalogFaultPoint {
    record_creation,
    record_write,
    record_sync,
    record_rename,
    directory_sync,
};
using ReplicaCatalogFaultInjector = std::function<bool(ReplicaCatalogFaultPoint)>;

[[nodiscard]] std::vector<std::byte> encode_replica_catalog(
    const ReplicaCatalogSnapshot& snapshot);
[[nodiscard]] std::optional<ReplicaCatalogSnapshot> decode_replica_catalog(
    std::span<const std::byte> encoded);

class DurableReplicaCatalog {
public:
    explicit DurableReplicaCatalog(
        std::filesystem::path root,
        ReplicaCatalogFaultInjector fault_injector = {});

    [[nodiscard]] ReplicaCatalogError upsert(
        ChunkIdentity identity,
        storage::ChunkMetadata chunk,
        std::vector<CatalogReplica> replicas);
    [[nodiscard]] ReplicaCatalogError erase(const ChunkIdentity& identity);

    [[nodiscard]] std::optional<ReplicaCatalogRecord> get(
        const ChunkIdentity& identity) const;
    [[nodiscard]] std::vector<ReplicaCatalogRecord> list() const;
    [[nodiscard]] std::uint64_t version() const noexcept;
    [[nodiscard]] bool durability_unknown() const noexcept;
    [[nodiscard]] const std::filesystem::path& record_path() const noexcept;

private:
    [[nodiscard]] ReplicaCatalogError publish(ReplicaCatalogSnapshot candidate);

    ReplicaCatalogSnapshot snapshot_;
    std::filesystem::path directory_;
    std::filesystem::path record_path_;
    ReplicaCatalogFaultInjector fault_injector_;
    bool durability_unknown_{};
};

}  // namespace atlasfs::metadata
