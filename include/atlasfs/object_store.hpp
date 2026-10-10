#pragma once

#include "atlasfs/object_protocol.hpp"
#include "atlasfs/manifest.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace atlasfs::storage {

enum class StoreError {
    none,
    not_found,
    invalid_object_id,
    checksum_mismatch,
    corrupt_object,
    filesystem_error,
    payload_too_large,
    operation_conflict,
    invalid_operation_state,
    result_unknown,
};

enum class PutFaultPoint {
    transaction_creation,
    checksum_calculation,
    chunk_creation,
    chunk_write,
    chunk_sync,
    manifest_creation,
    manifest_write,
    manifest_sync,
    generation_rename,
    commit_rename,
    directory_sync,
    rollback_deletion,
};

using PutFaultInjector = std::function<bool(PutFaultPoint)>;

struct GetResult {
    StoreError error{StoreError::none};
    std::vector<std::byte> data;
    std::uint32_t checksum{};
};

struct ChunkResult {
    StoreError error{StoreError::none};
    ObjectManifest manifest;
    ChunkMetadata chunk;
    std::vector<std::byte> data;
};

class ObjectStore {
public:
    explicit ObjectStore(
        std::filesystem::path root,
        std::size_t max_object_size = object_protocol::kMaxObjectDataSize,
        std::uint32_t chunk_size = kDefaultChunkSize,
        PutFaultInjector fault_injector = {});

    ObjectStore(const ObjectStore&) = delete;
    ObjectStore& operator=(const ObjectStore&) = delete;

    [[nodiscard]] StoreError put(std::string_view object_id,
                                 std::span<const std::byte> data,
                                 std::uint32_t supplied_checksum);
    [[nodiscard]] GetResult get(std::string_view object_id) const;
    [[nodiscard]] StoreError remove(std::string_view object_id);
    [[nodiscard]] StoreError remove_generation(std::string_view object_id,
                                               std::string_view generation_id);

    // Single-threaded two-phase primitives used by in-process replication. A
    // prepared generation is durable but invisible until commit_replica().
    [[nodiscard]] StoreError prepare_replica(
        std::string_view object_id, const ObjectManifest& manifest,
        std::span<const std::uint32_t> chunk_indices,
        std::span<const std::byte> object_data);
    [[nodiscard]] StoreError commit_replica(std::string_view object_id,
                                            std::string_view generation_id);
    [[nodiscard]] StoreError verify_prepared_replica(
        std::string_view object_id, std::string_view generation_id,
        std::span<const std::uint32_t> chunk_indices) const;
    [[nodiscard]] StoreError abort_replica(std::string_view object_id,
                                           std::string_view generation_id);
    [[nodiscard]] StoreError finalize_replica(std::string_view object_id,
                                              std::string_view generation_id);
    [[nodiscard]] ChunkResult get_replica_chunk(std::string_view object_id,
                                                std::string_view generation_id,
                                                std::uint32_t chunk_index) const;
    // Atomically publishes one verified repair chunk. The chunk is not visible
    // through get_replica_chunk until its current-object manifest is committed.
    [[nodiscard]] StoreError publish_repair_chunk(
        std::string_view object_id, const ObjectManifest& manifest,
        const ChunkMetadata& chunk, std::span<const std::byte> data);

    [[nodiscard]] const std::filesystem::path& objects_directory() const noexcept;
    [[nodiscard]] const std::filesystem::path& chunks_directory() const noexcept;
    [[nodiscard]] const std::filesystem::path& transactions_directory() const noexcept;
    [[nodiscard]] std::uint32_t chunk_size() const noexcept;
    [[nodiscard]] static bool valid_object_id(std::string_view object_id) noexcept;

private:
    [[nodiscard]] std::filesystem::path object_path(std::string_view object_id) const;
    [[nodiscard]] std::filesystem::path next_temporary_path();
    [[nodiscard]] StoreError sync_directory(const std::filesystem::path& path) const noexcept;
    void clean_incomplete_state();
    [[nodiscard]] bool should_fail(PutFaultPoint point) const;

    struct PendingReplica {
        std::optional<std::vector<std::byte>> previous_manifest;
        std::vector<std::byte> manifest;
        bool committed{};
    };
    [[nodiscard]] static std::string pending_key(std::string_view object_id,
                                                 std::string_view generation_id);

    std::filesystem::path objects_directory_;
    std::filesystem::path chunks_directory_;
    std::filesystem::path transactions_directory_;
    std::size_t max_object_size_;
    std::uint32_t chunk_size_;
    PutFaultInjector fault_injector_;
    std::atomic_uint64_t temporary_counter_{0};
    std::map<std::string, PendingReplica, std::less<>> pending_replicas_;
};

}  // namespace atlasfs::storage
