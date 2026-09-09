#pragma once

#include "atlasfs/object_protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace atlasfs::storage {

inline constexpr std::uint32_t kDefaultChunkSize = 1024U * 1024U;
inline constexpr std::uint32_t kMaxChunkCount = 65536U;
inline constexpr std::size_t kMaxGenerationIdSize = 64U;

struct ChunkMetadata {
    std::uint32_t index{};
    std::uint32_t length{};
    std::uint32_t checksum{};

    bool operator==(const ChunkMetadata&) const = default;
};

struct ObjectManifest {
    std::uint64_t object_size{};
    std::uint32_t chunk_size{};
    std::uint32_t object_checksum{};
    std::string generation_id;
    std::vector<ChunkMetadata> chunks;

    bool operator==(const ObjectManifest&) const = default;
};

[[nodiscard]] std::vector<std::byte> encode_manifest(const ObjectManifest& manifest);
[[nodiscard]] std::optional<ObjectManifest> decode_manifest(
    std::span<const std::byte> encoded,
    std::size_t max_object_size = object_protocol::kMaxObjectDataSize,
    std::uint32_t max_chunk_count = kMaxChunkCount);

}  // namespace atlasfs::storage
