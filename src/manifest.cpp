#include "atlasfs/manifest.hpp"

#include "atlasfs/checksum.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace atlasfs::storage {
namespace {

constexpr std::array<std::byte, 4> kMagic{
    static_cast<std::byte>('A'), static_cast<std::byte>('M'),
    static_cast<std::byte>('N'), static_cast<std::byte>('F')};
constexpr std::uint16_t kVersion = 1;
constexpr std::size_t kFixedSize = 30;
constexpr std::size_t kChunkEntrySize = 12;
constexpr std::size_t kChecksumSize = 4;

template <typename UInt>
void append_integer(std::vector<std::byte>& output, UInt value) {
    static_assert(std::is_unsigned_v<UInt>);
    for (std::size_t index = sizeof(UInt); index > 0; --index) {
        const auto shift = static_cast<unsigned>((index - 1U) * 8U);
        output.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
    }
}

template <typename UInt>
std::optional<UInt> read_integer(std::span<const std::byte> input, std::size_t& offset) {
    static_assert(std::is_unsigned_v<UInt>);
    if (offset > input.size() || input.size() - offset < sizeof(UInt)) {
        return std::nullopt;
    }
    UInt value = 0;
    for (std::size_t index = 0; index < sizeof(UInt); ++index) {
        value = static_cast<UInt>((value << 8U) |
                                  std::to_integer<unsigned>(input[offset + index]));
    }
    offset += sizeof(UInt);
    return value;
}

bool valid_generation_id(std::string_view value) {
    if (value.empty() || value.size() > kMaxGenerationIdSize) {
        return false;
    }
    return std::all_of(value.begin(), value.end(), [](unsigned char character) {
        return (character >= 'a' && character <= 'z') ||
               (character >= 'A' && character <= 'Z') ||
               (character >= '0' && character <= '9') || character == '-';
    });
}

bool consistent(const ObjectManifest& manifest, std::size_t max_object_size,
                std::uint32_t max_chunk_count) {
    if (manifest.object_size > max_object_size || manifest.chunk_size == 0 ||
        manifest.chunk_size > max_object_size || !valid_generation_id(manifest.generation_id) ||
        manifest.chunks.size() > max_chunk_count) {
        return false;
    }
    const auto expected_count = manifest.object_size == 0
                                    ? 0U
                                    : 1U + (manifest.object_size - 1U) / manifest.chunk_size;
    if (expected_count != manifest.chunks.size()) {
        return false;
    }
    for (std::size_t index = 0; index < manifest.chunks.size(); ++index) {
        const auto& chunk = manifest.chunks[index];
        const auto bytes_before = static_cast<std::uint64_t>(index) * manifest.chunk_size;
        const auto remaining = manifest.object_size - bytes_before;
        const auto expected_length = static_cast<std::uint32_t>(
            std::min<std::uint64_t>(remaining, manifest.chunk_size));
        if (chunk.index != static_cast<std::uint32_t>(index) ||
            chunk.length != expected_length || chunk.length == 0) {
            return false;
        }
    }
    return true;
}

}  // namespace

std::vector<std::byte> encode_manifest(const ObjectManifest& manifest) {
    if (!consistent(manifest, object_protocol::kMaxObjectDataSize, kMaxChunkCount) ||
        manifest.chunks.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("inconsistent AtlasFS object manifest");
    }
    const auto manifest_size = kFixedSize + manifest.generation_id.size() +
                               manifest.chunks.size() * kChunkEntrySize + kChecksumSize;
    std::vector<std::byte> output;
    output.reserve(manifest_size);
    output.insert(output.end(), kMagic.begin(), kMagic.end());
    append_integer(output, kVersion);
    append_integer<std::uint16_t>(output, 0);
    append_integer(output, manifest.object_size);
    append_integer(output, manifest.chunk_size);
    append_integer(output, static_cast<std::uint32_t>(manifest.chunks.size()));
    append_integer(output, manifest.object_checksum);
    append_integer(output, static_cast<std::uint16_t>(manifest.generation_id.size()));
    const auto* generation = reinterpret_cast<const std::byte*>(manifest.generation_id.data());
    output.insert(output.end(), generation, generation + manifest.generation_id.size());
    for (const auto& chunk : manifest.chunks) {
        append_integer(output, chunk.index);
        append_integer(output, chunk.length);
        append_integer(output, chunk.checksum);
    }
    append_integer(output, integrity::crc32c(output));
    return output;
}

std::optional<ObjectManifest> decode_manifest(std::span<const std::byte> encoded,
                                               std::size_t max_object_size,
                                               std::uint32_t max_chunk_count) {
    if (encoded.size() < kFixedSize + kChecksumSize ||
        !std::equal(kMagic.begin(), kMagic.end(), encoded.begin())) {
        return std::nullopt;
    }
    const auto content = encoded.first(encoded.size() - kChecksumSize);
    std::size_t checksum_offset = encoded.size() - kChecksumSize;
    const auto stored_checksum = read_integer<std::uint32_t>(encoded, checksum_offset);
    if (!stored_checksum || integrity::crc32c(content) != *stored_checksum) {
        return std::nullopt;
    }

    std::size_t offset = 4;
    const auto version = read_integer<std::uint16_t>(content, offset);
    const auto reserved = read_integer<std::uint16_t>(content, offset);
    const auto object_size = read_integer<std::uint64_t>(content, offset);
    const auto chunk_size = read_integer<std::uint32_t>(content, offset);
    const auto chunk_count = read_integer<std::uint32_t>(content, offset);
    const auto object_checksum = read_integer<std::uint32_t>(content, offset);
    const auto generation_size = read_integer<std::uint16_t>(content, offset);
    if (!version || *version != kVersion || !reserved || *reserved != 0 || !object_size ||
        !chunk_size || !chunk_count || !object_checksum || !generation_size ||
        *generation_size == 0 || *generation_size > kMaxGenerationIdSize ||
        *chunk_count > max_chunk_count || *object_size > max_object_size) {
        return std::nullopt;
    }
    const auto entries_size = static_cast<std::size_t>(*chunk_count) * kChunkEntrySize;
    if (offset > content.size() || *generation_size > content.size() - offset ||
        entries_size != content.size() - offset - *generation_size) {
        return std::nullopt;
    }

    ObjectManifest manifest;
    manifest.object_size = *object_size;
    manifest.chunk_size = *chunk_size;
    manifest.object_checksum = *object_checksum;
    manifest.generation_id.assign(
        reinterpret_cast<const char*>(content.data() + offset), *generation_size);
    offset += *generation_size;
    manifest.chunks.reserve(*chunk_count);
    for (std::uint32_t index = 0; index < *chunk_count; ++index) {
        const auto stored_index = read_integer<std::uint32_t>(content, offset);
        const auto length = read_integer<std::uint32_t>(content, offset);
        const auto checksum = read_integer<std::uint32_t>(content, offset);
        if (!stored_index || !length || !checksum) {
            return std::nullopt;
        }
        manifest.chunks.push_back({*stored_index, *length, *checksum});
    }
    if (offset != content.size() || !consistent(manifest, max_object_size, max_chunk_count)) {
        return std::nullopt;
    }
    return manifest;
}

}  // namespace atlasfs::storage
