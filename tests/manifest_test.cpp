#include "atlasfs/checksum.hpp"
#include "atlasfs/manifest.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace atlasfs::storage {
namespace {

ObjectManifest sample_manifest() {
    return {9, 4, 123, "generation-1", {{0, 4, 10}, {1, 4, 11}, {2, 1, 12}}};
}

void set_u32(std::vector<std::byte>& bytes, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        bytes[offset + index] =
            static_cast<std::byte>((value >> ((3U - index) * 8U)) & 0xffU);
    }
}

void refresh_checksum(std::vector<std::byte>& encoded) {
    const auto checksum = integrity::crc32c(std::span(encoded).first(encoded.size() - 4U));
    set_u32(encoded, encoded.size() - 4U, checksum);
}

TEST(Manifest, RoundTripsExplicitBinaryFormat) {
    const auto manifest = sample_manifest();
    EXPECT_EQ(decode_manifest(encode_manifest(manifest)), manifest);
}

TEST(Manifest, SupportsEmptyObject) {
    const ObjectManifest manifest{0, 4, 0, "generation-empty", {}};
    EXPECT_EQ(decode_manifest(encode_manifest(manifest)), manifest);
}

TEST(Manifest, RejectsEveryTruncation) {
    const auto encoded = encode_manifest(sample_manifest());
    for (std::size_t size = 0; size < encoded.size(); ++size) {
        EXPECT_FALSE(decode_manifest(std::span(encoded).first(size))) << size;
    }
}

TEST(Manifest, RejectsMagicVersionAndChecksumCorruption) {
    auto encoded = encode_manifest(sample_manifest());
    encoded[0] ^= std::byte{1};
    EXPECT_FALSE(decode_manifest(encoded));
    encoded = encode_manifest(sample_manifest());
    encoded[5] = std::byte{2};
    refresh_checksum(encoded);
    EXPECT_FALSE(decode_manifest(encoded));
    encoded = encode_manifest(sample_manifest());
    encoded.back() ^= std::byte{1};
    EXPECT_FALSE(decode_manifest(encoded));
}

TEST(Manifest, RejectsInvalidChunkCountAndLength) {
    auto encoded = encode_manifest(sample_manifest());
    set_u32(encoded, 20, 2);
    refresh_checksum(encoded);
    EXPECT_FALSE(decode_manifest(encoded));

    encoded = encode_manifest(sample_manifest());
    const std::size_t first_chunk_length = 30U + sample_manifest().generation_id.size() + 4U;
    set_u32(encoded, first_chunk_length, 3);
    refresh_checksum(encoded);
    EXPECT_FALSE(decode_manifest(encoded));
}

TEST(Manifest, RejectsInconsistentObjectSize) {
    auto encoded = encode_manifest(sample_manifest());
    encoded[15] = std::byte{8};
    refresh_checksum(encoded);
    EXPECT_FALSE(decode_manifest(encoded));
}

TEST(Manifest, RejectsTrailingGarbage) {
    auto encoded = encode_manifest(sample_manifest());
    encoded.resize(encoded.size() - 4U);
    encoded.push_back(std::byte{0});
    encoded.resize(encoded.size() + 4U);
    refresh_checksum(encoded);
    EXPECT_FALSE(decode_manifest(encoded));
}

TEST(Manifest, RejectsOversizedObjectAndChunkCountBeforeAllocation) {
    auto encoded = encode_manifest(sample_manifest());
    encoded[8] = std::byte{1};
    refresh_checksum(encoded);
    EXPECT_FALSE(decode_manifest(encoded));
    EXPECT_FALSE(decode_manifest(encode_manifest(sample_manifest()), 8));
}

TEST(Manifest, EncoderRejectsInconsistentMetadata) {
    auto manifest = sample_manifest();
    manifest.chunks[1].index = 5;
    EXPECT_THROW(encode_manifest(manifest), std::invalid_argument);
}

}  // namespace
}  // namespace atlasfs::storage
