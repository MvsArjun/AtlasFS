#include "atlasfs/checksum.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <string_view>
#include <vector>

namespace atlasfs::integrity {
namespace {

std::span<const std::byte> as_bytes(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

TEST(Crc32c, MatchesStandardCheckValue) {
    EXPECT_EQ(crc32c(as_bytes("123456789")), 0xe3069283U);
}

TEST(Crc32c, HasDefinedEmptyValue) {
    EXPECT_EQ(crc32c({}), 0U);
}
std::uint32_t reference_crc(std::span<const std::byte> data) {
    std::uint32_t crc = 0xffffffffU;
    for (auto byte : data) {
        crc ^= std::to_integer<std::uint8_t>(byte);
        for (unsigned bit = 0; bit < 8; ++bit) crc = (crc >> 1U) ^ (0x82f63b78U & (0U - (crc & 1U)));
    }
    return ~crc;
}
TEST(Crc32c, TableMatchesBitwiseReferenceAcrossLengthsSeedsAndUnalignedSpans) {
    for (std::uint32_t seed : {1U, 42U, 0xffffffffU}) {
        auto state = seed; std::vector<std::byte> data(1048584);
        for (auto& byte : data) { state = state * 1664525U + 1013904223U; byte = static_cast<std::byte>(state >> 24U); }
        for (std::size_t offset = 0; offset < 8; ++offset)
            for (std::size_t size : {0U, 1U, 7U, 8U, 15U, 16U, 63U, 64U, 255U, 256U, 1023U, 4096U}) {
                const auto view = std::span<const std::byte>(data).subspan(offset, size);
                EXPECT_EQ(crc32c(view), reference_crc(view)) << "seed=" << seed << " offset=" << offset << " size=" << size;
            }
        const auto large = std::span<const std::byte>(data).subspan(3, 1048576);
        EXPECT_EQ(crc32c(large), reference_crc(large));
    }
}
TEST(Crc32c, TableMatchesEveryByteValueAndDetectsChangedBytes) {
    std::vector<std::byte> data;
    for (unsigned n = 0; n < 256; ++n) {
        data.push_back(static_cast<std::byte>(n));
        EXPECT_EQ(crc32c(data), reference_crc(data));
    }
    const auto original = crc32c(data);
    for (auto& byte : data) { byte ^= std::byte{1}; EXPECT_NE(crc32c(data), original); byte ^= std::byte{1}; }
}

}  // namespace
}  // namespace atlasfs::integrity
