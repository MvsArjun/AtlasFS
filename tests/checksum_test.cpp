#include "atlasfs/checksum.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <string_view>

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

}  // namespace
}  // namespace atlasfs::integrity
