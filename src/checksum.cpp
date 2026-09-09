#include "atlasfs/checksum.hpp"

namespace atlasfs::integrity {

std::uint32_t crc32c(std::span<const std::byte> data) noexcept {
    std::uint32_t crc = 0xffffffffU;
    for (const auto byte : data) {
        crc ^= std::to_integer<std::uint8_t>(byte);
        for (unsigned bit = 0; bit < 8; ++bit) {
            const std::uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0x82f63b78U & mask);
        }
    }
    return ~crc;
}

}  // namespace atlasfs::integrity
