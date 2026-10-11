#include "atlasfs/checksum.hpp"
#include <array>

namespace atlasfs::integrity {
namespace {
constexpr auto make_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t value = 0; value < table.size(); ++value) {
        auto crc = value;
        for (unsigned bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0x82f63b78U & (0U - (crc & 1U)));
        table[value] = crc;
    }
    return table;
}
constexpr auto table = make_table(); // Immutable 1 KiB; no CPU-feature/alignment assumptions.
}

std::uint32_t crc32c(std::span<const std::byte> data) noexcept {
    std::uint32_t crc = 0xffffffffU;
    for (const auto byte : data) {
        crc = (crc >> 8U) ^ table[(crc ^ std::to_integer<std::uint8_t>(byte)) & 0xffU];
    }
    return ~crc;
}

}  // namespace atlasfs::integrity
