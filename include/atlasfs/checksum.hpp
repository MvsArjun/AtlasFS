#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace atlasfs::integrity {

// CRC-32C (Castagnoli), represented as an unsigned 32-bit value.
[[nodiscard]] std::uint32_t crc32c(std::span<const std::byte> data) noexcept;

}  // namespace atlasfs::integrity
