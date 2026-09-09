#pragma once

#include "atlasfs/object_protocol.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
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
};

struct GetResult {
    StoreError error{StoreError::none};
    std::vector<std::byte> data;
    std::uint32_t checksum{};
};

class ObjectStore {
public:
    explicit ObjectStore(
        std::filesystem::path root,
        std::size_t max_object_size = object_protocol::kMaxObjectDataSize);

    ObjectStore(const ObjectStore&) = delete;
    ObjectStore& operator=(const ObjectStore&) = delete;

    [[nodiscard]] StoreError put(std::string_view object_id,
                                 std::span<const std::byte> data,
                                 std::uint32_t supplied_checksum);
    [[nodiscard]] GetResult get(std::string_view object_id) const;
    [[nodiscard]] StoreError remove(std::string_view object_id);

    [[nodiscard]] const std::filesystem::path& objects_directory() const noexcept;
    [[nodiscard]] static bool valid_object_id(std::string_view object_id) noexcept;

private:
    [[nodiscard]] std::filesystem::path object_path(std::string_view object_id) const;
    [[nodiscard]] std::filesystem::path next_temporary_path();
    [[nodiscard]] StoreError sync_directory() const noexcept;

    std::filesystem::path objects_directory_;
    std::size_t max_object_size_;
    std::atomic_uint64_t temporary_counter_{0};
};

}  // namespace atlasfs::storage
