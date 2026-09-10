#pragma once

#include "atlasfs/protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::object_protocol {

inline constexpr std::size_t kMaxObjectIdSize = 255;
inline constexpr std::size_t kMaxObjectDataSize =
    protocol::kDefaultMaxPayloadSize - 512U;

[[nodiscard]] bool valid_object_id(std::string_view object_id) noexcept;

enum class Status : std::uint16_t {
    success = 0,
    not_found = 1,
    invalid_request = 2,
    invalid_object_id = 3,
    checksum_mismatch = 4,
    corrupt_object = 5,
    filesystem_error = 6,
    payload_too_large = 7,
    malformed_request = 8,
};

struct PutRequest {
    std::string object_id;
    std::vector<std::byte> data;
    std::uint32_t checksum{};

    bool operator==(const PutRequest&) const = default;
};

struct ObjectIdRequest {
    std::string object_id;

    bool operator==(const ObjectIdRequest&) const = default;
};

struct GetResponse {
    Status status{Status::success};
    std::vector<std::byte> data;
    std::uint32_t checksum{};

    bool operator==(const GetResponse&) const = default;
};

[[nodiscard]] std::vector<std::byte> encode_put_request(const PutRequest& request);
[[nodiscard]] std::optional<PutRequest> decode_put_request(
    std::span<const std::byte> payload,
    std::size_t max_data_size = kMaxObjectDataSize);

[[nodiscard]] std::vector<std::byte> encode_object_id_request(
    const ObjectIdRequest& request);
[[nodiscard]] std::optional<ObjectIdRequest> decode_object_id_request(
    std::span<const std::byte> payload);

[[nodiscard]] std::vector<std::byte> encode_status_response(Status status);
[[nodiscard]] std::optional<Status> decode_status_response(
    std::span<const std::byte> payload);

[[nodiscard]] std::vector<std::byte> encode_get_response(const GetResponse& response);
[[nodiscard]] std::optional<GetResponse> decode_get_response(
    std::span<const std::byte> payload,
    std::size_t max_data_size = kMaxObjectDataSize);

}  // namespace atlasfs::object_protocol
