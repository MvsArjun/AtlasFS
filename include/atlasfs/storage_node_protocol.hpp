#pragma once

#include "atlasfs/manifest.hpp"
#include "atlasfs/object_store.hpp"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::storage_node_protocol {

inline constexpr std::uint16_t kVersion = 1;
inline constexpr std::size_t kMaxOperationIdSize = 96;

enum class OperationState : std::uint8_t { unknown, prepared, committed, aborted };

struct PrepareRequest {
    std::string operation_id;
    std::string object_id;
    storage::ObjectManifest manifest;
    std::vector<std::uint32_t> chunk_indices;
    std::vector<std::byte> object_data;
};

struct IdentityRequest {
    std::string operation_id;
    std::string object_id;
    std::string generation_id;
};

struct GetChunkRequest {
    std::string object_id;
    std::string generation_id;
    std::uint32_t chunk_index{};
};

struct Response {
    storage::StoreError error{storage::StoreError::none};
    std::string operation_id;
    OperationState state{OperationState::unknown};
    storage::ChunkResult chunk;
};

[[nodiscard]] bool valid_operation_id(std::string_view value) noexcept;
[[nodiscard]] std::vector<std::byte> encode_prepare(const PrepareRequest& request);
[[nodiscard]] std::optional<PrepareRequest> decode_prepare(std::span<const std::byte> bytes);
[[nodiscard]] std::vector<std::byte> encode_identity(const IdentityRequest& request);
[[nodiscard]] std::optional<IdentityRequest> decode_identity(std::span<const std::byte> bytes);
[[nodiscard]] std::vector<std::byte> encode_get_chunk(const GetChunkRequest& request);
[[nodiscard]] std::optional<GetChunkRequest> decode_get_chunk(std::span<const std::byte> bytes);
[[nodiscard]] std::vector<std::byte> encode_response(const Response& response);
[[nodiscard]] std::optional<Response> decode_response(std::span<const std::byte> bytes);

}  // namespace atlasfs::storage_node_protocol
