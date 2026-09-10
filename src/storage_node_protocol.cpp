#include "atlasfs/storage_node_protocol.hpp"

#include "atlasfs/object_protocol.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace atlasfs::storage_node_protocol {
namespace {

template <class T> void append(std::vector<std::byte>& out, T value) {
    static_assert(std::is_unsigned_v<T>);
    for (std::size_t i = sizeof(T); i > 0; --i) {
        out.push_back(static_cast<std::byte>((value >> ((i - 1) * 8U)) & 0xffU));
    }
}

template <class T> bool take(std::span<const std::byte> in, std::size_t& at, T& value) {
    static_assert(std::is_unsigned_v<T>);
    if (in.size() - std::min(in.size(), at) < sizeof(T)) { return false; }
    value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        value = static_cast<T>((value << 8U) | std::to_integer<unsigned>(in[at++]));
    }
    return true;
}

void append_string(std::vector<std::byte>& out, std::string_view value) {
    if (value.size() > std::numeric_limits<std::uint16_t>::max()) {
        throw std::length_error("storage-node identifier too long");
    }
    append(out, static_cast<std::uint16_t>(value.size()));
    const auto* begin = reinterpret_cast<const std::byte*>(value.data());
    out.insert(out.end(), begin, begin + value.size());
}

bool take_string(std::span<const std::byte> in, std::size_t& at, std::size_t limit,
                 std::string& value) {
    std::uint16_t size{};
    if (!take(in, at, size) || size == 0 || size > limit || in.size() - at < size) { return false; }
    value.assign(reinterpret_cast<const char*>(in.data() + at), size);
    at += size;
    return true;
}

void prefix(std::vector<std::byte>& out) { append(out, kVersion); }

bool take_prefix(std::span<const std::byte> in, std::size_t& at) {
    std::uint16_t version{};
    return take(in, at, version) && version == kVersion;
}

bool valid_generation_id(std::string_view value) {
    if (value.empty() || value.size() > storage::kMaxGenerationIdSize) return false;
    for (const unsigned char c : value) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-')) return false;
    }
    return true;
}

}  // namespace

bool valid_operation_id(std::string_view value) noexcept {
    if (value.empty() || value.size() > kMaxOperationIdSize) { return false; }
    for (const unsigned char c : value) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-' || c == '_')) { return false; }
    }
    return true;
}

std::vector<std::byte> encode_prepare(const PrepareRequest& request) {
    if (!valid_operation_id(request.operation_id) ||
        !storage::ObjectStore::valid_object_id(request.object_id) ||
        request.chunk_indices.size() > storage::kMaxChunkCount ||
        request.object_data.size() > object_protocol::kMaxObjectDataSize) {
        throw std::invalid_argument("invalid storage-node prepare identifiers");
    }
    const auto manifest = storage::encode_manifest(request.manifest);
    std::vector<std::byte> out; prefix(out);
    append_string(out, request.operation_id); append_string(out, request.object_id);
    append(out, static_cast<std::uint32_t>(manifest.size()));
    out.insert(out.end(), manifest.begin(), manifest.end());
    append(out, static_cast<std::uint32_t>(request.chunk_indices.size()));
    for (const auto index : request.chunk_indices) { append(out, index); }
    append(out, static_cast<std::uint32_t>(request.object_data.size()));
    out.insert(out.end(), request.object_data.begin(), request.object_data.end());
    return out;
}

std::optional<PrepareRequest> decode_prepare(std::span<const std::byte> bytes) {
    PrepareRequest value; std::size_t at = 0; std::uint32_t size{};
    if (!take_prefix(bytes, at) ||
        !take_string(bytes, at, kMaxOperationIdSize, value.operation_id) ||
        !take_string(bytes, at, object_protocol::kMaxObjectIdSize, value.object_id) ||
        !valid_operation_id(value.operation_id) ||
        !storage::ObjectStore::valid_object_id(value.object_id) || !take(bytes, at, size) ||
        size > bytes.size() - at) return std::nullopt;
    const auto manifest = storage::decode_manifest(bytes.subspan(at, size));
    if (!manifest) return std::nullopt;
    value.manifest = *manifest; at += size;
    std::uint32_t count{};
    if (!take(bytes, at, count) || count > storage::kMaxChunkCount) return std::nullopt;
    value.chunk_indices.reserve(count);
    for (std::uint32_t i = 0, index{}; i < count; ++i) {
        if (!take(bytes, at, index)) return std::nullopt;
        value.chunk_indices.push_back(index);
    }
    if (!take(bytes, at, size) || size > object_protocol::kMaxObjectDataSize ||
        size != bytes.size() - at) return std::nullopt;
    value.object_data.assign(bytes.begin() + static_cast<std::ptrdiff_t>(at), bytes.end());
    return value;
}

std::vector<std::byte> encode_identity(const IdentityRequest& request) {
    if (!valid_operation_id(request.operation_id) ||
        !storage::ObjectStore::valid_object_id(request.object_id) ||
        !valid_generation_id(request.generation_id))
        throw std::invalid_argument("invalid operation identity");
    std::vector<std::byte> out; prefix(out); append_string(out, request.operation_id);
    append_string(out, request.object_id); append_string(out, request.generation_id); return out;
}

std::optional<IdentityRequest> decode_identity(std::span<const std::byte> bytes) {
    IdentityRequest value; std::size_t at = 0;
    if (!take_prefix(bytes, at) ||
        !take_string(bytes, at, kMaxOperationIdSize, value.operation_id) ||
        !take_string(bytes, at, object_protocol::kMaxObjectIdSize, value.object_id) ||
        !take_string(bytes, at, storage::kMaxGenerationIdSize, value.generation_id) ||
        at != bytes.size() || !valid_operation_id(value.operation_id) ||
        !storage::ObjectStore::valid_object_id(value.object_id) ||
        !valid_generation_id(value.generation_id)) return std::nullopt;
    return value;
}

std::vector<std::byte> encode_get_chunk(const GetChunkRequest& request) {
    if (!storage::ObjectStore::valid_object_id(request.object_id) ||
        !valid_generation_id(request.generation_id) ||
        request.chunk_index >= storage::kMaxChunkCount)
        throw std::invalid_argument("invalid chunk identity");
    std::vector<std::byte> out; prefix(out); append_string(out, request.object_id);
    append_string(out, request.generation_id); append(out, request.chunk_index); return out;
}

std::optional<GetChunkRequest> decode_get_chunk(std::span<const std::byte> bytes) {
    GetChunkRequest value; std::size_t at = 0;
    if (!take_prefix(bytes, at) ||
        !take_string(bytes, at, object_protocol::kMaxObjectIdSize, value.object_id) ||
        !take_string(bytes, at, storage::kMaxGenerationIdSize, value.generation_id) ||
        !take(bytes, at, value.chunk_index) || at != bytes.size() ||
        value.chunk_index >= storage::kMaxChunkCount ||
        !storage::ObjectStore::valid_object_id(value.object_id) ||
        !valid_generation_id(value.generation_id)) return std::nullopt;
    return value;
}

std::vector<std::byte> encode_response(const Response& response) {
    std::vector<std::byte> out; prefix(out);
    append(out, static_cast<std::uint16_t>(response.error));
    append_string(out, response.operation_id.empty() ? "-" : response.operation_id);
    append(out, static_cast<std::uint8_t>(response.state));
    append(out, response.fingerprint);
    const bool has_chunk = response.error == storage::StoreError::none &&
                           !response.chunk.manifest.generation_id.empty();
    append(out, static_cast<std::uint8_t>(has_chunk));
    if (has_chunk) {
        const auto manifest = storage::encode_manifest(response.chunk.manifest);
        append(out, static_cast<std::uint32_t>(manifest.size()));
        out.insert(out.end(), manifest.begin(), manifest.end());
        append(out, response.chunk.chunk.index); append(out, response.chunk.chunk.length);
        append(out, response.chunk.chunk.checksum);
        append(out, static_cast<std::uint32_t>(response.chunk.data.size()));
        out.insert(out.end(), response.chunk.data.begin(), response.chunk.data.end());
    }
    return out;
}

std::optional<Response> decode_response(std::span<const std::byte> bytes) {
    Response value; std::size_t at = 0; std::uint16_t error{}; std::uint8_t state{}, has{};
    if (!take_prefix(bytes, at) || !take(bytes, at, error) ||
        !take_string(bytes, at, kMaxOperationIdSize, value.operation_id) ||
        !take(bytes, at, state) || state > static_cast<std::uint8_t>(OperationState::aborted) ||
        !take(bytes, at, value.fingerprint) ||
        !take(bytes, at, has) || has > 1) return std::nullopt;
    value.error = static_cast<storage::StoreError>(error);
    value.state = static_cast<OperationState>(state);
    if (has) {
        std::uint32_t size{};
        if (!take(bytes, at, size) || size > bytes.size() - at) return std::nullopt;
        auto manifest = storage::decode_manifest(bytes.subspan(at, size));
        if (!manifest) return std::nullopt;
        value.chunk.manifest = *manifest; at += size;
        if (!take(bytes, at, value.chunk.chunk.index) ||
            !take(bytes, at, value.chunk.chunk.length) ||
            !take(bytes, at, value.chunk.chunk.checksum) || !take(bytes, at, size) ||
            size != bytes.size() - at) return std::nullopt;
        value.chunk.data.assign(bytes.begin() + static_cast<std::ptrdiff_t>(at), bytes.end());
        value.chunk.error = value.error;
    }
    if (at != bytes.size() && !has) return std::nullopt;
    return value;
}

}  // namespace atlasfs::storage_node_protocol
