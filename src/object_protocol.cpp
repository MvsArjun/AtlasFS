#include "atlasfs/object_protocol.hpp"

#include "atlasfs/checksum.hpp"

#include <limits>
#include <stdexcept>
#include <type_traits>

namespace atlasfs::object_protocol {
namespace {

template <typename UInt>
void append_integer(std::vector<std::byte>& output, UInt value) {
    static_assert(std::is_unsigned_v<UInt>);
    for (std::size_t index = sizeof(UInt); index > 0; --index) {
        const auto shift = static_cast<unsigned>((index - 1U) * 8U);
        output.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
    }
}

template <typename UInt>
std::optional<UInt> read_integer(std::span<const std::byte> input, std::size_t& offset) {
    static_assert(std::is_unsigned_v<UInt>);
    if (offset > input.size() || input.size() - offset < sizeof(UInt)) {
        return std::nullopt;
    }
    UInt value = 0;
    for (std::size_t index = 0; index < sizeof(UInt); ++index) {
        value = static_cast<UInt>((value << 8U) |
                                  std::to_integer<unsigned>(input[offset + index]));
    }
    offset += sizeof(UInt);
    return value;
}

void append_string(std::vector<std::byte>& output, const std::string& value) {
    const auto* begin = reinterpret_cast<const std::byte*>(value.data());
    output.insert(output.end(), begin, begin + value.size());
}

std::string read_string(std::span<const std::byte> input, std::size_t offset,
                        std::size_t length) {
    const auto* begin = reinterpret_cast<const char*>(input.data() + offset);
    return {begin, length};
}

bool valid_status(std::uint16_t raw) {
    return raw <= static_cast<std::uint16_t>(Status::malformed_request);
}

}  // namespace

bool valid_object_id(std::string_view object_id) noexcept {
    if (object_id.empty() || object_id.size() > kMaxObjectIdSize || object_id == "." ||
        object_id == ".." || object_id.starts_with(".atlasfs-tmp-")) {
        return false;
    }
    for (const unsigned char character : object_id) {
        const bool allowed = (character >= 'a' && character <= 'z') ||
                             (character >= 'A' && character <= 'Z') ||
                             (character >= '0' && character <= '9') || character == '-' ||
                             character == '_' || character == '.';
        if (!allowed) {
            return false;
        }
    }
    return true;
}

std::vector<std::byte> encode_put_request(const PutRequest& request) {
    if (request.object_id.size() > kMaxObjectIdSize ||
        request.object_id.size() > std::numeric_limits<std::uint16_t>::max()) {
        throw std::length_error("object identifier exceeds the protocol limit");
    }
    if (request.data.size() > kMaxObjectDataSize ||
        request.data.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("object data exceeds the protocol limit");
    }
    std::vector<std::byte> output;
    output.reserve(2U + 4U + request.object_id.size() + request.data.size() + 4U);
    append_integer(output, static_cast<std::uint16_t>(request.object_id.size()));
    append_integer(output, static_cast<std::uint32_t>(request.data.size()));
    append_string(output, request.object_id);
    output.insert(output.end(), request.data.begin(), request.data.end());
    append_integer(output, request.checksum);
    return output;
}

std::optional<PutRequest> decode_put_request(std::span<const std::byte> payload,
                                             std::size_t max_data_size) {
    std::size_t offset = 0;
    const auto id_size = read_integer<std::uint16_t>(payload, offset);
    const auto data_size = read_integer<std::uint32_t>(payload, offset);
    if (!id_size || !data_size || *id_size > kMaxObjectIdSize ||
        *data_size > max_data_size) {
        return std::nullopt;
    }
    constexpr std::size_t checksum_size = sizeof(std::uint32_t);
    const auto remaining = payload.size() - offset;
    const auto variable_size = static_cast<std::size_t>(*id_size) +
                               static_cast<std::size_t>(*data_size);
    if (variable_size > remaining || remaining - variable_size != checksum_size) {
        return std::nullopt;
    }

    PutRequest request;
    request.object_id = read_string(payload, offset, *id_size);
    offset += *id_size;
    request.data.assign(payload.begin() + static_cast<std::ptrdiff_t>(offset),
                        payload.begin() + static_cast<std::ptrdiff_t>(offset + *data_size));
    offset += *data_size;
    const auto checksum = read_integer<std::uint32_t>(payload, offset);
    if (!checksum || offset != payload.size()) {
        return std::nullopt;
    }
    request.checksum = *checksum;
    return request;
}

std::vector<std::byte> encode_object_id_request(const ObjectIdRequest& request) {
    if (request.object_id.size() > kMaxObjectIdSize ||
        request.object_id.size() > std::numeric_limits<std::uint16_t>::max()) {
        throw std::length_error("object identifier exceeds the protocol limit");
    }
    std::vector<std::byte> output;
    output.reserve(2U + request.object_id.size());
    append_integer(output, static_cast<std::uint16_t>(request.object_id.size()));
    append_string(output, request.object_id);
    return output;
}

std::optional<ObjectIdRequest> decode_object_id_request(
    std::span<const std::byte> payload) {
    std::size_t offset = 0;
    const auto id_size = read_integer<std::uint16_t>(payload, offset);
    if (!id_size || *id_size > kMaxObjectIdSize || payload.size() - offset != *id_size) {
        return std::nullopt;
    }
    return ObjectIdRequest{read_string(payload, offset, *id_size)};
}

std::vector<std::byte> encode_status_response(Status status) {
    std::vector<std::byte> output;
    output.reserve(2);
    append_integer(output, static_cast<std::uint16_t>(status));
    return output;
}

std::optional<Status> decode_status_response(std::span<const std::byte> payload) {
    std::size_t offset = 0;
    const auto raw = read_integer<std::uint16_t>(payload, offset);
    if (!raw || offset != payload.size() || !valid_status(*raw)) {
        return std::nullopt;
    }
    return static_cast<Status>(*raw);
}

std::vector<std::byte> encode_get_response(const GetResponse& response) {
    if (response.data.size() > kMaxObjectDataSize ||
        response.data.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("object data exceeds the protocol limit");
    }
    auto output = encode_status_response(response.status);
    if (response.status != Status::success) {
        if (!response.data.empty() || response.checksum != 0) {
            throw std::invalid_argument("failed GET response cannot contain object data");
        }
        return output;
    }
    output.reserve(2U + 4U + response.data.size() + 4U);
    append_integer(output, static_cast<std::uint32_t>(response.data.size()));
    output.insert(output.end(), response.data.begin(), response.data.end());
    append_integer(output, response.checksum);
    return output;
}

std::optional<GetResponse> decode_get_response(std::span<const std::byte> payload,
                                                std::size_t max_data_size) {
    std::size_t offset = 0;
    const auto raw_status = read_integer<std::uint16_t>(payload, offset);
    if (!raw_status || !valid_status(*raw_status)) {
        return std::nullopt;
    }
    const auto status = static_cast<Status>(*raw_status);
    if (status != Status::success) {
        if (offset != payload.size()) {
            return std::nullopt;
        }
        return GetResponse{status, {}, 0};
    }
    const auto data_size = read_integer<std::uint32_t>(payload, offset);
    if (!data_size || *data_size > max_data_size) {
        return std::nullopt;
    }
    constexpr std::size_t checksum_size = sizeof(std::uint32_t);
    const auto remaining = payload.size() - offset;
    if (*data_size > remaining || remaining - *data_size != checksum_size) {
        return std::nullopt;
    }
    GetResponse response;
    response.status = status;
    response.data.assign(payload.begin() + static_cast<std::ptrdiff_t>(offset),
                         payload.begin() + static_cast<std::ptrdiff_t>(offset + *data_size));
    offset += *data_size;
    const auto checksum = read_integer<std::uint32_t>(payload, offset);
    if (!checksum || offset != payload.size()) {
        return std::nullopt;
    }
    response.checksum = *checksum;
    if (integrity::crc32c(response.data) != response.checksum) {
        return std::nullopt;
    }
    return response;
}

}  // namespace atlasfs::object_protocol
