#include "atlasfs/protocol.hpp"

#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace atlasfs::protocol {
namespace {

template <typename UInt>
void append_big_endian(std::vector<std::byte>& output, UInt value) {
    static_assert(std::is_unsigned_v<UInt>);
    for (std::size_t index = sizeof(UInt); index > 0; --index) {
        const auto shift = static_cast<unsigned>((index - 1) * 8);
        output.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
    }
}

template <typename UInt>
UInt read_big_endian(std::span<const std::byte> input, std::size_t offset) {
    static_assert(std::is_unsigned_v<UInt>);
    UInt value = 0;
    for (std::size_t index = 0; index < sizeof(UInt); ++index) {
        value = static_cast<UInt>((value << 8U) |
                                  std::to_integer<unsigned>(input[offset + index]));
    }
    return value;
}

bool is_known_type(std::uint16_t raw_type) {
    switch (static_cast<MessageType>(raw_type)) {
        case MessageType::put_request:
        case MessageType::get_request:
        case MessageType::delete_request:
        case MessageType::metadata_request:
        case MessageType::put_response:
        case MessageType::get_response:
        case MessageType::delete_response:
        case MessageType::response:
        case MessageType::error_response:
        case MessageType::storage_prepare_request:
        case MessageType::storage_prepare_response:
        case MessageType::storage_commit_request:
        case MessageType::storage_commit_response:
        case MessageType::storage_abort_request:
        case MessageType::storage_abort_response:
        case MessageType::storage_get_chunk_request:
        case MessageType::storage_get_chunk_response:
        case MessageType::storage_delete_request:
        case MessageType::storage_delete_response:
        case MessageType::storage_query_request:
        case MessageType::storage_query_response:
            return true;
    }
    return false;
}

}  // namespace

std::vector<std::byte> encode(const Message& message) {
    if (!is_known_type(static_cast<std::uint16_t>(message.type))) {
        throw std::invalid_argument("cannot encode an unknown AtlasFS message type");
    }
    if (message.payload.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("AtlasFS payload exceeds the wire format limit");
    }

    std::vector<std::byte> output;
    output.reserve(kHeaderSize + message.payload.size());
    append_big_endian(output, kMagic);
    append_big_endian(output, kVersion);
    append_big_endian(output, static_cast<std::uint16_t>(message.type));
    append_big_endian(output, message.flags);
    append_big_endian(output, message.request_id);
    append_big_endian(output, static_cast<std::uint32_t>(message.payload.size()));
    output.insert(output.end(), message.payload.begin(), message.payload.end());
    return output;
}

StreamParser::StreamParser(std::uint32_t max_payload_size)
    : max_payload_size_(max_payload_size) {}

ParseResult StreamParser::feed(std::span<const std::byte> bytes) {
    if (failed_) {
        return {{}, ParseError::parser_failed};
    }
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());

    ParseResult result;
    std::size_t consumed = 0;
    while (buffer_.size() - consumed >= kHeaderSize) {
        const auto available = std::span<const std::byte>(buffer_).subspan(consumed);
        if (read_big_endian<std::uint32_t>(available, 0) != kMagic) {
            result.error = ParseError::invalid_magic;
            failed_ = true;
            break;
        }
        if (read_big_endian<std::uint16_t>(available, 4) != kVersion) {
            result.error = ParseError::unsupported_version;
            failed_ = true;
            break;
        }
        const auto raw_type = read_big_endian<std::uint16_t>(available, 6);
        if (!is_known_type(raw_type)) {
            result.error = ParseError::unknown_message_type;
            failed_ = true;
            break;
        }
        const auto payload_size = read_big_endian<std::uint32_t>(available, 20);
        if (payload_size > max_payload_size_) {
            result.error = ParseError::payload_too_large;
            failed_ = true;
            break;
        }
        const auto frame_size = kHeaderSize + static_cast<std::size_t>(payload_size);
        if (available.size() < frame_size) {
            break;
        }

        Message message;
        message.type = static_cast<MessageType>(raw_type);
        message.flags = read_big_endian<std::uint32_t>(available, 8);
        message.request_id = read_big_endian<std::uint64_t>(available, 12);
        message.payload.assign(available.begin() + static_cast<std::ptrdiff_t>(kHeaderSize),
                               available.begin() + static_cast<std::ptrdiff_t>(frame_size));
        result.messages.push_back(std::move(message));
        consumed += frame_size;
    }

    if (failed_) {
        buffer_.clear();
    } else if (consumed != 0) {
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(consumed));
    }
    return result;
}

bool StreamParser::has_partial_message() const noexcept {
    return !buffer_.empty();
}

bool StreamParser::failed() const noexcept {
    return failed_;
}

void StreamParser::reset() noexcept {
    buffer_.clear();
    failed_ = false;
}

}  // namespace atlasfs::protocol
