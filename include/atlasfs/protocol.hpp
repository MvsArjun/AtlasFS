#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace atlasfs::protocol {

inline constexpr std::uint32_t kMagic = 0x41544653U;  // "ATFS"
inline constexpr std::uint16_t kVersion = 1;
inline constexpr std::size_t kHeaderSize = 24;
inline constexpr std::uint32_t kDefaultMaxPayloadSize = 16U * 1024U * 1024U;

enum class MessageType : std::uint16_t {
    put_request = 1,
    get_request = 2,
    delete_request = 3,
    metadata_request = 4,
    put_response = 10,
    get_response = 11,
    delete_response = 12,
    response = 100,
    error_response = 101,
    storage_prepare_request = 200,
    storage_prepare_response = 201,
    storage_commit_request = 202,
    storage_commit_response = 203,
    storage_abort_request = 204,
    storage_abort_response = 205,
    storage_get_chunk_request = 206,
    storage_get_chunk_response = 207,
    storage_delete_request = 208,
    storage_delete_response = 209,
    storage_query_request = 210,
    storage_query_response = 211,
    storage_repair_chunk_request = 212,
    storage_repair_chunk_response = 213,
};

struct Message {
    MessageType type{};
    std::uint32_t flags{};
    std::uint64_t request_id{};
    std::vector<std::byte> payload;

    bool operator==(const Message&) const = default;
};

enum class ParseError {
    none,
    invalid_magic,
    unsupported_version,
    unknown_message_type,
    payload_too_large,
    parser_failed,
};

struct ParseResult {
    std::vector<Message> messages;
    ParseError error{ParseError::none};
};

[[nodiscard]] std::vector<std::byte> encode(const Message& message);

class StreamParser {
public:
    explicit StreamParser(std::uint32_t max_payload_size = kDefaultMaxPayloadSize);

    [[nodiscard]] ParseResult feed(std::span<const std::byte> bytes);
    [[nodiscard]] bool has_partial_message() const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    void reset() noexcept;

private:
    std::vector<std::byte> buffer_;
    std::uint32_t max_payload_size_;
    bool failed_{false};
};

}  // namespace atlasfs::protocol
