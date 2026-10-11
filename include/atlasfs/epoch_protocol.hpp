#pragma once
#include "atlasfs/epoch.hpp"
#include "atlasfs/protocol.hpp"

namespace atlasfs::epoch {
struct Request { Token token; std::vector<std::byte> payload; };
struct Activation { Token expected; Token desired; };
bool requires_epoch(protocol::MessageType type) noexcept;
std::vector<std::byte> encode_request(const Token& token, std::span<const std::byte> payload);
std::optional<Request> decode_request(std::span<const std::byte> bytes);
std::vector<std::byte> encode_activation(const Activation& value);
std::optional<Activation> decode_activation(std::span<const std::byte> bytes);
std::vector<std::byte> encode_result(const Result& value);
std::optional<Result> decode_result(std::span<const std::byte> bytes);
} // namespace atlasfs::epoch
