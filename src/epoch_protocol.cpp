#include "atlasfs/epoch_protocol.hpp"
#include <stdexcept>

namespace atlasfs::epoch {
namespace {
template<class T> void append(std::vector<std::byte>& out, T value) {
    for (std::size_t i = sizeof(T); i > 0; --i) out.push_back(static_cast<std::byte>((value >> ((i - 1) * 8U)) & 0xffU));
}
template<class T> bool take(std::span<const std::byte> in, std::size_t& at, T& value) {
    if (at > in.size() || in.size() - at < sizeof(T)) return false;
    value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) value = static_cast<T>((value << 8U) | std::to_integer<unsigned>(in[at++]));
    return true;
}
std::vector<std::byte> prefix() { return {std::byte{0}, std::byte{3}, std::byte{0}, std::byte{0}}; }
bool prefix(std::span<const std::byte> in, std::size_t& at) {
    std::uint16_t version{}, reserved{};
    return take(in, at, version) && version == 3 && take(in, at, reserved) && reserved == 0;
}
void token(std::vector<std::byte>& out, const Token& value) {
    const auto bytes = encode_token(value); append(out, static_cast<std::uint16_t>(bytes.size()));
    out.insert(out.end(), bytes.begin(), bytes.end());
}
bool token(std::span<const std::byte> in, std::size_t& at, Token& value) {
    std::uint16_t size{};
    if (!take(in, at, size) || size > 302 || in.size() - at < size) return false;
    const auto decoded = decode_token(in.subspan(at, size));
    if (!decoded) return false;
    value = *decoded; at += size; return true;
}
}
bool requires_epoch(protocol::MessageType type) noexcept {
    using T = protocol::MessageType;
    return type == T::storage_prepare_request || type == T::storage_commit_request ||
        type == T::storage_abort_request || type == T::storage_delete_request || type == T::storage_repair_chunk_request;
}
std::vector<std::byte> encode_request(const Token& value, std::span<const std::byte> payload) {
    if (!valid_token(value) || payload.size() > protocol::kDefaultMaxPayloadSize - 320)
        throw std::invalid_argument("invalid epoch request");
    auto out = prefix(); token(out, value); append(out, static_cast<std::uint32_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end()); return out;
}
std::optional<Request> decode_request(std::span<const std::byte> in) {
    Request value; std::size_t at{}; std::uint32_t size{};
    if (in.size() > protocol::kDefaultMaxPayloadSize || !prefix(in, at) || !token(in, at, value.token) ||
        !valid_token(value.token) || !take(in, at, size) || size != in.size() - at) return std::nullopt;
    value.payload.assign(in.begin() + static_cast<std::ptrdiff_t>(at), in.end()); return value;
}
std::vector<std::byte> encode_activation(const Activation& value) {
    if (!valid_token(value.expected, true) || !valid_token(value.desired)) throw std::invalid_argument("invalid epoch activation");
    auto out = prefix(); token(out, value.expected); token(out, value.desired); return out;
}
std::optional<Activation> decode_activation(std::span<const std::byte> in) {
    Activation value; std::size_t at{};
    return prefix(in, at) && token(in, at, value.expected) && token(in, at, value.desired) &&
        valid_token(value.desired) && at == in.size() ? std::optional<Activation>{value} : std::nullopt;
}
std::vector<std::byte> encode_result(const Result& value) {
    if (value.status > Status::invalid || (value.status == Status::accepted && !value.token))
        throw std::invalid_argument("invalid epoch result");
    auto out = prefix(); append(out, static_cast<std::uint16_t>(value.status));
    if (value.token) token(out, *value.token); else append(out, std::uint16_t{0});
    return out;
}
std::optional<Result> decode_result(std::span<const std::byte> in) {
    Result value; std::size_t at{}; std::uint16_t status{}, size{};
    if (!prefix(in, at) || !take(in, at, status) || status > static_cast<std::uint16_t>(Status::invalid)) return std::nullopt;
    value.status = static_cast<Status>(status);
    const auto saved = at;
    if (!take(in, at, size)) return std::nullopt;
    if (size != 0) { at = saved; Token t; if (!token(in, at, t)) return std::nullopt; value.token = t; }
    return at == in.size() && (value.status != Status::accepted || value.token) ? std::optional<Result>{value} : std::nullopt;
}
} // namespace atlasfs::epoch
