#pragma once

#include "atlasfs/protocol.hpp"

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace atlasfs::net {
using Deadline = std::chrono::steady_clock::time_point;
enum class PeerRole { coordinator, storage };
struct PeerIdentity {
    std::string id;
    PeerRole role{PeerRole::storage};
    bool operator==(const PeerIdentity&) const = default;
};
struct PeerPolicy {
    PeerIdentity identity;
    std::set<std::string, std::less<>> targets;
};
struct TlsConfig {
    std::filesystem::path certificate_file;
    std::filesystem::path private_key_file;
    std::filesystem::path trust_file;
    std::filesystem::path crl_file;
    PeerIdentity local_identity;
    std::map<std::string, PeerPolicy, std::less<>> peers;
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};
class TransportError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};
[[nodiscard]] std::string certificate_identity(const PeerIdentity& identity);
[[nodiscard]] bool authorize_storage(const PeerPolicy& peer,
    std::string_view target, protocol::MessageType operation) noexcept;
void wait_socket(int descriptor, bool writing, Deadline deadline);
void make_nonblocking(int descriptor);

class ByteStream {
public:
    virtual ~ByteStream() = default;
    [[nodiscard]] virtual std::size_t read(std::span<std::byte> buffer, Deadline deadline) = 0;
    virtual void write_all(std::span<const std::byte> bytes, Deadline deadline) = 0;
    [[nodiscard]] virtual const PeerPolicy* peer() const noexcept { return nullptr; }
};
[[nodiscard]] std::unique_ptr<ByteStream> plaintext_stream(int descriptor);
class TlsContext {
public:
    explicit TlsContext(TlsConfig config);
    ~TlsContext();
    TlsContext(const TlsContext&) = delete;
    TlsContext& operator=(const TlsContext&) = delete;
    [[nodiscard]] std::unique_ptr<ByteStream> connect(int descriptor,
        const PeerIdentity& expected) const;
    [[nodiscard]] std::unique_ptr<ByteStream> accept(int descriptor) const;
    [[nodiscard]] std::chrono::milliseconds timeout() const noexcept;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace atlasfs::net
