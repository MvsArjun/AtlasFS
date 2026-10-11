#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace atlasfs::epoch {
struct Token {
    std::string authority;
    std::uint64_t number{};
    std::string coordinator;
    std::string activation;
    bool operator==(const Token&) const = default;
};
enum class Status : std::uint16_t { accepted, stale, conflict, unauthorized, unresolved, invalid };
class Error : public std::runtime_error {
public:
    explicit Error(Status status) : std::runtime_error("epoch authorization unavailable"), status_(status) {}
    Status status() const noexcept { return status_; }
private: Status status_;
};
struct Result {
    Status status{Status::unresolved};
    std::optional<Token> token;
};
struct State {
    Token current;
    std::optional<Token> proposed;
    std::vector<std::string> participants;
    bool operator==(const State&) const = default;
};
enum class FaultPoint { creation, write, file_sync, rename, directory_sync };
using FaultInjector = std::function<bool(FaultPoint)>;
bool valid_token(const Token& token, bool allow_zero = false) noexcept;
std::vector<std::byte> encode_token(const Token& token);
std::optional<Token> decode_token(std::span<const std::byte> bytes);
std::vector<std::byte> encode_state(const State& state);
std::optional<State> decode_state(std::span<const std::byte> bytes);
class Guard {
public:
    ~Guard();
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
private:
    struct Impl;
    explicit Guard(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
    friend class Store;
};
class Store {
public:
    // Explicit offline enrollment only. Never called implicitly by startup.
    static void provision(const std::filesystem::path& directory, const Token& initial);
    Store(std::filesystem::path directory, Token minimum, FaultInjector fault = {});
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;
    State snapshot() const;
    std::unique_ptr<Guard> authorize(const Token& token) const;
    Result advance(const Token& expected, const Token& desired);
    Result begin(const Token& desired, std::vector<std::string> participants);
    const std::filesystem::path& directory() const noexcept;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::unique_ptr<Guard> lock() const;
    State read_locked() const;
    Status publish_locked(const State& state);
    Result finish(const Token& desired);
    friend class Coordinator;
};
class Participant {
public:
    virtual ~Participant() = default;
    virtual std::string_view id() const noexcept = 0;
    virtual Result activate_epoch(const Token& expected, const Token& desired) = 0;
    virtual Result query_epoch() const = 0;
};
class Coordinator {
public:
    Coordinator(Store& store, std::vector<std::shared_ptr<Participant>> participants);
    Result activate(std::string coordinator, std::string activation);
    Result reconcile();
    Result resume_activation();
private:
    Result confirm(const State& state, bool activate_nodes);
    Store& store_;
    std::vector<std::shared_ptr<Participant>> participants_;
};
} // namespace atlasfs::epoch
