#pragma once

#include <cstddef>
#include <chrono>
#include <cstdint>
#include <span>

namespace atlasfs::net {

class Socket {
public:
    Socket() noexcept = default;
    explicit Socket(int descriptor) noexcept;
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    [[nodiscard]] int get() const noexcept;
    [[nodiscard]] bool valid() const noexcept;
    int release() noexcept;
    void reset(int descriptor = -1) noexcept;

private:
    int descriptor_{-1};
};

[[nodiscard]] Socket connect_loopback(std::uint16_t port,
    std::chrono::milliseconds timeout = std::chrono::seconds(5));
void send_all(int descriptor, std::span<const std::byte> bytes);

}  // namespace atlasfs::net
