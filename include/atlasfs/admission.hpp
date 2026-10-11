#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stop_token>

namespace atlasfs::scheduling {

// Caller-thread admission, not a task executor. Exactly one lease is active;
// waiting callers enter in mutex-linearized FIFO order. Existing authority,
// catalog, and client-ownership guards are still required inside the lease.
class AdmissionQueue {
    struct State;
public:
    using Clock = std::chrono::steady_clock;
    static constexpr std::size_t max_capacity = 65536;
    enum class Status { admitted, overloaded, cancelled, deadline_expired, shutdown };

    class Lease {
    public:
        Lease() noexcept = default;
        ~Lease();
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept;
        Lease& operator=(Lease&& other) noexcept;
        explicit operator bool() const noexcept { return static_cast<bool>(state_); }
        void release() noexcept;
    private:
        explicit Lease(std::shared_ptr<State> state) noexcept;
        std::shared_ptr<State> state_;
        friend class AdmissionQueue;
    };
    struct Result {
        Status status{Status::shutdown};
        Lease lease;
        // Diagnostic admission order, not a durable operation identifier.
        // Zero means rejection before joining; this sequence saturates.
        std::uint64_t ticket{};
    };
    struct Snapshot {
        std::size_t capacity{}, waiting{}, peak_waiting{};
        bool active{};
        std::uint64_t admitted{}, overloaded{}, cancelled{}, deadline_expired{}, shutdown_rejected{};
    };

    // Capacity counts waiting callers, not the active lease. Zero permits one
    // immediate operation but no waiters. Recursive acquisition is unsupported.
    explicit AdmissionQueue(std::size_t capacity);
    ~AdmissionQueue();
    AdmissionQueue(const AdmissionQueue&) = delete;
    AdmissionQueue& operator=(const AdmissionQueue&) = delete;
    AdmissionQueue(AdmissionQueue&&) = delete;
    AdmissionQueue& operator=(AdmissionQueue&&) = delete;

    [[nodiscard]] Result acquire(Clock::time_point deadline = Clock::time_point::max(),
                                 std::stop_token cancellation = {});
    [[nodiscard]] Snapshot snapshot() const;
    // Reject new/pending calls, without preempting an active operation. After
    // grant, cancellation/deadlines do not revoke the lease or undo remote I/O.
    void shutdown() noexcept;
    [[nodiscard]] bool wait_idle(Clock::time_point deadline = Clock::time_point::max()) const;
private:
    std::shared_ptr<State> state_;
};

}  // namespace atlasfs::scheduling
