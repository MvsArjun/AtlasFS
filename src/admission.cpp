#include "atlasfs/admission.hpp"

#include <algorithm>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

namespace atlasfs::scheduling {

struct AdmissionQueue::State {
    struct Waiter {
        Waiter* previous{};
        Waiter* next{};
        std::uint64_t ticket{};
        bool linked{};
    };
    explicit State(std::size_t capacity) { stats.capacity = capacity; }
    static void increment(std::uint64_t& value) noexcept {
        if (value != std::numeric_limits<std::uint64_t>::max()) ++value;
    }
    std::uint64_t ticket() noexcept { increment(last_ticket); return last_ticket; }
    void append(Waiter& waiter) noexcept {
        waiter.previous = tail;
        if (tail) tail->next = &waiter;
        else head = &waiter;
        tail = &waiter;
        waiter.linked = true;
        ++stats.waiting;
        stats.peak_waiting = std::max(stats.peak_waiting, stats.waiting);
    }
    void erase(Waiter& waiter) noexcept {
        if (!waiter.linked) return;
        if (waiter.previous) waiter.previous->next = waiter.next;
        else head = waiter.next;
        if (waiter.next) waiter.next->previous = waiter.previous;
        else tail = waiter.previous;
        waiter.linked = false;
        --stats.waiting;
    }
    Result reject(Status status, std::uint64_t sequence = 0) noexcept {
        switch (status) {
            case Status::overloaded: increment(stats.overloaded); break;
            case Status::cancelled: increment(stats.cancelled); break;
            case Status::deadline_expired: increment(stats.deadline_expired); break;
            case Status::shutdown: increment(stats.shutdown_rejected); break;
            case Status::admitted: break;
        }
        return {status, {}, sequence};
    }
    std::mutex mutex;
    std::condition_variable changed;
    Snapshot stats;
    Waiter* head{};
    Waiter* tail{};
    std::uint64_t last_ticket{};
    bool closing{};
};

AdmissionQueue::Lease::Lease(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
AdmissionQueue::Lease::~Lease() { release(); }
AdmissionQueue::Lease::Lease(Lease&& other) noexcept : state_(std::move(other.state_)) {}
AdmissionQueue::Lease& AdmissionQueue::Lease::operator=(Lease&& other) noexcept {
    if (this != &other) { release(); state_ = std::move(other.state_); }
    return *this;
}
void AdmissionQueue::Lease::release() noexcept {
    auto state = std::move(state_);
    if (!state) return;
    { std::lock_guard lock(state->mutex); state->stats.active = false; }
    state->changed.notify_all();
}

AdmissionQueue::AdmissionQueue(std::size_t capacity) {
    if (capacity > max_capacity) throw std::invalid_argument("admission waiting capacity exceeds 65536");
    state_ = std::make_shared<State>(capacity);
}
AdmissionQueue::~AdmissionQueue() { shutdown(); }

AdmissionQueue::Result AdmissionQueue::acquire(Clock::time_point deadline,
                                               std::stop_token cancellation) {
    const auto state = state_;
    State::Waiter waiter;
    // Construct before taking the mutex: an already-requested token invokes
    // synchronously. Destruction must also occur after the lock is released.
    // Taking the mutex in the callback prevents a lost notify between checking
    // cancellation and atomically releasing the mutex in condition_variable.
    std::stop_callback stopped(cancellation, [state] {
        std::lock_guard lock(state->mutex);
        state->changed.notify_all();
    });
    std::unique_lock lock(state->mutex);
    const auto rejection = [&]() -> std::optional<Status> {
        if (state->closing) return Status::shutdown;
        if (cancellation.stop_requested()) return Status::cancelled;
        if (deadline != Clock::time_point::max() && Clock::now() >= deadline)
            return Status::deadline_expired;
        return std::nullopt;
    };
    if (const auto status = rejection()) return state->reject(*status);
    if (!state->stats.active && !state->head) {
        state->stats.active = true;
        State::increment(state->stats.admitted);
        return {Status::admitted, Lease(state), state->ticket()};
    }
    if (state->stats.waiting == state->stats.capacity) return state->reject(Status::overloaded);
    waiter.ticket = state->ticket();
    state->append(waiter);
    try {
        for (;;) {
            if (const auto status = rejection()) {
                state->erase(waiter);
                state->changed.notify_all();
                return state->reject(*status, waiter.ticket);
            }
            if (!state->stats.active && state->head == &waiter) {
                state->erase(waiter);
                state->stats.active = true;
                State::increment(state->stats.admitted);
                return {Status::admitted, Lease(state), waiter.ticket};
            }
            if (deadline == Clock::time_point::max()) state->changed.wait(lock);
            else state->changed.wait_until(lock, deadline);
        }
    } catch (...) {
        // No stack waiter may remain linked after a condition-variable error.
        state->erase(waiter);
        state->changed.notify_all();
        throw;
    }
}

AdmissionQueue::Snapshot AdmissionQueue::snapshot() const {
    const auto state = state_;
    std::lock_guard lock(state->mutex);
    return state->stats;
}
void AdmissionQueue::shutdown() noexcept {
    const auto state = state_;
    { std::lock_guard lock(state->mutex); state->closing = true; }
    state->changed.notify_all();
}
bool AdmissionQueue::wait_idle(Clock::time_point deadline) const {
    const auto state = state_;
    std::unique_lock lock(state->mutex);
    const auto idle = [&] { return !state->stats.active && state->stats.waiting == 0; };
    if (deadline == Clock::time_point::max()) { state->changed.wait(lock, idle); return true; }
    return state->changed.wait_until(lock, deadline, idle);
}

}  // namespace atlasfs::scheduling
