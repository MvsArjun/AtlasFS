#include "atlasfs/admission.hpp"

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <barrier>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace atlasfs::scheduling {
namespace {
using Queue = AdmissionQueue;
using Status = Queue::Status;
using Clock = Queue::Clock;

// Queue-depth synchronization establishes ordering; elapsed time is only a
// generous hang guard, never the fairness assertion or an admission ranking.
void await_waiting(const Queue& queue, std::size_t count) {
    const auto limit = Clock::now() + std::chrono::seconds(20);
    while (queue.snapshot().waiting != count) {
        if (Clock::now() >= limit) throw std::runtime_error("admission waiter failed to reach barrier");
        std::this_thread::yield();
    }
}
class HeldQueue {
public:
    explicit HeldQueue(std::size_t capacity) : queue(capacity), holder(queue.acquire()) {}
    ~HeldQueue() {
        queue.shutdown();
        holder.lease.release();
        join();
    }
    void start(std::function<void()> worker) { workers.emplace_back(std::move(worker)); }
    void join() { for (auto& worker : workers) if (worker.joinable()) worker.join(); }
    Queue queue;
    Queue::Result holder;
private:
    std::vector<std::thread> workers;
};

TEST(AdmissionQueue, ZeroCapacityPermitsOnlyOneImmediateLease) {
    Queue queue(0);
    auto first = queue.acquire();
    ASSERT_EQ(first.status, Status::admitted);
    EXPECT_TRUE(first.lease);
    EXPECT_EQ(first.ticket, 1U);
    const auto rejected = queue.acquire();
    EXPECT_EQ(rejected.status, Status::overloaded);
    EXPECT_FALSE(rejected.lease);
    EXPECT_EQ(rejected.ticket, 0U);
    EXPECT_EQ(queue.snapshot().waiting, 0U);
    first.lease.release();
    auto next = queue.acquire();
    EXPECT_EQ(next.status, Status::admitted);
    EXPECT_EQ(next.ticket, 2U);
    EXPECT_EQ(queue.snapshot().overloaded, 1U);
}

TEST(AdmissionQueue, WaitingCapacityIsValidatedAndBounded) {
    EXPECT_THROW(Queue(Queue::max_capacity + 1), std::invalid_argument);
    Queue maximum(Queue::max_capacity);
    EXPECT_EQ(maximum.snapshot().capacity, Queue::max_capacity);
    HeldQueue fixture(2);
    for (std::size_t n = 0; n < 2; ++n) {
        fixture.start([&] { auto result = fixture.queue.acquire(); EXPECT_EQ(result.status, Status::admitted); });
        await_waiting(fixture.queue, n + 1);
    }
    auto rejected = fixture.queue.acquire();
    EXPECT_EQ(rejected.status, Status::overloaded);
    EXPECT_FALSE(rejected.lease);
    const auto full = fixture.queue.snapshot();
    EXPECT_TRUE(full.active);
    EXPECT_EQ(full.waiting, 2U);
    EXPECT_EQ(full.peak_waiting, 2U);
    fixture.holder.lease.release();
    fixture.join();
    EXPECT_EQ(fixture.queue.snapshot().admitted, 3U);
}

TEST(AdmissionQueue, ManyWaitersExecuteInExactEnqueueOrder) {
    HeldQueue fixture(32);
    std::vector<unsigned> order;
    std::vector<std::uint64_t> tickets;
    for (unsigned n = 0; n < 32; ++n) {
        fixture.start([&, n] {
            auto result = fixture.queue.acquire();
            EXPECT_EQ(result.status, Status::admitted);
            if (result.lease) { order.push_back(n); tickets.push_back(result.ticket); }
        });
        await_waiting(fixture.queue, n + 1);
    }
    fixture.holder.lease.release();
    fixture.join();
    ASSERT_EQ(order.size(), 32U);
    for (unsigned n = 0; n < 32; ++n) {
        EXPECT_EQ(order[n], n);
        EXPECT_EQ(tickets[n], n + 2U);
    }
    const auto completed = fixture.queue.snapshot();
    EXPECT_EQ(completed.waiting, 0U);
    EXPECT_FALSE(completed.active);
    EXPECT_EQ(completed.peak_waiting, 32U);
    EXPECT_EQ(completed.admitted, 33U);
}

TEST(AdmissionQueue, ReacquiringHolderCannotBypassExistingWaiters) {
    HeldQueue fixture(9);
    std::vector<unsigned> order;
    for (unsigned n = 0; n < 8; ++n) {
        fixture.start([&, n] {
            auto result = fixture.queue.acquire();
            if (result.lease) order.push_back(n);
        });
        await_waiting(fixture.queue, n + 1);
    }
    fixture.holder.lease.release();
    auto reacquired = fixture.queue.acquire();
    EXPECT_EQ(reacquired.status, Status::admitted);
    if (reacquired.lease) order.push_back(8);
    reacquired.lease.release();
    fixture.join();
    ASSERT_EQ(order.size(), 9U);
    for (unsigned n = 0; n < 9; ++n) EXPECT_EQ(order[n], n);
}

TEST(AdmissionQueue, AlreadyCancelledTokenNeverJoinsOrExecutes) {
    Queue queue(4);
    std::stop_source cancellation;
    cancellation.request_stop();
    auto result = queue.acquire(Clock::time_point::max(), cancellation.get_token());
    EXPECT_EQ(result.status, Status::cancelled);
    EXPECT_FALSE(result.lease);
    EXPECT_EQ(result.ticket, 0U);
    EXPECT_EQ(queue.snapshot().admitted, 0U);
    EXPECT_EQ(queue.snapshot().cancelled, 1U);
    EXPECT_EQ(queue.snapshot().peak_waiting, 0U);
}

TEST(AdmissionQueue, CancellingHeadPreservesFollowingWaiterOrder) {
    HeldQueue fixture(3);
    std::stop_source cancellation;
    std::atomic<Status> cancelled{Status::admitted};
    std::vector<unsigned> order;
    fixture.start([&] {
        auto result = fixture.queue.acquire(Clock::time_point::max(), cancellation.get_token());
        cancelled = result.status;
        EXPECT_FALSE(result.lease);
    });
    await_waiting(fixture.queue, 1);
    for (unsigned n = 0; n < 2; ++n) {
        fixture.start([&, n] { auto result = fixture.queue.acquire(); if (result.lease) order.push_back(n); });
        await_waiting(fixture.queue, n + 2);
    }
    cancellation.request_stop();
    await_waiting(fixture.queue, 2);
    fixture.holder.lease.release();
    fixture.join();
    EXPECT_EQ(cancelled.load(), Status::cancelled);
    EXPECT_EQ(order, (std::vector<unsigned>{0, 1}));
    EXPECT_EQ(fixture.queue.snapshot().cancelled, 1U);
}

TEST(AdmissionQueue, CancellingMiddleUnlinksWithoutFifoHoles) {
    HeldQueue fixture(3);
    std::stop_source cancellation;
    std::vector<unsigned> order;
    for (unsigned n = 0; n < 3; ++n) {
        fixture.start([&, n] {
            auto result = fixture.queue.acquire(Clock::time_point::max(),
                n == 1 ? cancellation.get_token() : std::stop_token{});
            if (n == 1) { EXPECT_EQ(result.status, Status::cancelled); }
            if (result.lease) order.push_back(n);
        });
        await_waiting(fixture.queue, n + 1);
    }
    cancellation.request_stop();
    await_waiting(fixture.queue, 2);
    fixture.holder.lease.release();
    fixture.join();
    EXPECT_EQ(order, (std::vector<unsigned>{0, 2}));
}

TEST(AdmissionQueue, AlreadyExpiredDeadlineDoesNotExecuteEvenWhenIdle) {
    Queue queue(1);
    auto result = queue.acquire(Clock::now() - std::chrono::seconds(1));
    EXPECT_EQ(result.status, Status::deadline_expired);
    EXPECT_FALSE(result.lease);
    EXPECT_EQ(result.ticket, 0U);
    EXPECT_EQ(queue.snapshot().deadline_expired, 1U);
    EXPECT_EQ(queue.snapshot().admitted, 0U);
    EXPECT_FALSE(queue.snapshot().active);
}

TEST(AdmissionQueue, DeadlineExpirationRemovesWaiterBeforeExecution) {
    HeldQueue fixture(1);
    std::atomic<Status> status{Status::admitted};
    std::atomic_bool executed{};
    fixture.start([&] {
        auto result = fixture.queue.acquire(Clock::now() + std::chrono::milliseconds(100));
        status = result.status;
        if (result.lease) executed = true;
    });
    // The holder remains active until the waiter has returned; no throughput or
    // fairness claim depends on this elapsed deadline.
    fixture.join();
    EXPECT_EQ(status.load(), Status::deadline_expired);
    EXPECT_FALSE(executed.load());
    EXPECT_EQ(fixture.queue.snapshot().waiting, 0U);
    EXPECT_EQ(fixture.queue.snapshot().deadline_expired, 1U);
    EXPECT_TRUE(fixture.queue.snapshot().active);
}

TEST(AdmissionQueue, ShutdownRejectsQueuedAndNewButPreservesActiveLease) {
    HeldQueue fixture(4);
    for (unsigned n = 0; n < 4; ++n) {
        fixture.start([&] {
            auto result = fixture.queue.acquire();
            EXPECT_EQ(result.status, Status::shutdown);
            EXPECT_FALSE(result.lease);
        });
        await_waiting(fixture.queue, n + 1);
    }
    fixture.queue.shutdown();
    fixture.queue.shutdown();
    fixture.join();
    EXPECT_TRUE(fixture.holder.lease);
    EXPECT_TRUE(fixture.queue.snapshot().active);
    EXPECT_EQ(fixture.queue.snapshot().waiting, 0U);
    EXPECT_FALSE(fixture.queue.wait_idle(Clock::now()));
    auto rejected = fixture.queue.acquire();
    EXPECT_EQ(rejected.status, Status::shutdown);
    EXPECT_EQ(fixture.queue.snapshot().shutdown_rejected, 5U);
    fixture.holder.lease.release();
    EXPECT_TRUE(fixture.queue.wait_idle(Clock::now()));
}

TEST(AdmissionQueue, CancellationAfterGrantDoesNotPreemptRemoteWork) {
    Queue queue(0);
    std::stop_source cancellation;
    auto executing = queue.acquire(Clock::time_point::max(), cancellation.get_token());
    ASSERT_EQ(executing.status, Status::admitted);
    cancellation.request_stop();
    EXPECT_TRUE(executing.lease);
    EXPECT_TRUE(queue.snapshot().active);
    EXPECT_EQ(queue.acquire().status, Status::overloaded);
    EXPECT_EQ(queue.snapshot().cancelled, 0U);
    executing.lease.release();
    EXPECT_TRUE(queue.wait_idle(Clock::now()));
}

TEST(AdmissionQueue, LeaseMovesAndRepeatedReleaseAreExact) {
    Queue first(1), second(1);
    auto a = first.acquire();
    auto b = second.acquire();
    Queue::Lease moved(std::move(a.lease));
    EXPECT_FALSE(a.lease);
    EXPECT_TRUE(moved);
    b.lease = std::move(moved);
    EXPECT_FALSE(moved);
    EXPECT_FALSE(second.snapshot().active);
    EXPECT_TRUE(first.snapshot().active);
    b.lease.release();
    b.lease.release();
    EXPECT_FALSE(first.snapshot().active);
    Queue::Lease empty;
    empty.release();
}

TEST(AdmissionQueue, ExceptionsReleaseActiveLeaseAndNextCallerProceeds) {
    Queue queue(1);
    try {
        auto result = queue.acquire();
        EXPECT_EQ(result.status, Status::admitted);
        throw std::runtime_error("injected worker failure");
    } catch (const std::runtime_error&) {}
    EXPECT_FALSE(queue.snapshot().active);
    auto next = queue.acquire();
    EXPECT_EQ(next.status, Status::admitted);
    EXPECT_EQ(queue.snapshot().admitted, 2U);
}

TEST(AdmissionQueue, ActiveLeaseCanOutliveQueueOwnerSafely) {
    Queue::Lease surviving;
    {
        Queue queue(1);
        auto result = queue.acquire();
        surviving = std::move(result.lease);
    }
    EXPECT_TRUE(surviving);
    EXPECT_NO_THROW(surviving.release());
    EXPECT_FALSE(surviving);
}

TEST(AdmissionQueue, WaitIdleWakesAfterActiveLeaseRelease) {
    Queue queue(1);
    auto executing = queue.acquire();
    std::barrier start(2);
    std::atomic_bool idle{};
    std::thread waiter([&] { start.arrive_and_wait(); idle = queue.wait_idle(); });
    start.arrive_and_wait();
    EXPECT_FALSE(queue.wait_idle(Clock::now()));
    executing.lease.release();
    waiter.join();
    EXPECT_TRUE(idle.load());
}

TEST(AdmissionQueue, RepeatedStressKeepsOneActiveAndBoundedWaiting) {
    for (const unsigned count : {1U, 2U, 4U, 16U, 64U}) {
        Queue queue(count);
        std::barrier start(static_cast<std::ptrdiff_t>(count + 1));
        std::atomic_uint active{}, violations{}, failures{}, total{};
        std::vector<std::uint64_t> tickets;
        std::vector<std::thread> workers;
        for (unsigned n = 0; n < count; ++n) workers.emplace_back([&] {
            start.arrive_and_wait();
            for (unsigned k = 0; k < 8; ++k) {
                auto result = queue.acquire(Clock::now() + std::chrono::seconds(20));
                if (!result.lease) { ++failures; continue; }
                if (++active != 1) ++violations;
                tickets.push_back(result.ticket);
                std::this_thread::yield();
                ++total;
                --active;
            }
        });
        start.arrive_and_wait();
        for (auto& worker : workers) worker.join();
        EXPECT_EQ(violations.load(), 0U);
        EXPECT_EQ(failures.load(), 0U);
        EXPECT_EQ(total.load(), count * 8U);
        const auto snapshot = queue.snapshot();
        EXPECT_EQ(snapshot.waiting, 0U);
        EXPECT_FALSE(snapshot.active);
        EXPECT_LE(snapshot.peak_waiting, count);
        EXPECT_EQ(snapshot.admitted, count * 8U);
        EXPECT_TRUE(std::is_sorted(tickets.begin(), tickets.end()));
        EXPECT_EQ(std::adjacent_find(tickets.begin(), tickets.end()), tickets.end());
    }
}

}  // namespace
}  // namespace atlasfs::scheduling
