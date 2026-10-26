#pragma once

#include "flowd/concurrency/lockfree_queue.hpp"
#include "flowd/concurrency/unique_function.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <thread>
#include <type_traits>
#include <vector>

namespace flowd::concurrency {

// Fixed-size thread pool fed by a LockFreeQueue.
//
// Concurrency guarantees
//  * submit() is lock-free on the fast path (queue push + one atomic
//    increment); it only enters the kernel to wake a sleeping worker.
//  * Jobs are started in FIFO order per submitting thread; with more than one
//    worker, completion order is unspecified.
//  * Every job accepted by submit() runs exactly once, including jobs still
//    queued when shutdown() is called: shutdown drains the queue.
//  * submit() after shutdown() has begun throws std::runtime_error.
//  * Idle workers spin briefly and then block in std::atomic::wait on an
//    event counter. A producer increments the counter after pushing and only
//    issues a notify when a sleeper is registered; the sleeper re-checks the
//    queue after registering, which rules out lost wake-ups (see
//    worker_loop() for the ordering argument).
//  * Exceptions escaping a job are caught and counted; they never terminate
//    the worker thread.
//  * Jobs may submit further jobs. A job must not call shutdown() or destroy
//    the pool.
class ThreadPool {
public:
    using Job = UniqueFunction<void()>;

    struct Stats {
        std::uint64_t executed = 0;
        std::uint64_t failed = 0;
    };

    explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency());
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ~ThreadPool();

    void submit(Job job);

    template <typename F>
    auto submit_with_future(F&& f) -> std::future<std::invoke_result_t<std::decay_t<F>&>> {
        using R = std::invoke_result_t<std::decay_t<F>&>;
        std::packaged_task<R()> task(std::forward<F>(f));
        auto future = task.get_future();
        submit([t = std::move(task)]() mutable { t(); });
        return future;
    }

    // Stops accepting work, runs all queued jobs and joins the workers.
    // Idempotent; called by the destructor.
    void shutdown();

    [[nodiscard]] std::size_t size() const noexcept { return threads_.size(); }
    [[nodiscard]] Stats stats() const noexcept;

private:
    void worker_loop();
    bool run_one();

    LockFreeQueue<Job> queue_;
    std::vector<std::thread> threads_;

    alignas(64) std::atomic<std::uint32_t> events_{0};
    alignas(64) std::atomic<std::uint32_t> sleepers_{0};
    alignas(64) std::atomic<std::uint32_t> active_submitters_{0};
    std::atomic<bool> accepting_{true};
    std::atomic<bool> closed_{false};
    std::atomic<std::uint64_t> executed_{0};
    std::atomic<std::uint64_t> failed_{0};
};

} // namespace flowd::concurrency
