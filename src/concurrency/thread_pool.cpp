#include "flowd/concurrency/thread_pool.hpp"

#include <stdexcept>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define FLOWD_CPU_RELAX() _mm_pause()
#elif defined(__aarch64__)
#define FLOWD_CPU_RELAX() asm volatile("yield" ::: "memory")
#else
#define FLOWD_CPU_RELAX() std::this_thread::yield()
#endif

namespace flowd::concurrency {
namespace {
constexpr int kSpinIterations = 64;
}

ThreadPool::ThreadPool(std::size_t threads) {
    if (threads == 0) {
        threads = 1;
    }
    threads_.reserve(threads);
    try {
        for (std::size_t i = 0; i < threads; ++i) {
            threads_.emplace_back([this] { worker_loop(); });
        }
    } catch (...) {
        shutdown();
        throw;
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::submit(Job job) {
    // The submitter count lets shutdown() wait until every in-progress
    // submit() has finished pushing before it declares the queue closed.
    active_submitters_.fetch_add(1, std::memory_order_seq_cst);
    if (!accepting_.load(std::memory_order_seq_cst)) {
        active_submitters_.fetch_sub(1, std::memory_order_seq_cst);
        throw std::runtime_error("ThreadPool::submit called after shutdown");
    }
    queue_.push(std::move(job));
    active_submitters_.fetch_sub(1, std::memory_order_seq_cst);

    events_.fetch_add(1, std::memory_order_seq_cst);
    if (sleepers_.load(std::memory_order_seq_cst) != 0) {
        events_.notify_one();
    }
}

bool ThreadPool::run_one() {
    std::optional<Job> job = queue_.try_pop();
    if (!job) {
        return false;
    }
    try {
        (*job)();
    } catch (...) {
        failed_.fetch_add(1, std::memory_order_relaxed);
    }
    executed_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void ThreadPool::worker_loop() {
    for (;;) {
        if (run_one()) {
            continue;
        }
        bool ran = false;
        for (int i = 0; i < kSpinIterations && !ran; ++i) {
            FLOWD_CPU_RELAX();
            ran = run_one();
        }
        if (ran) {
            continue;
        }

        // Sleep protocol. Let E be the event counter value read here.
        //  - If a producer's push happened before its increment that we read
        //    as E (or earlier), the re-check below sees the job.
        //  - Otherwise the producer increments after our read, so the counter
        //    differs from E and wait(E) returns immediately or is woken: the
        //    producer's sleeper check is ordered after our registration
        //    (all seq_cst) and therefore observes sleepers_ != 0 and notifies.
        const std::uint32_t observed = events_.load(std::memory_order_seq_cst);
        sleepers_.fetch_add(1, std::memory_order_seq_cst);
        if (run_one()) {
            sleepers_.fetch_sub(1, std::memory_order_seq_cst);
            continue;
        }
        if (closed_.load(std::memory_order_seq_cst)) {
            sleepers_.fetch_sub(1, std::memory_order_seq_cst);
            // closed_ is set only after every submit() finished pushing, so
            // one last pop attempt is enough to drain the queue.
            while (run_one()) {
            }
            return;
        }
        events_.wait(observed, std::memory_order_seq_cst);
        sleepers_.fetch_sub(1, std::memory_order_seq_cst);
    }
}

void ThreadPool::shutdown() {
    if (accepting_.exchange(false, std::memory_order_seq_cst)) {
        while (active_submitters_.load(std::memory_order_seq_cst) != 0) {
            std::this_thread::yield();
        }
        closed_.store(true, std::memory_order_seq_cst);
        events_.fetch_add(1, std::memory_order_seq_cst);
        events_.notify_all();
    }
    for (std::thread& t : threads_) {
        if (t.joinable() && t.get_id() != std::this_thread::get_id()) {
            t.join();
        }
    }
}

ThreadPool::Stats ThreadPool::stats() const noexcept {
    return Stats{executed_.load(std::memory_order_relaxed), failed_.load(std::memory_order_relaxed)};
}

} // namespace flowd::concurrency
