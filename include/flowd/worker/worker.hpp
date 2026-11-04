#pragma once

#include "flowd/concurrency/thread_pool.hpp"
#include "flowd/core/clock.hpp"
#include "flowd/engine/handler.hpp"
#include "flowd/store/task_store.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <stop_token>
#include <string>
#include <thread>

namespace flowd {

struct WorkerOptions {
    // Unique per process; defaults to "<hostname>-<pid>-<random>".
    std::string worker_id;
    // Executor threads; tasks run on a ThreadPool of this size.
    std::size_t threads = std::thread::hardware_concurrency();
    // Maximum number of tasks claimed at once (0 = threads). Claimed tasks
    // that wait for a thread still hold a lease, so keep this close to
    // `threads` for long tasks.
    std::size_t max_in_flight = 0;
    // Maximum number of claims attempted per discovery query.
    std::size_t claim_batch = 32;

    std::chrono::milliseconds lease_duration{30'000};
    // Must be well below lease_duration; a lease is renewed when less than
    // lease_duration - heartbeat_interval of it is left.
    std::chrono::milliseconds heartbeat_interval{10'000};
    // Idle polling backs off exponentially from min to max.
    std::chrono::milliseconds min_poll_interval{50};
    std::chrono::milliseconds max_poll_interval{1'000};
    // How often expired leases and stuck propagations are repaired.
    std::chrono::milliseconds maintenance_interval{2'000};
    // A terminal task whose dependents were not updated within this time is
    // assumed to belong to a crashed worker and is propagated by the reaper.
    std::chrono::milliseconds propagation_grace{10'000};

    // Seed for backoff jitter; 0 picks a random seed.
    std::uint64_t seed = 0;
};

// A worker process (or one of several in a process, for tests) that claims
// claimable tasks from the shared TaskStore, executes them and records the
// outcome. Any number of workers may operate on the same store.
//
// Threads
//   poller       discovers claimable tasks and claims them with a conditional
//                write; claimed tasks are pushed to the executor pool.
//   executors    ThreadPool threads running handlers; they commit results
//                with writes fenced by (worker_id, attempt) and then update
//                dependent tasks.
//   heartbeat    renews leases of in-flight tasks and enforces timeouts. A
//                rejected renewal means another worker took over: the local
//                attempt is cancelled and its result is discarded.
//   maintenance  the lease reaper (expired RUNNING tasks -> RETRYING/FAILED)
//                and the propagation repairer (terminal tasks whose
//                dependents a crashed worker never updated).
class Worker {
public:
    struct Metrics {
        std::uint64_t claimed = 0;
        std::uint64_t claim_conflicts = 0;
        std::uint64_t succeeded = 0;
        std::uint64_t retried = 0;
        std::uint64_t failed = 0;
        std::uint64_t lease_renewals = 0;
        std::uint64_t leases_lost = 0;
        std::uint64_t leases_reaped = 0;
        std::uint64_t propagations_repaired = 0;
        std::uint64_t store_errors = 0;
    };

    Worker(TaskStore& store, const HandlerRegistry& handlers, WorkerOptions options,
           Clock& clock = SystemClock::instance());
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;
    ~Worker();

    void start();

    // Graceful shutdown: stop claiming, let in-flight tasks finish (or cancel
    // them after `drain_timeout`), commit their results, then stop.
    void stop(std::chrono::milliseconds drain_timeout = std::chrono::milliseconds{30'000});

    // Simulates a crash for tests: every thread stops immediately and no
    // further write reaches the store. Leases are left to expire.
    void kill();

    [[nodiscard]] const std::string& id() const noexcept { return options_.worker_id; }
    [[nodiscard]] Metrics metrics() const;
    [[nodiscard]] std::size_t in_flight() const;

private:
    struct InFlight {
        TaskRecord record;
        std::stop_source stop;
        std::atomic<bool> lease_lost{false};
        std::atomic<bool> timed_out{false};
        std::chrono::steady_clock::time_point deadline{};
        std::chrono::steady_clock::time_point next_renewal{};
    };

    void poll_loop(std::stop_token stop);
    void heartbeat_loop(std::stop_token stop);
    void maintenance_loop(std::stop_token stop);
    void execute(std::shared_ptr<InFlight> task);
    std::size_t claim_from(std::uint32_t shard, std::size_t capacity);

    void reap_expired_leases();
    void repair_propagation();
    void propagate(const TaskRecord& record);
    void propagate_success(const TaskRecord& record);
    void propagate_failure(const TaskRecord& record);

    FailureTransition failure_transition(const TaskRecord& record, bool retryable,
                                         std::string error);
    TimestampMs lease_deadline() const;
    void count_store_error(std::string_view what, const std::exception& e);
    bool should_write() const noexcept { return !killed_.load(std::memory_order_acquire); }
    void interruptible_wait(std::stop_token& stop, std::chrono::milliseconds d);

    TaskStore& store_;
    const HandlerRegistry& handlers_;
    WorkerOptions options_;
    Clock& clock_;

    std::unique_ptr<concurrency::ThreadPool> pool_;
    std::jthread poller_;
    std::jthread heartbeat_;
    std::jthread maintenance_;
    std::atomic<bool> started_{false};
    std::atomic<bool> killed_{false};
    std::atomic<bool> draining_{false};

    mutable std::mutex mutex_; // guards in_flight_, rng_, wake-ups
    std::condition_variable_any cv_;
    std::map<TaskKey, std::shared_ptr<InFlight>> in_flight_;
    std::mt19937_64 rng_;

    std::atomic<std::uint64_t> claimed_{0};
    std::atomic<std::uint64_t> claim_conflicts_{0};
    std::atomic<std::uint64_t> succeeded_{0};
    std::atomic<std::uint64_t> retried_{0};
    std::atomic<std::uint64_t> failed_{0};
    std::atomic<std::uint64_t> lease_renewals_{0};
    std::atomic<std::uint64_t> leases_lost_{0};
    std::atomic<std::uint64_t> leases_reaped_{0};
    std::atomic<std::uint64_t> propagations_repaired_{0};
    std::atomic<std::uint64_t> store_errors_{0};
};

[[nodiscard]] std::string default_worker_id();

} // namespace flowd
