#pragma once

#include "flowd/concurrency/thread_pool.hpp"
#include "flowd/core/task_state.hpp"
#include "flowd/core/workflow.hpp"
#include "flowd/engine/handler.hpp"
#include "flowd/engine/timer_service.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

namespace flowd {

struct TaskOutcome {
    TaskState state = TaskState::Pending;
    std::uint32_t attempts = 0;
    std::string output;
    std::string error;
};

struct WorkflowResult {
    std::vector<TaskOutcome> tasks; // indexed like the Dag
    std::size_t succeeded = 0;
    std::size_t failed = 0;
    std::chrono::nanoseconds elapsed{0};

    [[nodiscard]] bool ok() const noexcept { return failed == 0 && succeeded == tasks.size(); }
};

// In-process DAG executor.
//
// Dependency tracking uses one atomic counter of unfinished parents per task.
// When a task succeeds it decrements the counter of each child; the thread
// whose decrement reaches zero is the unique thread that schedules the child,
// so no task is ever started twice and no lock is needed. Ready tasks go to
// the ThreadPool (lock-free queue). Retries are re-scheduled through the
// TimerService after the task's backoff. A task that fails permanently marks
// all its descendants FAILED (PENDING -> FAILED) with a CAS on their state,
// which makes concurrent failure propagation along different paths safe.
//
// Within a single run every task is executed at most once per attempt and
// attempts of one task never overlap. The pool, timer service and handler
// registry must outlive every run started on them.
class DagExecutor {
public:
    struct Options {
        // Seed for backoff jitter; 0 picks a random seed.
        std::uint64_t seed = 0;
    };

    DagExecutor(concurrency::ThreadPool& pool, TimerService& timers,
                const HandlerRegistry& handlers, Options options);
    DagExecutor(concurrency::ThreadPool& pool, TimerService& timers,
                const HandlerRegistry& handlers)
        : DagExecutor(pool, timers, handlers, Options{}) {}

    // Starts the workflow and returns a future that becomes ready once every
    // task reached a terminal state.
    [[nodiscard]] std::future<WorkflowResult> start(std::shared_ptr<const Dag> dag,
                                                    std::string workflow_id = "local");

    // Blocking convenience wrapper around start().
    WorkflowResult run(const Dag& dag, std::string workflow_id = "local");

private:
    struct Run;

    void execute(const std::shared_ptr<Run>& run, std::uint32_t index);
    void on_success(const std::shared_ptr<Run>& run, std::uint32_t index);
    void fail_descendants(const std::shared_ptr<Run>& run, std::uint32_t index);
    void mark_terminal(const std::shared_ptr<Run>& run, std::size_t count);
    void schedule(const std::shared_ptr<Run>& run, std::uint32_t index);
    double next_random();

    concurrency::ThreadPool& pool_;
    TimerService& timers_;
    const HandlerRegistry& handlers_;
    std::atomic<std::uint64_t> rng_state_;
};

} // namespace flowd
