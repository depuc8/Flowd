#pragma once

#include "flowd/core/clock.hpp"
#include "flowd/core/retry_policy.hpp"
#include "flowd/core/task_state.hpp"

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace flowd {

struct TaskKey {
    std::string workflow_id;
    std::string task_id;

    friend auto operator<=>(const TaskKey&, const TaskKey&) = default;
    friend bool operator==(const TaskKey&, const TaskKey&) = default;
};

// Persistent state of one task. This is the unit of coordination between
// workers; every field change goes through a conditional TaskStore operation.
struct TaskRecord {
    std::string workflow_id;
    std::string task_id;
    std::string handler;
    std::string payload;
    RetryPolicy retry;
    std::chrono::milliseconds timeout{0};

    // Immutable graph edges, denormalized so that a completing worker can
    // update the dependents without loading the whole workflow.
    std::vector<std::string> depends_on;
    std::vector<std::string> children;
    // Parents that have not succeeded yet. Shrinks monotonically; the task
    // becomes claimable when it is empty.
    std::vector<std::string> pending_deps;

    TaskState state = TaskState::Pending;
    // Number of attempts started so far. Incremented by every successful
    // claim, so it doubles as the lease's fencing token.
    std::uint32_t attempt = 0;
    std::string lease_owner;
    TimestampMs lease_expires_at = 0;
    // Earliest time the task may be claimed (retry backoff).
    TimestampMs available_at = 0;

    std::string output;
    std::string last_error;
    // Set when the task becomes terminal and cleared once its dependents
    // were updated. Lets any worker finish propagation a crashed worker
    // started (see TaskStore::find_unpropagated).
    bool needs_propagation = false;

    std::uint32_t shard = 0;
    TimestampMs created_at = 0;
    TimestampMs updated_at = 0;
    TimestampMs finished_at = 0;

    [[nodiscard]] TaskKey key() const { return TaskKey{workflow_id, task_id}; }
};

struct WorkflowRecord {
    std::string workflow_id;
    std::string name;
    std::size_t task_count = 0;
    TimestampMs created_at = 0;
    // Original definition as JSON, kept for inspection and resubmission.
    std::string definition;
};

// How a failed attempt (or an expired lease) moves the task on.
struct FailureTransition {
    TaskState next = TaskState::Retrying; // Retrying or Failed
    TimestampMs available_at = 0;         // only used for Retrying
    std::string error;
    TimestampMs now = 0;
};

// Persistence and coordination contract shared by the in-memory store and the
// DynamoDB store. All methods are thread-safe and may be called concurrently
// from many processes (for the DynamoDB implementation).
//
// Discovery methods (find_*) may return stale candidates. Only the
// conditional methods are authoritative: each one checks its precondition and
// applies its update atomically, and returns false / nullopt when the
// precondition does not hold, so racing workers can never both win.
class TaskStore {
public:
    virtual ~TaskStore() = default;

    [[nodiscard]] virtual std::uint32_t shard_count() const = 0;

    // ---- workflows --------------------------------------------------------

    // Persists a workflow and all of its tasks. Root tasks (no dependencies)
    // become claimable once the call returns. Throws AlreadyExists if the id
    // is taken.
    virtual void create_workflow(const WorkflowRecord& workflow,
                                 const std::vector<TaskRecord>& tasks) = 0;
    [[nodiscard]] virtual std::optional<WorkflowRecord> get_workflow(std::string_view id) = 0;
    [[nodiscard]] virtual std::vector<TaskRecord> list_tasks(std::string_view workflow_id) = 0;
    [[nodiscard]] virtual std::optional<TaskRecord> get_task(const TaskKey& key) = 0;

    // ---- discovery --------------------------------------------------------

    // Claimable tasks of `shard` (PENDING with no pending dependencies, or
    // RETRYING) whose available_at <= now, oldest first.
    [[nodiscard]] virtual std::vector<TaskKey> find_ready(std::uint32_t shard, TimestampMs now,
                                                          std::size_t limit) = 0;
    // RUNNING tasks of `shard` whose lease expired before `now`.
    [[nodiscard]] virtual std::vector<TaskRecord> find_expired_leases(std::uint32_t shard,
                                                                      TimestampMs now,
                                                                      std::size_t limit) = 0;
    // Terminal tasks of `shard` that finished before `finished_before` and
    // whose dependents have not been updated yet.
    [[nodiscard]] virtual std::vector<TaskRecord> find_unpropagated(std::uint32_t shard,
                                                                    TimestampMs finished_before,
                                                                    std::size_t limit) = 0;

    // ---- conditional transitions ------------------------------------------

    // PENDING|RETRYING -> RUNNING if the task is claimable at `now`.
    // Increments `attempt`, records the lease and returns the updated record.
    [[nodiscard]] virtual std::optional<TaskRecord> try_claim(const TaskKey& key,
                                                              std::string_view worker_id,
                                                              TimestampMs now,
                                                              TimestampMs lease_expires_at) = 0;

    // Extends the lease if the task is still RUNNING under (worker, attempt).
    [[nodiscard]] virtual bool renew_lease(const TaskKey& key, std::string_view worker_id,
                                           std::uint32_t attempt, TimestampMs lease_expires_at) = 0;

    // RUNNING -> SUCCEEDED if the task is still RUNNING under (worker, attempt).
    [[nodiscard]] virtual bool complete(const TaskKey& key, std::string_view worker_id,
                                        std::uint32_t attempt, std::string_view output,
                                        TimestampMs now) = 0;

    // RUNNING -> RETRYING|FAILED if the task is still RUNNING under
    // (worker, attempt).
    [[nodiscard]] virtual bool fail(const TaskKey& key, std::string_view worker_id,
                                    std::uint32_t attempt, const FailureTransition& transition) = 0;

    // RUNNING -> RETRYING|FAILED for a lease that expired before
    // `transition.now`, if the task is still at `attempt`. Used by the lease
    // reaper to recover tasks of crashed workers.
    [[nodiscard]] virtual bool expire_lease(const TaskKey& key, std::uint32_t attempt,
                                            const FailureTransition& transition) = 0;

    // ---- dependency propagation (idempotent) --------------------------------

    // Removes `parent_id` from the child's pending dependencies. If none
    // remain and the child is PENDING it becomes claimable. Returns true if
    // this call made the child claimable. Safe to repeat.
    virtual bool resolve_dependency(const TaskKey& child, std::string_view parent_id,
                                    TimestampMs now) = 0;

    // PENDING -> FAILED because a dependency failed. Returns the updated
    // record if this call performed the transition.
    [[nodiscard]] virtual std::optional<TaskRecord> fail_upstream(const TaskKey& child,
                                                                  std::string_view error,
                                                                  TimestampMs now) = 0;

    // Clears needs_propagation once all dependents were updated.
    virtual void mark_propagated(const TaskKey& key) = 0;
};

// Builds the initial task records of a workflow. Exposed for stores and tests.
class Dag;
[[nodiscard]] std::vector<TaskRecord> make_task_records(const Dag& dag, std::string_view workflow_id,
                                                        std::uint32_t shard_count, TimestampMs now);

// Deterministic shard assignment (FNV-1a over "workflow/task").
[[nodiscard]] std::uint32_t shard_for(std::string_view workflow_id, std::string_view task_id,
                                      std::uint32_t shard_count) noexcept;

struct WorkflowSummary {
    enum class Status { Running, Succeeded, Failed };
    Status status = Status::Running;
    std::size_t total = 0;
    std::size_t pending = 0;
    std::size_t running = 0;
    std::size_t retrying = 0;
    std::size_t succeeded = 0;
    std::size_t failed = 0;
};

[[nodiscard]] WorkflowSummary summarize(const std::vector<TaskRecord>& tasks);
[[nodiscard]] std::string_view to_string(WorkflowSummary::Status status) noexcept;

} // namespace flowd
