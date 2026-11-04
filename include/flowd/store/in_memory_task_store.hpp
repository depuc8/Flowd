#pragma once

#include "flowd/store/task_store.hpp"

#include <map>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

namespace flowd {

// TaskStore kept in process memory behind a single mutex.
//
// It implements exactly the same conditional semantics as the DynamoDB store
// (each method is one atomic check-and-update), which makes it the reference
// implementation for the store contract tests and lets many Worker instances
// in one process exercise the distributed protocol deterministically.
// Secondary indexes mirror the DynamoDB sparse GSI: a task is in at most one
// of the ready / lease / propagation indexes at any time.
class InMemoryTaskStore final : public TaskStore {
public:
    explicit InMemoryTaskStore(std::uint32_t shard_count = 8);

    [[nodiscard]] std::uint32_t shard_count() const override { return shard_count_; }

    void create_workflow(const WorkflowRecord& workflow,
                         const std::vector<TaskRecord>& tasks) override;
    [[nodiscard]] std::optional<WorkflowRecord> get_workflow(std::string_view id) override;
    [[nodiscard]] std::vector<TaskRecord> list_tasks(std::string_view workflow_id) override;
    [[nodiscard]] std::optional<TaskRecord> get_task(const TaskKey& key) override;

    [[nodiscard]] std::vector<TaskKey> find_ready(std::uint32_t shard, TimestampMs now,
                                                  std::size_t limit) override;
    [[nodiscard]] std::vector<TaskRecord> find_expired_leases(std::uint32_t shard, TimestampMs now,
                                                              std::size_t limit) override;
    [[nodiscard]] std::vector<TaskRecord> find_unpropagated(std::uint32_t shard,
                                                            TimestampMs finished_before,
                                                            std::size_t limit) override;

    [[nodiscard]] std::optional<TaskRecord> try_claim(const TaskKey& key, std::string_view worker_id,
                                                      TimestampMs now,
                                                      TimestampMs lease_expires_at) override;
    [[nodiscard]] bool renew_lease(const TaskKey& key, std::string_view worker_id,
                                   std::uint32_t attempt, TimestampMs lease_expires_at) override;
    [[nodiscard]] bool complete(const TaskKey& key, std::string_view worker_id,
                                std::uint32_t attempt, std::string_view output,
                                TimestampMs now) override;
    [[nodiscard]] bool fail(const TaskKey& key, std::string_view worker_id, std::uint32_t attempt,
                            const FailureTransition& transition) override;
    [[nodiscard]] bool expire_lease(const TaskKey& key, std::uint32_t attempt,
                                    const FailureTransition& transition) override;

    bool resolve_dependency(const TaskKey& child, std::string_view parent_id,
                            TimestampMs now) override;
    [[nodiscard]] std::optional<TaskRecord> fail_upstream(const TaskKey& child,
                                                          std::string_view error,
                                                          TimestampMs now) override;
    void mark_propagated(const TaskKey& key) override;

    // Number of transitions rejected because their precondition failed.
    [[nodiscard]] std::uint64_t rejected_transitions() const;

private:
    enum class Index { None, Ready, Lease, Propagate };
    using IndexKey = std::pair<TimestampMs, TaskKey>;

    struct Entry {
        TaskRecord record;
        Index index = Index::None;
        TimestampMs index_time = 0;
    };

    Entry* find_locked(const TaskKey& key);
    void set_index_locked(Entry& e, Index index, TimestampMs time);
    std::set<IndexKey>& index_set(Index index, std::uint32_t shard);
    void apply_failure_locked(Entry& e, const FailureTransition& t);
    bool reject_locked();

    const std::uint32_t shard_count_;
    mutable std::mutex mutex_;
    std::map<std::string, WorkflowRecord, std::less<>> workflows_;
    std::map<TaskKey, Entry> tasks_;
    std::vector<std::set<IndexKey>> ready_;
    std::vector<std::set<IndexKey>> leases_;
    std::vector<std::set<IndexKey>> propagate_;
    std::uint64_t rejected_ = 0;
};

} // namespace flowd
