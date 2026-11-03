#pragma once

#include "flowd/store/task_store.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace Aws::DynamoDB {
class DynamoDBClient;
}

namespace flowd {

struct DynamoDbOptions {
    std::string table_name = "flowd";
    // Empty: use the SDK default region resolution (AWS_REGION, profile, IMDS).
    std::string region;
    // e.g. "http://localhost:8000" for DynamoDB Local. Empty for AWS.
    std::string endpoint;
    // Only for DynamoDB Local: sign requests with static dummy credentials
    // instead of resolving real ones. Never used against AWS.
    bool use_dummy_credentials = false;
    std::uint32_t shard_count = 8;
    std::uint32_t max_retries = 10;
    std::chrono::milliseconds request_timeout{5'000};
    std::chrono::milliseconds connect_timeout{2'000};
    std::size_t max_connections = 64;
};

// Process-wide AWS SDK initialisation. Create exactly one before using any
// DynamoDbTaskStore and destroy it after the last store is gone.
class AwsSdkSession {
public:
    AwsSdkSession();
    AwsSdkSession(const AwsSdkSession&) = delete;
    AwsSdkSession& operator=(const AwsSdkSession&) = delete;
    ~AwsSdkSession();
};

// TaskStore on a single DynamoDB table (schema: docs/ARCHITECTURE.md).
//
//   pk = "WF#<workflow>"   sk = "META" | "TASK#<task>"
//   GSI "sched": sched_pk = "READY#<shard>" | "LEASE#<shard>" | "PROP#<shard>",
//                sched_at = available_at | lease_expires_at | finished_at
//
// Every state transition is a single UpdateItem with a ConditionExpression
// encoding its precondition, so concurrent workers are serialized by
// DynamoDB itself. Discovery queries the sparse "sched" index, which is
// eventually consistent; a stale candidate simply fails its conditional
// claim.
class DynamoDbTaskStore final : public TaskStore {
public:
    explicit DynamoDbTaskStore(DynamoDbOptions options);
    ~DynamoDbTaskStore() override;

    // Creates the table and its index if missing and waits until ACTIVE.
    // Returns true if the table was created by this call.
    bool create_table(std::chrono::seconds timeout = std::chrono::seconds{120});
    // Deletes the table (used by tests against DynamoDB Local).
    void delete_table();
    [[nodiscard]] bool table_exists();

    [[nodiscard]] std::uint32_t shard_count() const override { return options_.shard_count; }

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

    // Largest output / error text persisted per task; longer text is cut.
    static constexpr std::size_t kMaxTextBytes = 32 * 1024;

private:
    class Impl;
    DynamoDbOptions options_;
    std::unique_ptr<Impl> impl_;
};

} // namespace flowd
