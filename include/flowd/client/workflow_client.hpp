#pragma once

#include "flowd/core/clock.hpp"
#include "flowd/core/workflow.hpp"
#include "flowd/store/task_store.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

namespace flowd {

// Submits workflows to a TaskStore and inspects their progress. Execution is
// done by Worker processes polling the same store.
class WorkflowClient {
public:
    explicit WorkflowClient(TaskStore& store, Clock& clock = SystemClock::instance())
        : store_(store), clock_(clock) {}

    // Validates the definition as a DAG and persists it. Returns the workflow
    // id (`workflow_id` if given, otherwise "<name>-<utc timestamp>-<random>").
    // Throws ValidationError or AlreadyExists.
    std::string submit(const WorkflowSpec& spec, std::optional<std::string> workflow_id = {});

    [[nodiscard]] WorkflowSummary status(std::string_view workflow_id);

    // Polls until the workflow is terminal or the timeout expires. Returns
    // the last observed summary.
    WorkflowSummary wait(std::string_view workflow_id, std::chrono::milliseconds timeout,
                         std::chrono::milliseconds poll_interval = std::chrono::milliseconds{200});

private:
    TaskStore& store_;
    Clock& clock_;
};

[[nodiscard]] std::string generate_workflow_id(std::string_view name, TimestampMs now);

} // namespace flowd
