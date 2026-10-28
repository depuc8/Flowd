#include "flowd/store/task_store.hpp"

#include "flowd/core/workflow.hpp"

#include <algorithm>

namespace flowd {

std::uint32_t shard_for(std::string_view workflow_id, std::string_view task_id,
                        std::uint32_t shard_count) noexcept {
    std::uint64_t h = 0xcbf29ce484222325ULL;
    auto mix = [&h](std::string_view s) {
        for (char c : s) {
            h ^= static_cast<unsigned char>(c);
            h *= 0x100000001b3ULL;
        }
    };
    mix(workflow_id);
    mix("/");
    mix(task_id);
    return shard_count == 0 ? 0 : static_cast<std::uint32_t>(h % shard_count);
}

std::vector<TaskRecord> make_task_records(const Dag& dag, std::string_view workflow_id,
                                          std::uint32_t shard_count, TimestampMs now) {
    std::vector<TaskRecord> records;
    records.reserve(dag.size());
    for (Dag::Index i = 0; i < dag.size(); ++i) {
        const TaskSpec& spec = dag.task(i);
        TaskRecord r;
        r.workflow_id = std::string(workflow_id);
        r.task_id = spec.id;
        r.handler = spec.handler;
        r.payload = spec.payload;
        r.retry = spec.retry;
        r.timeout = spec.timeout;
        r.depends_on = spec.depends_on;
        std::ranges::sort(r.depends_on);
        for (Dag::Index c : dag.children(i)) {
            r.children.push_back(dag.task(c).id);
        }
        std::ranges::sort(r.children);
        r.pending_deps = r.depends_on;
        r.state = TaskState::Pending;
        r.available_at = now;
        r.shard = shard_for(workflow_id, spec.id, shard_count);
        r.created_at = now;
        r.updated_at = now;
        records.push_back(std::move(r));
    }
    return records;
}

WorkflowSummary summarize(const std::vector<TaskRecord>& tasks) {
    WorkflowSummary s;
    s.total = tasks.size();
    for (const TaskRecord& t : tasks) {
        switch (t.state) {
        case TaskState::Pending:
            ++s.pending;
            break;
        case TaskState::Running:
            ++s.running;
            break;
        case TaskState::Retrying:
            ++s.retrying;
            break;
        case TaskState::Succeeded:
            ++s.succeeded;
            break;
        case TaskState::Failed:
            ++s.failed;
            break;
        }
    }
    if (s.total > 0 && s.succeeded == s.total) {
        s.status = WorkflowSummary::Status::Succeeded;
    } else if (s.total > 0 && s.succeeded + s.failed == s.total) {
        s.status = WorkflowSummary::Status::Failed;
    } else {
        s.status = WorkflowSummary::Status::Running;
    }
    return s;
}

std::string_view to_string(WorkflowSummary::Status status) noexcept {
    switch (status) {
    case WorkflowSummary::Status::Running:
        return "RUNNING";
    case WorkflowSummary::Status::Succeeded:
        return "SUCCEEDED";
    case WorkflowSummary::Status::Failed:
        return "FAILED";
    }
    return "UNKNOWN";
}

} // namespace flowd
