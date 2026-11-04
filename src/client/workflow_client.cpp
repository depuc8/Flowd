#include "flowd/client/workflow_client.hpp"

#include "flowd/core/error.hpp"
#include "flowd/core/workflow_json.hpp"

#include <chrono>
#include <format>
#include <random>
#include <thread>

namespace flowd {

std::string generate_workflow_id(std::string_view name, TimestampMs now) {
    std::string base;
    for (char c : name.substr(0, 64)) {
        base += is_valid_identifier(std::string_view(&c, 1)) ? c : '-';
    }
    if (base.empty()) {
        base = "workflow";
    }
    const auto tp = std::chrono::sys_time<std::chrono::milliseconds>(std::chrono::milliseconds{now});
    std::random_device rd;
    return std::format("{}-{:%Y%m%dT%H%M%S}-{:06x}", base, std::chrono::floor<std::chrono::seconds>(tp),
                       rd() & 0xffffffU);
}

std::string WorkflowClient::submit(const WorkflowSpec& spec, std::optional<std::string> workflow_id) {
    Dag dag(spec); // validates
    const TimestampMs now = clock_.now_ms();
    std::string id = workflow_id ? std::move(*workflow_id) : generate_workflow_id(spec.name, now);
    if (!is_valid_identifier(id)) {
        throw ValidationError("invalid workflow id '" + id + "'");
    }
    WorkflowRecord wf{.workflow_id = id,
                      .name = spec.name,
                      .task_count = dag.size(),
                      .created_at = now,
                      .definition = workflow_to_json(spec)};
    store_.create_workflow(wf, make_task_records(dag, id, store_.shard_count(), now));
    return id;
}

WorkflowSummary WorkflowClient::status(std::string_view workflow_id) {
    if (!store_.get_workflow(workflow_id)) {
        throw Error("unknown workflow '" + std::string(workflow_id) + "'");
    }
    return summarize(store_.list_tasks(workflow_id));
}

WorkflowSummary WorkflowClient::wait(std::string_view workflow_id, std::chrono::milliseconds timeout,
                                     std::chrono::milliseconds poll_interval) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        WorkflowSummary s = status(workflow_id);
        if (s.status != WorkflowSummary::Status::Running ||
            std::chrono::steady_clock::now() >= deadline) {
            return s;
        }
        std::this_thread::sleep_for(poll_interval);
    }
}

} // namespace flowd
