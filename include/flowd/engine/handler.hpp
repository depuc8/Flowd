#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace flowd {

// Everything a handler learns about the attempt it is executing.
struct TaskContext {
    std::string_view workflow_id;
    std::string_view task_id;
    std::string_view payload;
    // 1-based attempt number. Doubles as the fencing token of the lease in
    // the distributed worker: a later attempt always has a larger number.
    std::uint32_t attempt = 1;
    // Per-attempt timeout (zero = none). Enforced cooperatively via `stop`.
    std::chrono::milliseconds timeout{0};
    // Requested when the attempt timed out, the lease was lost or the worker
    // is shutting down. Long-running handlers must poll it.
    std::stop_token stop;

    // Stable across attempts and workers. External side effects should be
    // keyed on it so that a duplicate execution (at-least-once delivery) is
    // harmless; see docs/ARCHITECTURE.md, "Delivery semantics".
    [[nodiscard]] std::string idempotency_key() const {
        std::string key;
        key.reserve(workflow_id.size() + task_id.size() + 1);
        key.append(workflow_id).append("/").append(task_id);
        return key;
    }
};

struct TaskResult {
    bool ok = true;
    // Small textual output persisted with the task (truncated by stores).
    std::string output;
    std::string error;
    // A non-retryable failure goes straight to FAILED regardless of the
    // remaining attempts (e.g. invalid input).
    bool retryable = true;

    static TaskResult success(std::string output = {}) {
        return TaskResult{.ok = true, .output = std::move(output)};
    }
    static TaskResult failure(std::string error) {
        return TaskResult{.ok = false, .error = std::move(error), .retryable = true};
    }
    static TaskResult fatal(std::string error) {
        return TaskResult{.ok = false, .error = std::move(error), .retryable = false};
    }
};

// Handlers may run concurrently on many threads and must be thread-safe.
// Exceptions thrown by a handler are converted into retryable failures.
using TaskHandler = std::function<TaskResult(const TaskContext&)>;

// Name -> handler map. Populate it before starting executors or workers; it
// is read concurrently afterwards and must not be modified then.
class HandlerRegistry {
public:
    void add(std::string name, TaskHandler handler);
    [[nodiscard]] const TaskHandler* find(std::string_view name) const;
    [[nodiscard]] bool contains(std::string_view name) const { return find(name) != nullptr; }

    // Runs the handler for `handler_name`, converting a missing handler and
    // escaping exceptions into failures.
    [[nodiscard]] TaskResult invoke(std::string_view handler_name, const TaskContext& ctx) const;

private:
    std::map<std::string, TaskHandler, std::less<>> handlers_;
};

// Registers the built-in handlers:
//   noop   - succeeds immediately
//   sleep  - {"ms": N} sleeps N milliseconds (interruptible)
//   fail   - {"times": N} fails the first N attempts, then succeeds;
//            {"fatal": true} fails without retry
//   echo   - succeeds with the payload's "message" (or the payload) as output
//   shell  - {"command": "..."} runs /bin/sh -c command; non-zero exit status
//            is a retryable failure; stdout is captured (first 64 KiB); the
//            process group is killed on timeout or cancellation
void register_builtin_handlers(HandlerRegistry& registry);

} // namespace flowd
