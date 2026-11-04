#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace flowd {

// Lifecycle of a single task.
//
//            claim                 success
//   PENDING ───────► RUNNING ─────────────────► SUCCEEDED
//      │               │  ▲
//      │      failure, │  │ claim (after backoff)
//      │  attempts left│  │
//      │  or lease     ▼  │
//      │  expired    RETRYING
//      │
//      │ upstream          failure / lease expired, attempts exhausted
//      └──failed──► FAILED ◄───────────────── RUNNING
//
// SUCCEEDED and FAILED are terminal. PENDING -> FAILED is only taken when a
// (transitive) dependency failed, because such a task can never become
// runnable.
enum class TaskState : std::uint8_t {
    Pending,
    Running,
    Retrying,
    Succeeded,
    Failed,
};

[[nodiscard]] constexpr std::string_view to_string(TaskState state) noexcept {
    switch (state) {
    case TaskState::Pending:
        return "PENDING";
    case TaskState::Running:
        return "RUNNING";
    case TaskState::Retrying:
        return "RETRYING";
    case TaskState::Succeeded:
        return "SUCCEEDED";
    case TaskState::Failed:
        return "FAILED";
    }
    return "UNKNOWN";
}

[[nodiscard]] constexpr std::optional<TaskState> parse_task_state(std::string_view text) noexcept {
    if (text == "PENDING") {
        return TaskState::Pending;
    }
    if (text == "RUNNING") {
        return TaskState::Running;
    }
    if (text == "RETRYING") {
        return TaskState::Retrying;
    }
    if (text == "SUCCEEDED") {
        return TaskState::Succeeded;
    }
    if (text == "FAILED") {
        return TaskState::Failed;
    }
    return std::nullopt;
}

[[nodiscard]] constexpr bool is_terminal(TaskState state) noexcept {
    return state == TaskState::Succeeded || state == TaskState::Failed;
}

// The only legal edges of the state machine. Every store implementation and
// the in-process executor validate transitions against this table.
[[nodiscard]] constexpr bool is_valid_transition(TaskState from, TaskState to) noexcept {
    switch (from) {
    case TaskState::Pending:
        return to == TaskState::Running || to == TaskState::Failed;
    case TaskState::Running:
        return to == TaskState::Succeeded || to == TaskState::Retrying || to == TaskState::Failed;
    case TaskState::Retrying:
        return to == TaskState::Running;
    case TaskState::Succeeded:
    case TaskState::Failed:
        return false;
    }
    return false;
}

} // namespace flowd
