#pragma once

#include "flowd/core/retry_policy.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace flowd {

// Declarative description of one task inside a workflow.
struct TaskSpec {
    std::string id;
    // Name of the handler (see HandlerRegistry) that executes the task.
    std::string handler;
    // Opaque handler input, conventionally a JSON document.
    std::string payload;
    std::vector<std::string> depends_on;
    RetryPolicy retry;
    // Execution timeout per attempt; zero means no timeout.
    std::chrono::milliseconds timeout{0};
};

struct WorkflowSpec {
    std::string name;
    std::vector<TaskSpec> tasks;
};

// Task and workflow ids end up in DynamoDB keys and log lines, so they are
// restricted to [A-Za-z0-9._-] and at most 128 characters.
[[nodiscard]] bool is_valid_identifier(std::string_view id) noexcept;

// Immutable, validated dependency graph built from a WorkflowSpec.
//
// Tasks are addressed by dense indices in [0, size()). Construction rejects
// empty or duplicate ids, unknown or duplicate dependencies, self
// dependencies and cycles; a successfully constructed Dag is always acyclic.
class Dag {
public:
    using Index = std::uint32_t;

    explicit Dag(WorkflowSpec spec);

    [[nodiscard]] std::size_t size() const noexcept { return spec_.tasks.size(); }
    [[nodiscard]] const WorkflowSpec& spec() const noexcept { return spec_; }
    [[nodiscard]] const TaskSpec& task(Index i) const { return spec_.tasks.at(i); }

    [[nodiscard]] std::span<const Index> parents(Index i) const { return parents_.at(i); }
    [[nodiscard]] std::span<const Index> children(Index i) const { return children_.at(i); }

    // Tasks without dependencies, in declaration order.
    [[nodiscard]] std::span<const Index> roots() const noexcept { return roots_; }

    // A topological order (Kahn's algorithm, stable w.r.t. declaration order).
    [[nodiscard]] std::span<const Index> topological_order() const noexcept { return topo_; }

    // Throws std::out_of_range for unknown ids.
    [[nodiscard]] Index index_of(std::string_view id) const;
    [[nodiscard]] bool contains(std::string_view id) const;

    // Ids of every task reachable from `i` (excluding `i`).
    [[nodiscard]] std::vector<Index> descendants(Index i) const;

private:
    WorkflowSpec spec_;
    std::unordered_map<std::string, Index> index_;
    std::vector<std::vector<Index>> parents_;
    std::vector<std::vector<Index>> children_;
    std::vector<Index> roots_;
    std::vector<Index> topo_;
};

} // namespace flowd
