#include "flowd/core/workflow.hpp"

#include "flowd/core/error.hpp"

#include <algorithm>
#include <deque>
#include <stdexcept>
#include <unordered_set>

namespace flowd {

bool is_valid_identifier(std::string_view id) noexcept {
    if (id.empty() || id.size() > 128) {
        return false;
    }
    return std::ranges::all_of(id, [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
               c == '_' || c == '-' || c == '.';
    });
}

Dag::Dag(WorkflowSpec spec) : spec_(std::move(spec)) {
    const std::size_t n = spec_.tasks.size();
    if (n == 0) {
        throw ValidationError("workflow must contain at least one task");
    }
    if (n > std::size_t{UINT32_MAX}) {
        throw ValidationError("workflow has too many tasks");
    }

    index_.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const TaskSpec& t = spec_.tasks[i];
        if (!is_valid_identifier(t.id)) {
            throw ValidationError("invalid task id '" + t.id +
                                  "' (expected 1-128 characters of [A-Za-z0-9._-])");
        }
        if (t.handler.empty()) {
            throw ValidationError("task '" + t.id + "' has no handler");
        }
        if (t.timeout.count() < 0) {
            throw ValidationError("task '" + t.id + "' has a negative timeout");
        }
        try {
            t.retry.validate();
        } catch (const ValidationError& e) {
            throw ValidationError("task '" + t.id + "': " + e.what());
        }
        if (!index_.emplace(t.id, static_cast<Index>(i)).second) {
            throw ValidationError("duplicate task id '" + t.id + "'");
        }
    }

    parents_.resize(n);
    children_.resize(n);
    for (std::size_t i = 0; i < n; ++i) {
        const TaskSpec& t = spec_.tasks[i];
        std::unordered_set<std::string_view> seen;
        for (const std::string& dep : t.depends_on) {
            if (dep == t.id) {
                throw ValidationError("task '" + t.id + "' depends on itself");
            }
            auto it = index_.find(dep);
            if (it == index_.end()) {
                throw ValidationError("task '" + t.id + "' depends on unknown task '" + dep + "'");
            }
            if (!seen.insert(dep).second) {
                throw ValidationError("task '" + t.id + "' lists dependency '" + dep + "' twice");
            }
            parents_[i].push_back(it->second);
            children_[it->second].push_back(static_cast<Index>(i));
        }
        if (t.depends_on.empty()) {
            roots_.push_back(static_cast<Index>(i));
        }
    }

    // Kahn's algorithm doubles as cycle detection: any task that never
    // reaches in-degree zero is on (or behind) a cycle.
    std::vector<std::size_t> indegree(n);
    for (std::size_t i = 0; i < n; ++i) {
        indegree[i] = parents_[i].size();
    }
    std::deque<Index> ready(roots_.begin(), roots_.end());
    topo_.reserve(n);
    while (!ready.empty()) {
        const Index u = ready.front();
        ready.pop_front();
        topo_.push_back(u);
        for (Index v : children_[u]) {
            if (--indegree[v] == 0) {
                ready.push_back(v);
            }
        }
    }
    if (topo_.size() != n) {
        std::string members;
        for (std::size_t i = 0; i < n && members.size() < 200; ++i) {
            if (indegree[i] != 0) {
                members += (members.empty() ? "" : ", ") + spec_.tasks[i].id;
            }
        }
        throw ValidationError("workflow contains a dependency cycle involving: " + members);
    }
}

Dag::Index Dag::index_of(std::string_view id) const {
    auto it = index_.find(std::string(id));
    if (it == index_.end()) {
        throw std::out_of_range("unknown task id '" + std::string(id) + "'");
    }
    return it->second;
}

bool Dag::contains(std::string_view id) const {
    return index_.contains(std::string(id));
}

std::vector<Dag::Index> Dag::descendants(Index i) const {
    std::vector<Index> out;
    std::vector<bool> visited(size(), false);
    std::vector<Index> stack(children_.at(i).begin(), children_.at(i).end());
    while (!stack.empty()) {
        const Index u = stack.back();
        stack.pop_back();
        if (visited[u]) {
            continue;
        }
        visited[u] = true;
        out.push_back(u);
        for (Index v : children_[u]) {
            if (!visited[v]) {
                stack.push_back(v);
            }
        }
    }
    std::ranges::sort(out);
    return out;
}

} // namespace flowd
