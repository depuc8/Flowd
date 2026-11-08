#pragma once

#include "flowd/store/task_store.hpp"

#include <atomic>
#include <functional>
#include <string>

namespace flowd::testing {

// Decorator that forwards to a real store but lets tests inject faults:
// dropped heartbeats (a partitioned or paused worker), failing commits, or
// arbitrary hooks before an operation.
class FaultyStore final : public TaskStore {
public:
    explicit FaultyStore(TaskStore& inner) : inner_(inner) {}

    // Returning false from the predicate makes renew_lease report rejection.
    std::function<bool(std::string_view worker)> allow_renewal = [](std::string_view) {
        return true;
    };
    // Throwing from the hook simulates a store/network error on commit.
    std::function<void(std::string_view worker)> before_commit = [](std::string_view) {};
    // Runs after a successful claim, before the worker sees the result
    // (widens the window between "claimed in the store" and "tracked locally").
    std::function<void(std::string_view worker)> after_claim = [](std::string_view) {};
    std::atomic<int> renewals_dropped{0};

    std::uint32_t shard_count() const override { return inner_.shard_count(); }
    void create_workflow(const WorkflowRecord& w, const std::vector<TaskRecord>& t) override {
        inner_.create_workflow(w, t);
    }
    std::optional<WorkflowRecord> get_workflow(std::string_view id) override {
        return inner_.get_workflow(id);
    }
    std::vector<TaskRecord> list_tasks(std::string_view wf) override { return inner_.list_tasks(wf); }
    std::optional<TaskRecord> get_task(const TaskKey& k) override { return inner_.get_task(k); }
    std::vector<TaskKey> find_ready(std::uint32_t s, TimestampMs now, std::size_t l) override {
        return inner_.find_ready(s, now, l);
    }
    std::vector<TaskRecord> find_expired_leases(std::uint32_t s, TimestampMs now,
                                                std::size_t l) override {
        return inner_.find_expired_leases(s, now, l);
    }
    std::vector<TaskRecord> find_unpropagated(std::uint32_t s, TimestampMs before,
                                              std::size_t l) override {
        return inner_.find_unpropagated(s, before, l);
    }
    std::optional<TaskRecord> try_claim(const TaskKey& k, std::string_view w, TimestampMs now,
                                        TimestampMs exp) override {
        auto claimed = inner_.try_claim(k, w, now, exp);
        if (claimed) {
            after_claim(w);
        }
        return claimed;
    }
    bool renew_lease(const TaskKey& k, std::string_view w, std::uint32_t a,
                     TimestampMs exp) override {
        if (!allow_renewal(w)) {
            renewals_dropped.fetch_add(1);
            return false;
        }
        return inner_.renew_lease(k, w, a, exp);
    }
    bool complete(const TaskKey& k, std::string_view w, std::uint32_t a, std::string_view out,
                  TimestampMs now) override {
        before_commit(w);
        return inner_.complete(k, w, a, out, now);
    }
    bool fail(const TaskKey& k, std::string_view w, std::uint32_t a,
              const FailureTransition& t) override {
        before_commit(w);
        return inner_.fail(k, w, a, t);
    }
    bool expire_lease(const TaskKey& k, std::uint32_t a, const FailureTransition& t) override {
        return inner_.expire_lease(k, a, t);
    }
    bool resolve_dependency(const TaskKey& c, std::string_view p, TimestampMs now) override {
        return inner_.resolve_dependency(c, p, now);
    }
    std::optional<TaskRecord> fail_upstream(const TaskKey& c, std::string_view e,
                                            TimestampMs now) override {
        return inner_.fail_upstream(c, e, now);
    }
    void mark_propagated(const TaskKey& k) override { inner_.mark_propagated(k); }

private:
    TaskStore& inner_;
};

} // namespace flowd::testing
