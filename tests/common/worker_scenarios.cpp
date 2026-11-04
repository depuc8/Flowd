#include "worker_scenarios.hpp"

#include "faulty_store.hpp"

#include "flowd/client/workflow_client.hpp"
#include "flowd/core/error.hpp"
#include "flowd/engine/handler.hpp"
#include "flowd/worker/worker.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace flowd::testing {
namespace {

using namespace std::chrono_literals;
using ::testing::HasSubstr;
using Ms = std::chrono::milliseconds;

// Records every handler invocation so tests can assert how often each task
// actually ran (as opposed to how often it was committed).
class ExecutionLog {
public:
    void record(std::string_view task, std::uint32_t attempt) {
        std::lock_guard lock(mutex_);
        runs_[std::string(task)].push_back(attempt);
    }
    std::vector<std::uint32_t> runs(const std::string& task) const {
        std::lock_guard lock(mutex_);
        auto it = runs_.find(task);
        return it == runs_.end() ? std::vector<std::uint32_t>{} : it->second;
    }
    std::size_t distinct() const {
        std::lock_guard lock(mutex_);
        return runs_.size();
    }
    std::size_t total() const {
        std::lock_guard lock(mutex_);
        std::size_t n = 0;
        for (const auto& [k, v] : runs_) {
            n += v.size();
        }
        return n;
    }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::vector<std::uint32_t>> runs_;
};

std::string unique_id(const std::string& base) {
    static std::atomic<int> n{0};
    return base + "-" + std::to_string(n++) + "-" + std::to_string(std::random_device{}() % 1000000);
}

TaskSpec task(std::string id, std::string handler, std::vector<std::string> deps = {},
              std::string payload = "{}", std::uint32_t max_attempts = 3) {
    TaskSpec t{.id = std::move(id),
               .handler = std::move(handler),
               .payload = std::move(payload),
               .depends_on = std::move(deps)};
    t.retry = RetryPolicy{.max_attempts = max_attempts,
                          .initial_backoff = 20ms,
                          .multiplier = 2.0,
                          .max_backoff = 200ms,
                          .jitter = 0.0};
    return t;
}

class Scenario : public ::testing::TestWithParam<ScenarioStore> {
protected:
    void SetUp() override {
        store_ = GetParam().make();
        if (store_ == nullptr) {
            GTEST_SKIP() << GetParam().name << " is not available in this environment";
        }
        scale_ = GetParam().time_scale;
        faulty_ = std::make_unique<FaultyStore>(*store_);
        register_builtin_handlers(registry_);
        registry_.add("record", [this](const TaskContext& ctx) {
            log_.record(ctx.task_id, ctx.attempt);
            return TaskResult::success("attempt " + std::to_string(ctx.attempt));
        });
        // Blocks on attempts below payload.until (honouring cancellation)
        // and succeeds afterwards.
        registry_.add("block", [this](const TaskContext& ctx) {
            log_.record(ctx.task_id, ctx.attempt);
            const auto payload = nlohmann::json::parse(ctx.payload);
            const bool blocks = payload.value("forever", false) ||
                                ctx.attempt < payload.value("until", std::uint32_t{0});
            if (blocks) {
                while (!ctx.stop.stop_requested()) {
                    std::this_thread::sleep_for(2ms);
                }
                return TaskResult::failure("cancelled");
            }
            return TaskResult::success("attempt " + std::to_string(ctx.attempt));
        });
    }

    void TearDown() override {
        for (auto& w : workers_) {
            w->stop(2s);
        }
        workers_.clear();
    }

    Ms scaled(Ms d) const { return d * scale_; }

    WorkerOptions options(const std::string& name, std::size_t threads = 4) const {
        WorkerOptions o;
        o.worker_id = name;
        o.threads = threads;
        o.lease_duration = scaled(600ms);
        o.heartbeat_interval = scaled(150ms);
        o.min_poll_interval = 5ms;
        o.max_poll_interval = scaled(40ms);
        o.maintenance_interval = scaled(50ms);
        o.propagation_grace = scaled(300ms);
        return o;
    }

    Worker& add_worker(WorkerOptions o, TaskStore* store = nullptr) {
        workers_.push_back(
            std::make_unique<Worker>(store != nullptr ? *store : *store_, registry_, std::move(o)));
        workers_.back()->start();
        return *workers_.back();
    }

    std::string submit(std::vector<TaskSpec> tasks) {
        WorkflowClient client(*store_);
        return client.submit(WorkflowSpec{"scenario", std::move(tasks)}, unique_id("wf"));
    }

    WorkflowSummary wait(const std::string& wf, Ms timeout = 30s) {
        WorkflowClient client(*store_);
        return client.wait(wf, timeout * scale_, 10ms);
    }

    TaskRecord get(const std::string& wf, const std::string& id) {
        auto r = store_->get_task({wf, id});
        if (!r) {
            throw Error("missing task " + id);
        }
        return *r;
    }

    template <typename Pred>
    bool eventually(Pred pred, Ms timeout = 10s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout * scale_;
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) {
                return true;
            }
            std::this_thread::sleep_for(5ms);
        }
        return pred();
    }

    std::unique_ptr<TaskStore> store_;
    std::unique_ptr<FaultyStore> faulty_;
    HandlerRegistry registry_;
    ExecutionLog log_;
    std::vector<std::unique_ptr<Worker>> workers_;
    int scale_ = 1;
};

TEST_P(Scenario, SingleWorkerRunsDiamondInOrder) {
    const std::string wf = submit({task("a", "record"), task("b", "record", {"a"}),
                                   task("c", "record", {"a"}), task("d", "record", {"b", "c"})});
    add_worker(options("w1"));
    WorkflowSummary s = wait(wf);
    ASSERT_EQ(s.status, WorkflowSummary::Status::Succeeded);
    for (const char* id : {"a", "b", "c", "d"}) {
        TaskRecord r = get(wf, id);
        EXPECT_EQ(r.attempt, 1U) << id;
        EXPECT_EQ(r.output, "attempt 1");
        EXPECT_FALSE(r.needs_propagation) << id;
        EXPECT_EQ(log_.runs(id).size(), 1U);
    }
    EXPECT_LE(get(wf, "a").finished_at, get(wf, "b").finished_at);
    EXPECT_LE(get(wf, "c").finished_at, get(wf, "d").finished_at);
}

TEST_P(Scenario, ManyWorkersShareWorkAndExecuteEachTaskOnce) {
    // Layered DAG; without faults every task must run exactly once even
    // though five workers race to claim the same ready tasks.
    const int layers = scale_ == 1 ? 10 : 4;
    const int width = scale_ == 1 ? 50 : 15;
    std::vector<TaskSpec> tasks;
    for (int l = 0; l < layers; ++l) {
        for (int w = 0; w < width; ++w) {
            std::vector<std::string> deps;
            if (l > 0) {
                deps.push_back("t" + std::to_string(l - 1) + "_" + std::to_string(w));
                deps.push_back("t" + std::to_string(l - 1) + "_" + std::to_string((w + 1) % width));
            }
            tasks.push_back(task("t" + std::to_string(l) + "_" + std::to_string(w), "record", deps));
        }
    }
    const std::string wf = submit(tasks);
    for (int i = 0; i < 5; ++i) {
        add_worker(options("w" + std::to_string(i), 3));
    }
    WorkflowSummary s = wait(wf, 60s);
    ASSERT_EQ(s.status, WorkflowSummary::Status::Succeeded);
    EXPECT_EQ(log_.distinct(), static_cast<std::size_t>(layers * width));
    EXPECT_EQ(log_.total(), static_cast<std::size_t>(layers * width)) << "duplicate execution";

    std::uint64_t claimed = 0;
    int active_workers = 0;
    for (auto& w : workers_) {
        claimed += w->metrics().claimed;
        active_workers += w->metrics().claimed > 0 ? 1 : 0;
        EXPECT_EQ(w->metrics().leases_lost, 0U);
    }
    EXPECT_EQ(claimed, static_cast<std::uint64_t>(layers * width));
    EXPECT_GT(active_workers, 1) << "work was not distributed";
}

TEST_P(Scenario, FailedAttemptsAreRetriedWithBackoff) {
    TaskSpec flaky = task("flaky", "fail", {}, R"({"times": 2})", 4);
    flaky.retry.initial_backoff = scaled(100ms);
    flaky.retry.max_backoff = scaled(1000ms);
    const std::string wf = submit({flaky, task("after", "record", {"flaky"})});
    add_worker(options("w1"));
    const auto start = std::chrono::steady_clock::now();
    ASSERT_EQ(wait(wf).status, WorkflowSummary::Status::Succeeded);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    TaskRecord r = get(wf, "flaky");
    EXPECT_EQ(r.attempt, 3U);
    EXPECT_EQ(r.output, "succeeded on attempt 3");
    // Backoff 100ms after the first failure, 200ms after the second.
    EXPECT_GE(elapsed, scaled(300ms));
    EXPECT_EQ(workers_[0]->metrics().retried, 2U);
}

TEST_P(Scenario, PermanentFailureFailsAllDescendantsWithoutRunningThem) {
    const std::string wf =
        submit({task("root", "record"), task("bad", "fail", {"root"}, R"({"times": 100})", 2),
                task("child", "record", {"bad"}), task("grandchild", "record", {"child", "root"}),
                task("independent", "record", {"root"})});
    add_worker(options("w1"));
    WorkflowSummary s = wait(wf);
    ASSERT_EQ(s.status, WorkflowSummary::Status::Failed);
    EXPECT_EQ(s.succeeded, 2U);
    EXPECT_EQ(s.failed, 3U);
    EXPECT_EQ(get(wf, "bad").attempt, 2U);
    for (const char* id : {"child", "grandchild"}) {
        TaskRecord r = get(wf, id);
        EXPECT_EQ(r.state, TaskState::Failed);
        EXPECT_EQ(r.attempt, 0U);
        EXPECT_THAT(r.last_error, HasSubstr("upstream task"));
        EXPECT_TRUE(log_.runs(id).empty());
    }
    EXPECT_EQ(get(wf, "independent").state, TaskState::Succeeded);
}

TEST_P(Scenario, CrashedWorkersLeaseExpiresAndTaskIsRecovered) {
    const std::string wf = submit({task("work", "block", {}, R"({"until":2})"),
                                   task("next", "record", {"work"})});
    Worker& doomed = add_worker(options("doomed", 1));
    ASSERT_TRUE(eventually([&] { return get(wf, "work").state == TaskState::Running; }));
    EXPECT_EQ(get(wf, "work").lease_owner, "doomed");
    doomed.kill();

    Worker& rescuer = add_worker(options("rescuer", 2));
    ASSERT_EQ(wait(wf).status, WorkflowSummary::Status::Succeeded);
    TaskRecord r = get(wf, "work");
    EXPECT_EQ(r.attempt, 2U);
    EXPECT_EQ(r.output, "attempt 2");
    EXPECT_GE(rescuer.metrics().leases_reaped, 1U);
    EXPECT_EQ(log_.runs("work"), (std::vector<std::uint32_t>{1, 2}));
    EXPECT_EQ(get(wf, "next").state, TaskState::Succeeded);
}

TEST_P(Scenario, ZombieWorkerCannotCommitAfterLosingItsLease) {
    // The "zombie" keeps running its attempt after its heartbeats stop
    // reaching the store (a long GC pause or a network partition). Another
    // worker takes over; the zombie's late result must be rejected.
    registry_.add("slow-first", [this](const TaskContext& ctx) {
        log_.record(ctx.task_id, ctx.attempt);
        if (ctx.attempt == 1) {
            std::this_thread::sleep_for(scaled(1500ms)); // ignores cancellation
            return TaskResult::success("zombie");
        }
        return TaskResult::success("healthy");
    });
    faulty_->allow_renewal = [](std::string_view worker) { return worker != "zombie"; };
    const std::string wf = submit({task("t", "slow-first", {}, "{}", 5)});

    WorkerOptions zopts = options("zombie", 1);
    zopts.max_in_flight = 1;
    Worker& zombie = add_worker(zopts, faulty_.get());
    ASSERT_TRUE(eventually([&] { return get(wf, "t").state == TaskState::Running; }));
    add_worker(options("healthy", 2), faulty_.get());

    ASSERT_EQ(wait(wf).status, WorkflowSummary::Status::Succeeded);
    // Wait for the zombie's attempt to finish and be rejected.
    ASSERT_TRUE(eventually([&] { return zombie.metrics().leases_lost >= 1; }));
    TaskRecord r = get(wf, "t");
    EXPECT_EQ(r.output, "healthy");
    EXPECT_GE(r.attempt, 2U);
    EXPECT_EQ(log_.runs("t").front(), 1U);
    EXPECT_GE(log_.runs("t").size(), 2U) << "at-least-once: both attempts executed";
    EXPECT_GT(faulty_->renewals_dropped.load(), 0);
}

TEST_P(Scenario, StoreErrorDuringCommitIsRecoveredByLeaseExpiry) {
    std::atomic<int> failures_left{1};
    faulty_->before_commit = [&](std::string_view) {
        if (failures_left.fetch_sub(1) > 0) {
            throw StoreError("injected network error");
        }
    };
    const std::string wf = submit({task("t", "record")});
    Worker& w = add_worker(options("w1"), faulty_.get());
    ASSERT_EQ(wait(wf).status, WorkflowSummary::Status::Succeeded);
    EXPECT_EQ(get(wf, "t").attempt, 2U);
    EXPECT_GE(w.metrics().store_errors, 1U);
    EXPECT_GE(w.metrics().leases_reaped, 1U);
}

TEST_P(Scenario, PropagationInterruptedByCrashIsRepaired) {
    const std::string wf = submit({task("a", "record"), task("b", "record", {"a"})});
    // A worker that committed "a" and died before updating its dependents.
    TaskRecord claimed = [&] {
        for (std::uint32_t s = 0; s < store_->shard_count(); ++s) {
            for (const TaskKey& k : store_->find_ready(s, SystemClock::instance().now_ms(), 10)) {
                if (k.workflow_id == wf) {
                    if (auto r = store_->try_claim(k, "ghost", SystemClock::instance().now_ms(),
                                                   SystemClock::instance().now_ms() + 60'000)) {
                        return *r;
                    }
                }
            }
        }
        throw Error("could not claim root");
    }();
    ASSERT_EQ(claimed.task_id, "a");
    ASSERT_TRUE(store_->complete(claimed.key(), "ghost", 1, "done", SystemClock::instance().now_ms()));

    Worker& w = add_worker(options("repairer"));
    ASSERT_EQ(wait(wf).status, WorkflowSummary::Status::Succeeded);
    EXPECT_GE(w.metrics().propagations_repaired, 1U);
    EXPECT_FALSE(get(wf, "a").needs_propagation);
    EXPECT_EQ(log_.runs("a").size(), 0U) << "committed work is not redone";
    EXPECT_EQ(log_.runs("b").size(), 1U);
}

TEST_P(Scenario, TaskThatKeepsLosingItsLeaseFailsAfterMaxAttempts) {
    faulty_->allow_renewal = [](std::string_view) { return false; };
    const std::string wf = submit({task("doomed", "block", {}, R"({"forever":true})", 2),
                                   task("child", "record", {"doomed"})});
    WorkerOptions o = options("w1");
    o.lease_duration = scaled(300ms);
    o.heartbeat_interval = scaled(50ms);
    add_worker(o, faulty_.get());
    WorkflowSummary s = wait(wf);
    ASSERT_EQ(s.status, WorkflowSummary::Status::Failed);
    TaskRecord r = get(wf, "doomed");
    EXPECT_EQ(r.attempt, 2U);
    EXPECT_THAT(r.last_error, HasSubstr("lease"));
    EXPECT_EQ(get(wf, "child").state, TaskState::Failed);
    EXPECT_TRUE(log_.runs("child").empty());
}

TEST_P(Scenario, GracefulStopCommitsInFlightTasks) {
    std::vector<TaskSpec> tasks;
    for (int i = 0; i < 6; ++i) {
        tasks.push_back(task("s" + std::to_string(i), "sleep", {}, R"({"ms": 300})"));
    }
    const std::string wf = submit(tasks);
    Worker& w = add_worker(options("w1", 6));
    ASSERT_TRUE(eventually([&] { return w.in_flight() == 6; }));
    w.stop(10s);
    WorkflowSummary s = WorkflowClient(*store_).status(wf);
    EXPECT_EQ(s.succeeded, 6U);
    EXPECT_EQ(s.running, 0U);
}

TEST_P(Scenario, ThousandsOfTasksAcrossWorkers) {
    const int n = scale_ == 1 ? 5000 : 400;
    std::vector<TaskSpec> tasks;
    tasks.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        tasks.push_back(task("t" + std::to_string(i), "record"));
    }
    const std::string wf = submit(tasks);
    for (int i = 0; i < 4; ++i) {
        WorkerOptions o = options("w" + std::to_string(i), 8);
        o.max_in_flight = 32;
        add_worker(o);
    }
    WorkflowSummary s = wait(wf, 120s);
    ASSERT_EQ(s.status, WorkflowSummary::Status::Succeeded);
    EXPECT_EQ(log_.total(), static_cast<std::size_t>(n));
}

// A binary may legitimately provide no stores (e.g. DynamoDB not configured).
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(Scenario);

INSTANTIATE_TEST_SUITE_P(Stores, Scenario, ::testing::ValuesIn(scenario_stores()), scenario_name);

} // namespace
} // namespace flowd::testing
