#include "flowd/engine/dag_executor.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

namespace flowd {
namespace {

using namespace std::chrono_literals;
using ::testing::HasSubstr;

TaskSpec task(std::string id, std::string handler, std::vector<std::string> deps = {},
              std::string payload = {}) {
    TaskSpec t{.id = std::move(id),
               .handler = std::move(handler),
               .payload = std::move(payload),
               .depends_on = std::move(deps)};
    t.retry = RetryPolicy{.max_attempts = 3,
                          .initial_backoff = 1ms,
                          .multiplier = 2.0,
                          .max_backoff = 10ms,
                          .jitter = 0.5};
    return t;
}

class DagExecutorTest : public ::testing::Test {
protected:
    DagExecutorTest() : pool_(8), executor_(pool_, timers_, registry_) {
        register_builtin_handlers(registry_);
        // Records the order in which tasks start so tests can check that
        // dependencies completed first.
        registry_.add("record", [this](const TaskContext& ctx) {
            std::lock_guard lock(mutex_);
            started_.emplace_back(ctx.task_id);
            return TaskResult::success(std::string(ctx.task_id));
        });
    }

    std::size_t position(const std::string& id) {
        std::lock_guard lock(mutex_);
        return static_cast<std::size_t>(std::ranges::find(started_, id) - started_.begin());
    }

    HandlerRegistry registry_;
    concurrency::ThreadPool pool_;
    TimerService timers_;
    DagExecutor executor_;
    std::mutex mutex_;
    std::vector<std::string> started_;
};

TEST_F(DagExecutorTest, ExecutesDiamondInDependencyOrder) {
    Dag dag({"diamond",
             {task("a", "record"), task("b", "record", {"a"}), task("c", "record", {"a"}),
              task("d", "record", {"b", "c"})}});
    WorkflowResult r = executor_.run(dag);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.succeeded, 4U);
    EXPECT_LT(position("a"), position("b"));
    EXPECT_LT(position("a"), position("c"));
    EXPECT_LT(position("b"), position("d"));
    EXPECT_LT(position("c"), position("d"));
    EXPECT_EQ(r.tasks[dag.index_of("d")].output, "d");
    for (const auto& t : r.tasks) {
        EXPECT_EQ(t.attempts, 1U);
    }
}

TEST_F(DagExecutorTest, RetriesWithBackoffUntilSuccess) {
    Dag dag({"retry", {task("flaky", "fail", {}, R"({"times": 2})"), task("after", "noop", {"flaky"})}});
    WorkflowResult r = executor_.run(dag);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(r.tasks[0].state, TaskState::Succeeded);
    EXPECT_EQ(r.tasks[0].attempts, 3U);
    EXPECT_EQ(r.tasks[1].attempts, 1U);
}

TEST_F(DagExecutorTest, ExhaustedRetriesFailTaskAndAllDescendants) {
    // a -> b -> d, a -> c, x independent; b always fails.
    Dag dag({"fail",
             {task("a", "noop"), task("b", "fail", {"a"}), task("c", "noop", {"a"}),
              task("d", "noop", {"b", "c"}), task("e", "noop", {"d"}), task("x", "noop")}});
    WorkflowResult r = executor_.run(dag);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.succeeded, 3U); // a, c, x
    EXPECT_EQ(r.failed, 3U);    // b, d, e
    const auto& b = r.tasks[dag.index_of("b")];
    EXPECT_EQ(b.state, TaskState::Failed);
    EXPECT_EQ(b.attempts, 3U);
    for (const char* id : {"d", "e"}) {
        const auto& t = r.tasks[dag.index_of(id)];
        EXPECT_EQ(t.state, TaskState::Failed);
        EXPECT_EQ(t.attempts, 0U) << id << " must never start";
        EXPECT_THAT(t.error, HasSubstr("upstream task 'b' failed"));
    }
    EXPECT_EQ(r.tasks[dag.index_of("x")].state, TaskState::Succeeded);
}

TEST_F(DagExecutorTest, NonRetryableFailureSkipsRemainingAttempts) {
    Dag dag({"fatal", {task("t", "fail", {}, R"({"fatal": true})")}});
    WorkflowResult r = executor_.run(dag);
    EXPECT_EQ(r.tasks[0].state, TaskState::Failed);
    EXPECT_EQ(r.tasks[0].attempts, 1U);
}

TEST_F(DagExecutorTest, TimeoutCountsAsFailedAttempt) {
    TaskSpec slow = task("slow", "sleep", {}, R"({"ms": 10000})");
    slow.timeout = 50ms;
    slow.retry.max_attempts = 2;
    Dag dag({"timeout", {slow}});
    const auto start = std::chrono::steady_clock::now();
    WorkflowResult r = executor_.run(dag);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 5s);
    EXPECT_EQ(r.tasks[0].state, TaskState::Failed);
    EXPECT_EQ(r.tasks[0].attempts, 2U);
    EXPECT_THAT(r.tasks[0].error, HasSubstr("timed out"));
}

TEST_F(DagExecutorTest, ConcurrentFailurePropagationCountsEachTaskOnce) {
    // Many failing parents share the same children; each child must be
    // failed exactly once or the run would never (or prematurely) complete.
    std::vector<TaskSpec> tasks;
    std::vector<std::string> parents;
    for (int i = 0; i < 64; ++i) {
        TaskSpec p = task("p" + std::to_string(i), "fail", {}, R"({"fatal": true})");
        parents.push_back(p.id);
        tasks.push_back(p);
    }
    for (int i = 0; i < 64; ++i) {
        tasks.push_back(task("c" + std::to_string(i), "noop", parents));
    }
    Dag dag({"fan-in-fail", tasks});
    for (int round = 0; round < 20; ++round) {
        WorkflowResult r = executor_.run(dag);
        ASSERT_EQ(r.failed, 128U);
        ASSERT_EQ(r.succeeded, 0U);
    }
}

TEST_F(DagExecutorTest, RunsThousandsOfTasksWithEveryTaskExecutedOnce) {
    // 50 layers of 200 tasks with fan-in 4: 10,000 tasks.
    std::vector<std::atomic<int>> executions(10'000);
    registry_.add("count", [&executions](const TaskContext& ctx) {
        const auto idx = std::stoul(std::string(ctx.task_id.substr(1)));
        executions[idx].fetch_add(1, std::memory_order_relaxed);
        return TaskResult::success();
    });
    std::vector<TaskSpec> tasks;
    for (int l = 0; l < 50; ++l) {
        for (int w = 0; w < 200; ++w) {
            std::vector<std::string> deps;
            if (l > 0) {
                for (int k = 0; k < 4; ++k) {
                    deps.push_back("t" + std::to_string((l - 1) * 200 + (w * 7 + k * 53) % 200));
                }
                std::ranges::sort(deps);
                deps.erase(std::unique(deps.begin(), deps.end()), deps.end());
            }
            tasks.push_back(task("t" + std::to_string(l * 200 + w), "count", deps));
        }
    }
    Dag dag({"big", tasks});
    WorkflowResult r = executor_.run(dag);
    ASSERT_TRUE(r.ok());
    for (const auto& e : executions) {
        ASSERT_EQ(e.load(), 1);
    }
}

TEST_F(DagExecutorTest, ConcurrentWorkflowsOnSharedPool) {
    std::vector<std::shared_ptr<const Dag>> dags;
    std::vector<std::future<WorkflowResult>> futures;
    for (int w = 0; w < 16; ++w) {
        std::vector<TaskSpec> tasks{task("root", "noop")};
        for (int i = 0; i < 100; ++i) {
            tasks.push_back(task("t" + std::to_string(i), i % 10 == 0 ? "fail" : "noop", {"root"},
                                 R"({"times": 1})"));
        }
        auto dag = std::make_shared<const Dag>(WorkflowSpec{"wf" + std::to_string(w), tasks});
        futures.push_back(executor_.start(dag, dag->spec().name));
        dags.push_back(std::move(dag));
    }
    for (auto& f : futures) {
        ASSERT_EQ(f.wait_for(30s), std::future_status::ready);
        EXPECT_TRUE(f.get().ok());
    }
}

} // namespace
} // namespace flowd
