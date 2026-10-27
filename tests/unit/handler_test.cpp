#include "flowd/engine/handler.hpp"
#include "flowd/engine/timer_service.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace flowd {
namespace {

using namespace std::chrono_literals;
using ::testing::HasSubstr;

class BuiltinHandlers : public ::testing::Test {
protected:
    void SetUp() override { register_builtin_handlers(registry_); }

    TaskResult run(std::string_view handler, std::string payload, std::uint32_t attempt = 1,
                   std::chrono::milliseconds timeout = 0ms, std::stop_token stop = {}) {
        payload_ = std::move(payload);
        TaskContext ctx{.workflow_id = "wf",
                        .task_id = "t1",
                        .payload = payload_,
                        .attempt = attempt,
                        .timeout = timeout,
                        .stop = std::move(stop)};
        return registry_.invoke(handler, ctx);
    }

    HandlerRegistry registry_;
    std::string payload_;
};

TEST_F(BuiltinHandlers, NoopAndEcho) {
    EXPECT_TRUE(run("noop", "").ok);
    TaskResult r = run("echo", R"({"message": "hello"})");
    EXPECT_TRUE(r.ok);
    EXPECT_EQ(r.output, "hello");
}

TEST_F(BuiltinHandlers, FailSucceedsAfterConfiguredAttempts) {
    EXPECT_FALSE(run("fail", R"({"times": 2})", 1).ok);
    EXPECT_FALSE(run("fail", R"({"times": 2})", 2).ok);
    EXPECT_TRUE(run("fail", R"({"times": 2})", 3).ok);
    TaskResult fatal = run("fail", R"({"fatal": true})");
    EXPECT_FALSE(fatal.ok);
    EXPECT_FALSE(fatal.retryable);
}

TEST_F(BuiltinHandlers, UnknownHandlerAndExceptionsBecomeFailures) {
    TaskResult missing = run("does-not-exist", "");
    EXPECT_FALSE(missing.ok);
    EXPECT_THAT(missing.error, HasSubstr("no handler"));

    registry_.add("throws", [](const TaskContext&) -> TaskResult { throw std::logic_error("bad"); });
    TaskResult thrown = run("throws", "");
    EXPECT_FALSE(thrown.ok);
    EXPECT_TRUE(thrown.retryable);
    EXPECT_THAT(thrown.error, HasSubstr("bad"));

    TaskResult bad_json = run("sleep", "{not json");
    EXPECT_FALSE(bad_json.ok);
}

TEST_F(BuiltinHandlers, ShellCapturesOutputAndExportsContext) {
    TaskResult r = run("shell", R"({"command": "echo out; echo $FLOWD_IDEMPOTENCY_KEY:$FLOWD_ATTEMPT >&2"})",
                       4);
    ASSERT_TRUE(r.ok) << r.error;
    EXPECT_EQ(r.output, "out\nwf/t1:4\n");
}

TEST_F(BuiltinHandlers, ShellReportsNonZeroExit) {
    TaskResult r = run("shell", R"({"command": "echo broken; exit 3"})");
    EXPECT_FALSE(r.ok);
    EXPECT_TRUE(r.retryable);
    EXPECT_THAT(r.error, HasSubstr("exit status 3"));
    EXPECT_THAT(r.error, HasSubstr("broken"));

    TaskResult no_command = run("shell", R"({"cmd": "true"})");
    EXPECT_FALSE(no_command.ok);
    EXPECT_FALSE(no_command.retryable);
}

TEST_F(BuiltinHandlers, ShellKillsProcessTreeOnTimeout) {
    const auto start = std::chrono::steady_clock::now();
    TaskResult r = run("shell", R"({"command": "sleep 30 & sleep 30; wait"})", 1, 200ms);
    EXPECT_FALSE(r.ok);
    EXPECT_THAT(r.error, HasSubstr("timed out"));
    EXPECT_LT(std::chrono::steady_clock::now() - start, 5s);
}

TEST_F(BuiltinHandlers, SleepAndShellObserveCancellation) {
    std::stop_source source;
    std::thread canceller([&source] {
        std::this_thread::sleep_for(100ms);
        source.request_stop();
    });
    const auto start = std::chrono::steady_clock::now();
    TaskResult r = run("sleep", R"({"ms": 30000})", 1, 0ms, source.get_token());
    canceller.join();
    EXPECT_FALSE(r.ok);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 5s);

    std::stop_source shell_source;
    shell_source.request_stop();
    TaskResult s = run("shell", R"({"command": "sleep 30"})", 1, 0ms, shell_source.get_token());
    EXPECT_FALSE(s.ok);
    EXPECT_THAT(s.error, HasSubstr("cancelled"));
}

TEST(TimerService, FiresInDeadlineOrderAndSupportsCancel) {
    TimerService timers;
    std::mutex m;
    std::vector<int> order;
    std::promise<void> done;
    auto record = [&](int v) {
        std::lock_guard lock(m);
        order.push_back(v);
    };
    timers.schedule_after(60ms, [&] { record(3); });
    timers.schedule_after(20ms, [&] { record(1); });
    auto cancelled = timers.schedule_after(40ms, [&] { record(99); });
    timers.schedule_after(40ms, [&] { record(2); });
    timers.schedule_after(80ms, [&] { done.set_value(); });
    EXPECT_TRUE(timers.cancel(cancelled));
    EXPECT_FALSE(timers.cancel(cancelled));
    ASSERT_EQ(done.get_future().wait_for(5s), std::future_status::ready);
    std::lock_guard lock(m);
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

TEST(TimerService, ShutdownDiscardsPendingCallbacks) {
    std::atomic<bool> fired{false};
    {
        TimerService timers;
        timers.schedule_after(10s, [&] { fired = true; });
        timers.shutdown();
        EXPECT_EQ(timers.schedule_after(0ms, [&] { fired = true; }), 0U);
    }
    EXPECT_FALSE(fired.load());
}

} // namespace
} // namespace flowd
