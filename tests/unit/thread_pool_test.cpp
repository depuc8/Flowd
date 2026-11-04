#include "flowd/concurrency/thread_pool.hpp"
#include "flowd/concurrency/unique_function.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

namespace flowd::concurrency {
namespace {

using namespace std::chrono_literals;

TEST(UniqueFunction, StoresSmallAndLargeCallables) {
    int calls = 0;
    UniqueFunction<int(int)> small = [&calls](int x) {
        ++calls;
        return x + 1;
    };
    EXPECT_EQ(small(1), 2);

    std::array<char, 256> big{};
    big[0] = 5;
    UniqueFunction<int(int)> large = [big](int x) { return x + big[0]; };
    EXPECT_EQ(large(1), 6);

    UniqueFunction<int(int)> moved = std::move(large);
    EXPECT_FALSE(static_cast<bool>(large)); // NOLINT(bugprone-use-after-move)
    EXPECT_EQ(moved(2), 7);

    auto owned = std::make_unique<int>(3);
    UniqueFunction<int()> move_only = [p = std::move(owned)] { return *p; };
    UniqueFunction<int()> target;
    target = std::move(move_only);
    EXPECT_EQ(target(), 3);
    EXPECT_EQ(calls, 1);
}

TEST(ThreadPool, RunsEveryJob) {
    std::atomic<int> count{0};
    {
        ThreadPool pool(4);
        for (int i = 0; i < 10'000; ++i) {
            pool.submit([&count] { count.fetch_add(1, std::memory_order_relaxed); });
        }
    } // destructor drains
    EXPECT_EQ(count.load(), 10'000);
}

TEST(ThreadPool, ReturnsResultsThroughFutures) {
    ThreadPool pool(2);
    std::vector<std::future<int>> futures;
    for (int i = 0; i < 100; ++i) {
        futures.push_back(pool.submit_with_future([i] { return i * i; }));
    }
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(futures[static_cast<std::size_t>(i)].get(), i * i);
    }
}

TEST(ThreadPool, ShutdownDrainsQueuedJobsAndRejectsNewOnes) {
    ThreadPool pool(1);
    std::promise<void> gate;
    std::shared_future<void> open = gate.get_future().share();
    std::atomic<int> ran{0};
    pool.submit([open] { open.wait(); });
    for (int i = 0; i < 50; ++i) {
        pool.submit([&ran] { ran.fetch_add(1); });
    }
    std::thread closer([&pool] { pool.shutdown(); });
    std::this_thread::sleep_for(20ms);
    gate.set_value();
    closer.join();
    EXPECT_EQ(ran.load(), 50);
    EXPECT_THROW(pool.submit([] {}), std::runtime_error);
    pool.shutdown(); // idempotent
}

TEST(ThreadPool, CountsJobsThatThrow) {
    ThreadPool pool(2);
    for (int i = 0; i < 10; ++i) {
        pool.submit([] { throw std::runtime_error("boom"); });
    }
    pool.submit([] {});
    pool.shutdown();
    EXPECT_EQ(pool.stats().executed, 11U);
    EXPECT_EQ(pool.stats().failed, 10U);
}

TEST(ThreadPool, JobsCanSubmitJobs) {
    // Binary fan-out: 2^14 - 1 jobs, each spawning two children.
    ThreadPool pool(4);
    std::atomic<int> count{0};
    std::promise<void> done;
    constexpr int kTotal = (1 << 14) - 1;
    std::function<void(int)> spawn = [&](int depth) {
        if (count.fetch_add(1) + 1 == kTotal) {
            done.set_value();
        }
        if (depth > 1) {
            pool.submit([&spawn, depth] { spawn(depth - 1); });
            pool.submit([&spawn, depth] { spawn(depth - 1); });
        }
    };
    pool.submit([&spawn] { spawn(14); });
    ASSERT_EQ(done.get_future().wait_for(30s), std::future_status::ready);
    EXPECT_EQ(count.load(), kTotal);
}

TEST(ThreadPool, ManyConcurrentProducers) {
    ThreadPool pool(4);
    std::atomic<int> count{0};
    std::vector<std::thread> producers;
    for (int p = 0; p < 8; ++p) {
        producers.emplace_back([&] {
            for (int i = 0; i < 5'000; ++i) {
                pool.submit([&count] { count.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    pool.shutdown();
    EXPECT_EQ(count.load(), 40'000);
}

// Workers that went to sleep must always be woken by a later submit.
TEST(ThreadPool, NoLostWakeupsAfterIdlePeriods) {
    ThreadPool pool(3);
    for (int i = 0; i < 200; ++i) {
        if (i % 20 == 0) {
            std::this_thread::sleep_for(5ms); // let every worker reach wait()
        }
        auto f = pool.submit_with_future([i] { return i; });
        ASSERT_EQ(f.wait_for(10s), std::future_status::ready) << "iteration " << i;
        EXPECT_EQ(f.get(), i);
    }
}

} // namespace
} // namespace flowd::concurrency
