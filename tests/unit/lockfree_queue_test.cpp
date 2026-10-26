#include "flowd/concurrency/lockfree_queue.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <memory>
#include <thread>
#include <vector>

namespace flowd::concurrency {
namespace {

TEST(LockFreeQueue, SingleThreadedFifo) {
    LockFreeQueue<int> q;
    EXPECT_TRUE(q.empty_approx());
    EXPECT_FALSE(q.try_pop().has_value());
    for (int i = 0; i < 100; ++i) {
        q.push(i);
    }
    EXPECT_FALSE(q.empty_approx());
    for (int i = 0; i < 100; ++i) {
        auto v = q.try_pop();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, i);
    }
    EXPECT_FALSE(q.try_pop().has_value());
    EXPECT_TRUE(q.empty_approx());
}

TEST(LockFreeQueue, SupportsMoveOnlyValues) {
    LockFreeQueue<std::unique_ptr<int>> q;
    q.push(std::make_unique<int>(7));
    q.push(std::make_unique<int>(8));
    auto a = q.try_pop();
    auto b = q.try_pop();
    ASSERT_TRUE(a && b);
    EXPECT_EQ(**a, 7);
    EXPECT_EQ(**b, 8);
}

struct Counted {
    explicit Counted(std::atomic<int>* live) : live_(live) { live_->fetch_add(1); }
    Counted(Counted&& o) noexcept : live_(std::exchange(o.live_, nullptr)) {}
    Counted& operator=(Counted&&) = delete;
    ~Counted() {
        if (live_ != nullptr) {
            live_->fetch_sub(1);
        }
    }
    std::atomic<int>* live_;
};

TEST(LockFreeQueue, DestructorDestroysRemainingValues) {
    std::atomic<int> live{0};
    {
        LockFreeQueue<Counted> q;
        for (int i = 0; i < 10; ++i) {
            q.push(Counted(&live));
        }
        (void)q.try_pop();
        EXPECT_EQ(live.load(), 9);
    }
    EXPECT_EQ(live.load(), 0);
}

// Every value must be delivered exactly once, and values from one producer
// must be observed by any single consumer in the order they were pushed.
TEST(LockFreeQueue, MpmcStressDeliversEachValueExactlyOnceInProducerOrder) {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 50'000;
    LockFreeQueue<std::uint64_t> q;
    std::atomic<int> producers_done{0};
    std::vector<std::vector<std::uint64_t>> received(kConsumers);
    std::barrier start(kProducers + kConsumers);

    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back([&, p] {
            start.arrive_and_wait();
            for (int i = 0; i < kPerProducer; ++i) {
                q.push((static_cast<std::uint64_t>(p) << 32) | static_cast<std::uint64_t>(i));
            }
            producers_done.fetch_add(1);
        });
    }
    for (int c = 0; c < kConsumers; ++c) {
        threads.emplace_back([&, c] {
            start.arrive_and_wait();
            for (;;) {
                if (auto v = q.try_pop()) {
                    received[static_cast<std::size_t>(c)].push_back(*v);
                } else if (producers_done.load() == kProducers) {
                    // Producers are finished; drain whatever is left.
                    while (auto rest = q.try_pop()) {
                        received[static_cast<std::size_t>(c)].push_back(*rest);
                    }
                    break;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    std::vector<std::uint64_t> all;
    for (const auto& r : received) {
        std::vector<std::int64_t> last(kProducers, -1);
        for (std::uint64_t v : r) {
            const auto producer = static_cast<std::size_t>(v >> 32);
            const auto seq = static_cast<std::int64_t>(v & 0xffffffffU);
            ASSERT_LT(last[producer], seq) << "per-producer FIFO order violated";
            last[producer] = seq;
        }
        all.insert(all.end(), r.begin(), r.end());
    }
    ASSERT_EQ(all.size(), static_cast<std::size_t>(kProducers * kPerProducer));
    std::ranges::sort(all);
    EXPECT_EQ(std::ranges::adjacent_find(all), all.end()) << "duplicate delivery";
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(LockFreeQueue, ReclaimsNodesUnderContention) {
    LockFreeQueue<int> q;
    constexpr int kThreads = 8;
    constexpr int kOps = 20'000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kOps; ++i) {
                q.push(i);
                (void)q.try_pop();
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    while (q.try_pop()) {
    }
    // Each thread keeps at most a scan threshold's worth of retired nodes;
    // without reclamation this would be kThreads * kOps.
    HazardDomain::global().scan();
    EXPECT_LT(HazardDomain::global().pending_reclamation(),
              static_cast<std::size_t>(kThreads * kOps) / 10);
}

} // namespace
} // namespace flowd::concurrency
