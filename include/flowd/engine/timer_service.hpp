#pragma once

#include "flowd/concurrency/unique_function.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>
#include <vector>

namespace flowd {

// Single-threaded scheduler for delayed callbacks (retry backoff, attempt
// timeouts). Callbacks run on the timer thread and must be short; anything
// heavy should be handed to a ThreadPool. This component is off the hot
// path, so it uses a mutex and a condition variable rather than lock-free
// structures.
class TimerService {
public:
    using Callback = concurrency::UniqueFunction<void()>;
    using TimePoint = std::chrono::steady_clock::time_point;
    using TimerId = std::uint64_t;

    TimerService();
    TimerService(const TimerService&) = delete;
    TimerService& operator=(const TimerService&) = delete;
    ~TimerService();

    TimerId schedule_at(TimePoint when, Callback cb);
    TimerId schedule_after(std::chrono::milliseconds delay, Callback cb) {
        return schedule_at(std::chrono::steady_clock::now() + delay, std::move(cb));
    }

    // Best effort: returns true if the callback had not started yet and will
    // now never run.
    bool cancel(TimerId id);

    // Discards pending callbacks and joins the thread. Idempotent.
    void shutdown();

private:
    struct Entry {
        TimePoint when;
        TimerId id;
        // mutable so the callback can be moved out of the priority queue's top().
        mutable Callback cb;
        bool operator>(const Entry& o) const { return when != o.when ? when > o.when : id > o.id; }
    };

    void run();

    std::mutex mutex_;
    std::condition_variable cv_;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<>> queue_;
    std::unordered_set<TimerId> pending_; // scheduled, not yet fired or cancelled
    TimerId next_id_ = 1;
    bool stopping_ = false;
    std::thread thread_;
};

} // namespace flowd
