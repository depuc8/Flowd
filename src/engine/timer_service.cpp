#include "flowd/engine/timer_service.hpp"

namespace flowd {

TimerService::TimerService() : thread_([this] { run(); }) {}

TimerService::~TimerService() {
    shutdown();
}

TimerService::TimerId TimerService::schedule_at(TimePoint when, Callback cb) {
    TimerId id = 0;
    {
        std::lock_guard lock(mutex_);
        if (stopping_) {
            return 0;
        }
        id = next_id_++;
        pending_.insert(id);
        queue_.push(Entry{when, id, std::move(cb)});
    }
    cv_.notify_one();
    return id;
}

bool TimerService::cancel(TimerId id) {
    std::lock_guard lock(mutex_);
    // Lazy deletion: the queue entry is skipped when it reaches the top.
    return pending_.erase(id) == 1;
}

void TimerService::shutdown() {
    {
        std::lock_guard lock(mutex_);
        if (stopping_ && !thread_.joinable()) {
            return;
        }
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) {
        thread_.join();
    }
    std::lock_guard lock(mutex_);
    while (!queue_.empty()) {
        queue_.pop();
    }
    pending_.clear();
}

void TimerService::run() {
    std::unique_lock lock(mutex_);
    while (!stopping_) {
        if (queue_.empty()) {
            cv_.wait(lock);
            continue;
        }
        const TimePoint when = queue_.top().when;
        if (std::chrono::steady_clock::now() < when) {
            cv_.wait_until(lock, when);
            continue;
        }
        Callback cb = std::move(queue_.top().cb);
        const TimerId id = queue_.top().id;
        queue_.pop();
        if (pending_.erase(id) == 0) {
            continue; // cancelled
        }
        lock.unlock();
        cb();
        lock.lock();
    }
}

} // namespace flowd
