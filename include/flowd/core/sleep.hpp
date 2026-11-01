#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stop_token>

namespace flowd {

// Sleeps for `duration` or until stop is requested on `stop`, whichever comes
// first. Returns true if the full duration elapsed.
template <typename Rep, typename Period>
bool interruptible_sleep(std::chrono::duration<Rep, Period> duration, const std::stop_token& stop) {
    // The mutex only exists to satisfy the condition variable interface;
    // wake-ups come from the stop token.
    std::mutex m;
    std::condition_variable_any cv;
    std::unique_lock lock(m); // cppcheck-suppress localMutex
    (void)cv.wait_for(lock, stop, duration, [] { return false; });
    return !stop.stop_requested();
}

} // namespace flowd
