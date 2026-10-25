#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace flowd {

// Milliseconds since the Unix epoch. Leases and retry deadlines are stored
// as wall-clock timestamps because they are compared across machines; see
// docs/ARCHITECTURE.md for the clock-skew assumptions this implies.
using TimestampMs = std::int64_t;

class Clock {
public:
    virtual ~Clock() = default;
    [[nodiscard]] virtual TimestampMs now_ms() const = 0;
};

class SystemClock final : public Clock {
public:
    [[nodiscard]] TimestampMs now_ms() const override {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    static SystemClock& instance() {
        static SystemClock clock;
        return clock;
    }
};

// Manually advanced clock for deterministic tests of lease expiry and backoff.
class ManualClock final : public Clock {
public:
    explicit ManualClock(TimestampMs start = 1'000'000) : now_(start) {}

    [[nodiscard]] TimestampMs now_ms() const override {
        return now_.load(std::memory_order_acquire);
    }
    void advance(std::chrono::milliseconds d) {
        now_.fetch_add(d.count(), std::memory_order_acq_rel);
    }
    void set(TimestampMs t) { now_.store(t, std::memory_order_release); }

private:
    std::atomic<TimestampMs> now_;
};

} // namespace flowd
