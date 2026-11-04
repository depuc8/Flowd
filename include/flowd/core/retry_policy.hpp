#pragma once

#include <chrono>
#include <cstdint>

namespace flowd {

// Exponential backoff with optional jitter.
//
//   delay(n) = min(max_backoff, initial_backoff * multiplier^(n-1))
//
// where n is the number of attempts that have failed so far (n >= 1). With
// jitter j in [0, 1] the delay is scaled by a uniformly distributed factor in
// [1 - j, 1], which spreads out retries of tasks that failed together.
struct RetryPolicy {
    // Total number of attempts, including the first one. 1 disables retries.
    std::uint32_t max_attempts = 3;
    std::chrono::milliseconds initial_backoff{100};
    double multiplier = 2.0;
    std::chrono::milliseconds max_backoff{30'000};
    double jitter = 0.0;

    // Throws ValidationError if the policy is inconsistent.
    void validate() const;

    // True if another attempt may be started after `attempts_made` attempts.
    [[nodiscard]] bool can_retry(std::uint32_t attempts_made) const noexcept {
        return attempts_made < max_attempts;
    }

    // Delay before the next attempt after `failed_attempts` failures.
    // `random01` must be in [0, 1); it is ignored when jitter is 0.
    [[nodiscard]] std::chrono::milliseconds backoff(std::uint32_t failed_attempts,
                                                    double random01 = 0.0) const noexcept;

    friend bool operator==(const RetryPolicy&, const RetryPolicy&) = default;
};

} // namespace flowd
