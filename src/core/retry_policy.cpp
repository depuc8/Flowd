#include "flowd/core/retry_policy.hpp"

#include "flowd/core/error.hpp"

#include <algorithm>
#include <cmath>

namespace flowd {

void RetryPolicy::validate() const {
    if (max_attempts == 0) {
        throw ValidationError("retry.max_attempts must be at least 1");
    }
    if (initial_backoff.count() < 0 || max_backoff.count() < 0) {
        throw ValidationError("retry backoff durations must not be negative");
    }
    if (max_backoff < initial_backoff) {
        throw ValidationError("retry.max_backoff must be >= retry.initial_backoff");
    }
    if (!(multiplier >= 1.0) || !std::isfinite(multiplier)) {
        throw ValidationError("retry.multiplier must be a finite number >= 1");
    }
    if (!(jitter >= 0.0 && jitter <= 1.0)) {
        throw ValidationError("retry.jitter must be within [0, 1]");
    }
}

std::chrono::milliseconds RetryPolicy::backoff(std::uint32_t failed_attempts,
                                               double random01) const noexcept {
    if (failed_attempts == 0) {
        return std::chrono::milliseconds{0};
    }
    const auto cap = static_cast<double>(max_backoff.count());
    const double exponent = static_cast<double>(failed_attempts - 1);
    double delay = static_cast<double>(initial_backoff.count()) * std::pow(multiplier, exponent);
    if (!std::isfinite(delay) || delay > cap) {
        delay = cap;
    }
    if (jitter > 0.0) {
        const double r = std::clamp(random01, 0.0, 1.0);
        delay *= 1.0 - jitter * r;
    }
    return std::chrono::milliseconds{static_cast<std::int64_t>(delay)};
}

} // namespace flowd
