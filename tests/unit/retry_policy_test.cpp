#include "flowd/core/error.hpp"
#include "flowd/core/retry_policy.hpp"

#include <gtest/gtest.h>

namespace flowd {
namespace {

using std::chrono::milliseconds;

TEST(RetryPolicy, ExponentialGrowthIsCapped) {
    RetryPolicy p{.max_attempts = 10,
                  .initial_backoff = milliseconds{100},
                  .multiplier = 2.0,
                  .max_backoff = milliseconds{1000},
                  .jitter = 0.0};
    EXPECT_EQ(p.backoff(0), milliseconds{0});
    EXPECT_EQ(p.backoff(1), milliseconds{100});
    EXPECT_EQ(p.backoff(2), milliseconds{200});
    EXPECT_EQ(p.backoff(3), milliseconds{400});
    EXPECT_EQ(p.backoff(4), milliseconds{800});
    EXPECT_EQ(p.backoff(5), milliseconds{1000});
    EXPECT_EQ(p.backoff(60), milliseconds{1000}); // no overflow
    EXPECT_EQ(p.backoff(4'000'000'000U), milliseconds{1000});
}

TEST(RetryPolicy, JitterScalesDelayDownwards) {
    RetryPolicy p{.max_attempts = 5,
                  .initial_backoff = milliseconds{1000},
                  .multiplier = 1.0,
                  .max_backoff = milliseconds{1000},
                  .jitter = 0.5};
    EXPECT_EQ(p.backoff(1, 0.0), milliseconds{1000});
    EXPECT_EQ(p.backoff(1, 0.5), milliseconds{750});
    EXPECT_EQ(p.backoff(1, 0.999), milliseconds{500});
}

TEST(RetryPolicy, CanRetryCountsTotalAttempts) {
    RetryPolicy p{.max_attempts = 3};
    EXPECT_TRUE(p.can_retry(1));
    EXPECT_TRUE(p.can_retry(2));
    EXPECT_FALSE(p.can_retry(3));
    RetryPolicy once{.max_attempts = 1};
    EXPECT_FALSE(once.can_retry(1));
}

TEST(RetryPolicy, ValidationRejectsInconsistentSettings) {
    EXPECT_NO_THROW(RetryPolicy{}.validate());
    EXPECT_THROW((RetryPolicy{.max_attempts = 0}.validate()), ValidationError);
    EXPECT_THROW((RetryPolicy{.multiplier = 0.5}.validate()), ValidationError);
    EXPECT_THROW((RetryPolicy{.jitter = 1.5}.validate()), ValidationError);
    EXPECT_THROW((RetryPolicy{.initial_backoff = milliseconds{10}, .max_backoff = milliseconds{5}}
                      .validate()),
                 ValidationError);
}

} // namespace
} // namespace flowd
