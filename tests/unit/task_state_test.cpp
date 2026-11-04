#include "flowd/core/task_state.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>

namespace flowd {
namespace {

constexpr std::array kAllStates{TaskState::Pending, TaskState::Running, TaskState::Retrying,
                                TaskState::Succeeded, TaskState::Failed};

TEST(TaskState, RoundTripsThroughStrings) {
    for (TaskState s : kAllStates) {
        auto parsed = parse_task_state(to_string(s));
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(*parsed, s);
    }
    EXPECT_FALSE(parse_task_state("running").has_value());
    EXPECT_FALSE(parse_task_state("").has_value());
}

TEST(TaskState, TerminalStates) {
    EXPECT_TRUE(is_terminal(TaskState::Succeeded));
    EXPECT_TRUE(is_terminal(TaskState::Failed));
    EXPECT_FALSE(is_terminal(TaskState::Pending));
    EXPECT_FALSE(is_terminal(TaskState::Running));
    EXPECT_FALSE(is_terminal(TaskState::Retrying));
}

TEST(TaskState, TransitionTableMatchesSpecification) {
    using S = TaskState;
    // Exhaustive: every pair not listed here must be rejected.
    const std::array allowed{
        std::pair{S::Pending, S::Running},   std::pair{S::Pending, S::Failed},
        std::pair{S::Running, S::Succeeded}, std::pair{S::Running, S::Retrying},
        std::pair{S::Running, S::Failed},    std::pair{S::Retrying, S::Running},
    };
    for (S from : kAllStates) {
        for (S to : kAllStates) {
            const bool expected = std::find(allowed.begin(), allowed.end(), std::pair{from, to}) !=
                                  allowed.end();
            EXPECT_EQ(is_valid_transition(from, to), expected)
                << to_string(from) << " -> " << to_string(to);
        }
    }
}

TEST(TaskState, TerminalStatesHaveNoOutgoingEdges) {
    for (TaskState to : kAllStates) {
        EXPECT_FALSE(is_valid_transition(TaskState::Succeeded, to));
        EXPECT_FALSE(is_valid_transition(TaskState::Failed, to));
    }
}

} // namespace
} // namespace flowd
