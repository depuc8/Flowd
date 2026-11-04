#pragma once

#include "flowd/store/task_store.hpp"

#include <functional>
#include <ostream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace flowd::testing {

// Distributed-execution scenarios run against any TaskStore: the in-memory
// store in the unit test binary and DynamoDB Local in the integration binary.
struct ScenarioStore {
    std::string name;
    // Returns nullptr when the backend is not available (test is skipped).
    std::function<std::unique_ptr<TaskStore>()> make;
    // Timing scale: DynamoDB round trips are much slower than memory.
    int time_scale = 1;
};

inline void PrintTo(const ScenarioStore& s, std::ostream* os) {
    *os << s.name;
}

inline std::string scenario_name(const ::testing::TestParamInfo<ScenarioStore>& info) {
    return info.param.name;
}

// Defined by each test binary: the stores the scenarios run against.
std::vector<ScenarioStore> scenario_stores();

} // namespace flowd::testing
