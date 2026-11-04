#pragma once

#include "flowd/store/task_store.hpp"

#include <gtest/gtest.h>

#include <functional>
#include <ostream>
#include <memory>
#include <string>
#include <vector>

namespace flowd::testing {

// A named factory producing a fresh, empty store, or nullptr when the
// backend is not available in this environment (the test is then skipped). The contract suite in
// task_store_contract.cpp runs against every instantiation, so the in-memory
// and DynamoDB implementations are held to identical semantics.
struct StoreFactory {
    std::string name;
    std::function<std::unique_ptr<TaskStore>()> make;
};

inline void PrintTo(const StoreFactory& f, std::ostream* os) {
    *os << f.name;
}

inline std::string factory_name(const ::testing::TestParamInfo<StoreFactory>& info) {
    return info.param.name;
}

// Defined by each test binary: the stores the contract suite runs against.
std::vector<StoreFactory> store_factories();

class TaskStoreContract : public ::testing::TestWithParam<StoreFactory> {
protected:
    void SetUp() override;

    // Unique per test so that stores backed by a shared table do not see
    // each other's data.
    [[nodiscard]] std::string workflow_id(const std::string& suffix = "wf") const;

    std::unique_ptr<TaskStore> store_;
};

} // namespace flowd::testing
