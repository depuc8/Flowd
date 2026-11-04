#include "../common/task_store_contract.hpp"
#include "../common/worker_scenarios.hpp"

#include "flowd/store/in_memory_task_store.hpp"

namespace flowd::testing {

std::vector<StoreFactory> store_factories() {
    return {
        StoreFactory{"InMemorySharded", [] { return std::make_unique<InMemoryTaskStore>(8); }},
        StoreFactory{"InMemorySingleShard", [] { return std::make_unique<InMemoryTaskStore>(1); }},
    };
}

std::vector<ScenarioStore> scenario_stores() {
    return {ScenarioStore{"InMemory", [] { return std::make_unique<InMemoryTaskStore>(8); }, 1}};
}

} // namespace flowd::testing
