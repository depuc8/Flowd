include(GoogleTest)

# Store-agnostic suites shared by the unit and integration binaries.
add_library(flowd_test_common STATIC
    common/task_store_contract.cpp
    common/test_main.cpp
    common/worker_scenarios.cpp
)
target_link_libraries(flowd_test_common
    PUBLIC flowd::core GTest::gtest GTest::gmock
    PRIVATE nlohmann_json::nlohmann_json)

add_executable(flowd_unit_tests
    unit/dag_executor_test.cpp
    unit/handler_test.cpp
    unit/in_memory_store_test.cpp
    unit/lockfree_queue_test.cpp
    unit/retry_policy_test.cpp
    unit/task_state_test.cpp
    unit/thread_pool_test.cpp
    unit/workflow_test.cpp
)
# The parameterized suites live in the static library and are only
# referenced through static registration, so link it as a whole.
target_link_libraries(flowd_unit_tests PRIVATE
    $<LINK_LIBRARY:WHOLE_ARCHIVE,flowd_test_common> flowd::core GTest::gtest GTest::gmock)
gtest_discover_tests(flowd_unit_tests
    DISCOVERY_TIMEOUT 60
    PROPERTIES LABELS unit TIMEOUT 300)
