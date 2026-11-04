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

if(FLOWD_WITH_DYNAMODB)
    add_executable(flowd_integration_tests integration/dynamodb_store_test.cpp)
    target_link_libraries(flowd_integration_tests PRIVATE
        $<LINK_LIBRARY:WHOLE_ARCHIVE,flowd_test_common> flowd::dynamodb GTest::gtest GTest::gmock)
    gtest_discover_tests(flowd_integration_tests
        DISCOVERY_TIMEOUT 60
        PROPERTIES LABELS integration TIMEOUT 600)

    # End-to-end: real worker processes, one of them killed with SIGKILL.
    add_test(NAME crash_recovery_multiprocess
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/integration/crash_recovery.sh $<TARGET_FILE:flowd>)
    set_tests_properties(crash_recovery_multiprocess PROPERTIES
        LABELS integration TIMEOUT 600 SKIP_RETURN_CODE 77)
endif()
