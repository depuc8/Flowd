include(GoogleTest)

add_executable(flowd_unit_tests
    unit/task_state_test.cpp
    unit/retry_policy_test.cpp
    unit/workflow_test.cpp
)
target_link_libraries(flowd_unit_tests PRIVATE flowd::core GTest::gtest_main GTest::gmock)
gtest_discover_tests(flowd_unit_tests
    DISCOVERY_TIMEOUT 60
    PROPERTIES LABELS unit TIMEOUT 300)
