add_library(flowd_core
    core/retry_policy.cpp
    core/workflow.cpp
    core/workflow_json.cpp
)
add_library(flowd::core ALIAS flowd_core)
target_include_directories(flowd_core PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
target_link_libraries(flowd_core
    PUBLIC flowd::options Threads::Threads
    PRIVATE nlohmann_json::nlohmann_json)
