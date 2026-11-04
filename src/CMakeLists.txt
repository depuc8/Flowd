add_library(flowd_core
    client/workflow_client.cpp
    concurrency/hazard_pointer.cpp
    concurrency/thread_pool.cpp
    core/log.cpp
    core/retry_policy.cpp
    core/workflow.cpp
    core/workflow_json.cpp
    engine/builtin_handlers.cpp
    engine/dag_executor.cpp
    engine/handler.cpp
    engine/timer_service.cpp
    store/in_memory_task_store.cpp
    store/task_store.cpp
    worker/worker.cpp
)
add_library(flowd::core ALIAS flowd_core)
target_include_directories(flowd_core PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
target_link_libraries(flowd_core
    PUBLIC flowd::options Threads::Threads
    PRIVATE nlohmann_json::nlohmann_json)
