cmake_minimum_required(VERSION 3.25)

project(flowd
    VERSION 0.1.0
    DESCRIPTION "Distributed DAG workflow scheduler"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(FLOWD_WITH_DYNAMODB "Build the DynamoDB task store (requires AWS SDK for C++)" OFF)
option(FLOWD_BUILD_TESTS "Build unit and integration tests" ON)
option(FLOWD_BUILD_BENCHMARKS "Build benchmarks" ON)
option(FLOWD_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
set(FLOWD_SANITIZER "" CACHE STRING "Sanitizer to enable: thread, address, undefined, address+undefined")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(FlowdOptions)
include(FlowdDependencies)

add_subdirectory(src)
add_subdirectory(apps)

if(FLOWD_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(FLOWD_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

message(STATUS "Flowd ${PROJECT_VERSION}: dynamodb=${FLOWD_WITH_DYNAMODB} tests=${FLOWD_BUILD_TESTS} "
               "benchmarks=${FLOWD_BUILD_BENCHMARKS} sanitizer='${FLOWD_SANITIZER}'")
