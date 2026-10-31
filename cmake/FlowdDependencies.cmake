# Third-party dependencies. Installed packages are preferred; GoogleTest and
# nlohmann/json fall back to FetchContent so a bare toolchain can build Flowd.

include(FetchContent)
find_package(Threads REQUIRED)

find_package(nlohmann_json 3.11 QUIET)
if(NOT nlohmann_json_FOUND)
    FetchContent_Declare(nlohmann_json
        URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM)
    FetchContent_MakeAvailable(nlohmann_json)
endif()

if(FLOWD_BUILD_TESTS)
    find_package(GTest 1.14 QUIET)
    if(NOT GTest_FOUND)
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
        FetchContent_Declare(googletest
            URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM)
        FetchContent_MakeAvailable(googletest)
    endif()
endif()

if(FLOWD_WITH_DYNAMODB)
    find_package(AWSSDK REQUIRED COMPONENTS dynamodb)
endif()
