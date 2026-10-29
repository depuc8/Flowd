#include "flowd/core/log.hpp"

#include <gtest/gtest.h>

#include <cstdlib>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    flowd::log::Level level = flowd::log::Level::Error;
    if (const char* env = std::getenv("FLOWD_LOG_LEVEL")) {
        (void)flowd::log::parse_level(env, level);
    }
    flowd::log::set_level(level);
    return RUN_ALL_TESTS();
}
