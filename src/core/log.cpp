#include "flowd/core/log.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>

namespace flowd::log {
namespace {

std::atomic<Level> g_level{Level::Info};
std::mutex g_mutex;

constexpr std::string_view name(Level l) {
    switch (l) {
    case Level::Debug:
        return "DEBUG";
    case Level::Info:
        return "INFO ";
    case Level::Warn:
        return "WARN ";
    case Level::Error:
        return "ERROR";
    case Level::Off:
        break;
    }
    return "?????";
}

} // namespace

void set_level(Level l) noexcept {
    g_level.store(l, std::memory_order_relaxed);
}

Level level() noexcept {
    return g_level.load(std::memory_order_relaxed);
}

bool parse_level(std::string_view text, Level& out) noexcept {
    if (text == "debug") {
        out = Level::Debug;
    } else if (text == "info") {
        out = Level::Info;
    } else if (text == "warn") {
        out = Level::Warn;
    } else if (text == "error") {
        out = Level::Error;
    } else if (text == "off") {
        out = Level::Off;
    } else {
        return false;
    }
    return true;
}

void write(Level l, std::string_view message) {
    const auto now = std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
    const std::string line = std::format("{:%FT%T}Z {} {}\n", now, name(l), message);
    std::lock_guard lock(g_mutex);
    std::fwrite(line.data(), 1, line.size(), stderr);
}

} // namespace flowd::log
