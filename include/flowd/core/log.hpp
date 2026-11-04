#pragma once

#include <format>
#include <string>
#include <string_view>
#include <utility>

namespace flowd::log {

enum class Level { Debug = 0, Info = 1, Warn = 2, Error = 3, Off = 4 };

void set_level(Level level) noexcept;
[[nodiscard]] Level level() noexcept;
[[nodiscard]] bool parse_level(std::string_view text, Level& out) noexcept;

// Writes one line "<utc timestamp> <LEVEL> <message>" to stderr. Lines from
// concurrent threads never interleave.
void write(Level level, std::string_view message);

template <typename... Args>
void debug(std::format_string<Args...> fmt, Args&&... args) {
    if (level() <= Level::Debug) {
        write(Level::Debug, std::format(fmt, std::forward<Args>(args)...));
    }
}
template <typename... Args>
void info(std::format_string<Args...> fmt, Args&&... args) {
    if (level() <= Level::Info) {
        write(Level::Info, std::format(fmt, std::forward<Args>(args)...));
    }
}
template <typename... Args>
void warn(std::format_string<Args...> fmt, Args&&... args) {
    if (level() <= Level::Warn) {
        write(Level::Warn, std::format(fmt, std::forward<Args>(args)...));
    }
}
template <typename... Args>
void error(std::format_string<Args...> fmt, Args&&... args) {
    if (level() <= Level::Error) {
        write(Level::Error, std::format(fmt, std::forward<Args>(args)...));
    }
}

} // namespace flowd::log
