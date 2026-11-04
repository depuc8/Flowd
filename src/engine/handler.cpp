#include "flowd/engine/handler.hpp"

#include "flowd/core/error.hpp"

#include <exception>

namespace flowd {

void HandlerRegistry::add(std::string name, TaskHandler handler) {
    if (name.empty() || !handler) {
        throw Error("handler registration requires a name and a callable");
    }
    handlers_.insert_or_assign(std::move(name), std::move(handler));
}

const TaskHandler* HandlerRegistry::find(std::string_view name) const {
    auto it = handlers_.find(name);
    return it == handlers_.end() ? nullptr : &it->second;
}

TaskResult HandlerRegistry::invoke(std::string_view handler_name, const TaskContext& ctx) const {
    const TaskHandler* handler = find(handler_name);
    if (handler == nullptr) {
        // A worker without the handler cannot make progress on any attempt,
        // but another worker build might have it; keep it retryable.
        return TaskResult::failure("no handler registered under '" + std::string(handler_name) +
                                   "'");
    }
    try {
        return (*handler)(ctx);
    } catch (const std::exception& e) {
        return TaskResult::failure(std::string("handler threw: ") + e.what());
    } catch (...) {
        return TaskResult::failure("handler threw a non-standard exception");
    }
}

} // namespace flowd
