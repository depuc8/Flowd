#include "flowd/core/workflow_json.hpp"

#include "flowd/core/error.hpp"

#include <nlohmann/json.hpp>

namespace flowd {
namespace {

using nlohmann::json;

template <typename T>
T get_field(const json& obj, const char* key, const T& fallback, const std::string& where) {
    auto it = obj.find(key);
    if (it == obj.end() || it->is_null()) {
        return fallback;
    }
    try {
        return it->get<T>();
    } catch (const json::exception&) {
        throw ValidationError(where + ": field '" + key + "' has the wrong type");
    }
}

RetryPolicy parse_retry(const json& node, RetryPolicy base, const std::string& where) {
    if (node.is_null()) {
        return base;
    }
    if (!node.is_object()) {
        throw ValidationError(where + ": 'retry' must be an object");
    }
    const std::string ctx = where + ".retry";
    base.max_attempts = get_field<std::uint32_t>(node, "max_attempts", base.max_attempts, ctx);
    base.initial_backoff = std::chrono::milliseconds{
        get_field<std::int64_t>(node, "initial_backoff_ms", base.initial_backoff.count(), ctx)};
    base.multiplier = get_field<double>(node, "multiplier", base.multiplier, ctx);
    base.max_backoff = std::chrono::milliseconds{
        get_field<std::int64_t>(node, "max_backoff_ms", base.max_backoff.count(), ctx)};
    base.jitter = get_field<double>(node, "jitter", base.jitter, ctx);
    return base;
}

json retry_to_json(const RetryPolicy& r) {
    return json{{"max_attempts", r.max_attempts},
                {"initial_backoff_ms", r.initial_backoff.count()},
                {"multiplier", r.multiplier},
                {"max_backoff_ms", r.max_backoff.count()},
                {"jitter", r.jitter}};
}

} // namespace

WorkflowSpec parse_workflow_json(std::string_view text) {
    json doc;
    try {
        doc = json::parse(text);
    } catch (const json::parse_error& e) {
        throw ValidationError(std::string("workflow is not valid JSON: ") + e.what());
    }
    if (!doc.is_object()) {
        throw ValidationError("workflow document must be a JSON object");
    }

    WorkflowSpec spec;
    spec.name = get_field<std::string>(doc, "name", "", "workflow");
    if (spec.name.empty()) {
        throw ValidationError("workflow: 'name' is required");
    }

    RetryPolicy default_retry;
    std::chrono::milliseconds default_timeout{0};
    if (auto it = doc.find("defaults"); it != doc.end() && !it->is_null()) {
        if (!it->is_object()) {
            throw ValidationError("workflow: 'defaults' must be an object");
        }
        default_retry = parse_retry(it->value("retry", json{}), default_retry, "defaults");
        default_timeout =
            std::chrono::milliseconds{get_field<std::int64_t>(*it, "timeout_ms", 0, "defaults")};
    }

    auto tasks = doc.find("tasks");
    if (tasks == doc.end() || !tasks->is_array()) {
        throw ValidationError("workflow: 'tasks' must be an array");
    }
    spec.tasks.reserve(tasks->size());
    std::size_t i = 0;
    for (const json& node : *tasks) {
        const std::string where = "tasks[" + std::to_string(i++) + "]";
        if (!node.is_object()) {
            throw ValidationError(where + " must be an object");
        }
        TaskSpec t;
        t.id = get_field<std::string>(node, "id", "", where);
        t.handler = get_field<std::string>(node, "handler", "", where);
        t.depends_on = get_field<std::vector<std::string>>(node, "depends_on", {}, where);
        t.retry = parse_retry(node.value("retry", json{}), default_retry, where);
        t.timeout = std::chrono::milliseconds{
            get_field<std::int64_t>(node, "timeout_ms", default_timeout.count(), where)};
        if (auto p = node.find("payload"); p != node.end() && !p->is_null()) {
            t.payload = p->dump();
        }
        spec.tasks.push_back(std::move(t));
    }
    return spec;
}

std::string workflow_to_json(const WorkflowSpec& spec) {
    json tasks = json::array();
    for (const TaskSpec& t : spec.tasks) {
        json node{{"id", t.id},
                  {"handler", t.handler},
                  {"depends_on", t.depends_on},
                  {"retry", retry_to_json(t.retry)},
                  {"timeout_ms", t.timeout.count()}};
        if (!t.payload.empty()) {
            json payload = json::parse(t.payload, nullptr, /*allow_exceptions=*/false);
            node["payload"] = payload.is_discarded() ? json(t.payload) : std::move(payload);
        }
        tasks.push_back(std::move(node));
    }
    return json{{"name", spec.name}, {"tasks", std::move(tasks)}}.dump(2);
}

} // namespace flowd
