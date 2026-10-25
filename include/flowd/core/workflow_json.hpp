#pragma once

#include "flowd/core/workflow.hpp"

#include <string>
#include <string_view>

namespace flowd {

// Parses a workflow definition document:
//
//   {
//     "name": "nightly-etl",
//     "defaults": {
//       "retry":   {"max_attempts": 3, "initial_backoff_ms": 200,
//                   "multiplier": 2.0, "max_backoff_ms": 10000, "jitter": 0.2},
//       "timeout_ms": 60000
//     },
//     "tasks": [
//       {"id": "extract", "handler": "shell", "payload": {"command": "./extract.sh"}},
//       {"id": "load", "handler": "shell", "depends_on": ["extract"],
//        "retry": {"max_attempts": 5}, "payload": {"command": "./load.sh"}}
//     ]
//   }
//
// Task-level "retry" fields override individual default fields. "payload"
// may be any JSON value and is stored as its serialized JSON text, so
// handlers always receive a JSON document. Throws
// ValidationError on malformed input. The result is not yet validated as a
// DAG; construct a Dag from it for that.
[[nodiscard]] WorkflowSpec parse_workflow_json(std::string_view text);

[[nodiscard]] std::string workflow_to_json(const WorkflowSpec& spec);

} // namespace flowd
