#include "flowd/core/error.hpp"
#include "flowd/core/workflow.hpp"
#include "flowd/core/workflow_json.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>

namespace flowd {
namespace {

using ::testing::ElementsAre;
using ::testing::HasSubstr;

TaskSpec task(std::string id, std::vector<std::string> deps = {}) {
    return TaskSpec{.id = std::move(id), .handler = "noop", .depends_on = std::move(deps)};
}

WorkflowSpec workflow(std::vector<TaskSpec> tasks) {
    return WorkflowSpec{.name = "wf", .tasks = std::move(tasks)};
}

std::string validation_message(const WorkflowSpec& spec) {
    try {
        Dag dag(spec);
    } catch (const ValidationError& e) {
        return e.what();
    }
    return {};
}

TEST(Dag, BuildsDiamond) {
    Dag dag(workflow({task("a"), task("b", {"a"}), task("c", {"a"}), task("d", {"b", "c"})}));
    ASSERT_EQ(dag.size(), 4U);
    EXPECT_THAT(dag.roots(), ElementsAre(0U));
    EXPECT_THAT(dag.children(0), ElementsAre(1U, 2U));
    EXPECT_THAT(dag.parents(3), ElementsAre(1U, 2U));
    EXPECT_THAT(dag.topological_order(), ElementsAre(0U, 1U, 2U, 3U));
    EXPECT_THAT(dag.descendants(0), ElementsAre(1U, 2U, 3U));
    EXPECT_THAT(dag.descendants(1), ElementsAre(3U));
    EXPECT_EQ(dag.index_of("c"), 2U);
    EXPECT_THROW((void)dag.index_of("zzz"), std::out_of_range);
}

TEST(Dag, TopologicalOrderRespectsEveryEdge) {
    // Declared in reverse so the order cannot just mirror the input.
    Dag dag(workflow({task("e", {"d", "b"}), task("d", {"c"}), task("c", {"a"}), task("b", {"a"}),
                      task("a")}));
    std::vector<std::size_t> position(dag.size());
    auto order = dag.topological_order();
    for (std::size_t i = 0; i < order.size(); ++i) {
        position[order[i]] = i;
    }
    for (Dag::Index v = 0; v < dag.size(); ++v) {
        for (Dag::Index p : dag.parents(v)) {
            EXPECT_LT(position[p], position[v]);
        }
    }
}

TEST(Dag, RejectsCycles) {
    EXPECT_THAT(validation_message(workflow({task("a", {"c"}), task("b", {"a"}), task("c", {"b"}),
                                             task("root")})),
                HasSubstr("cycle"));
    EXPECT_THAT(validation_message(workflow({task("a", {"a"})})), HasSubstr("depends on itself"));
}

TEST(Dag, RejectsMalformedDefinitions) {
    EXPECT_THAT(validation_message(workflow({})), HasSubstr("at least one task"));
    EXPECT_THAT(validation_message(workflow({task("a"), task("a")})), HasSubstr("duplicate"));
    EXPECT_THAT(validation_message(workflow({task("a", {"missing"})})), HasSubstr("unknown task"));
    EXPECT_THAT(validation_message(workflow({task("a"), task("b", {"a", "a"})})),
                HasSubstr("twice"));
    EXPECT_THAT(validation_message(workflow({task("bad id")})), HasSubstr("invalid task id"));
    EXPECT_THAT(validation_message(workflow({task("#workflow")})), HasSubstr("invalid task id"));

    TaskSpec no_handler = task("a");
    no_handler.handler.clear();
    EXPECT_THAT(validation_message(workflow({no_handler})), HasSubstr("no handler"));

    TaskSpec bad_retry = task("a");
    bad_retry.retry.max_attempts = 0;
    EXPECT_THAT(validation_message(workflow({bad_retry})), HasSubstr("max_attempts"));
}

TEST(Dag, HandlesLargeLayeredGraphs) {
    // 100 layers x 100 tasks, every task depends on 3 tasks of the previous layer.
    std::vector<TaskSpec> tasks;
    constexpr int kLayers = 100;
    constexpr int kWidth = 100;
    for (int l = 0; l < kLayers; ++l) {
        for (int w = 0; w < kWidth; ++w) {
            std::vector<std::string> deps;
            if (l > 0) {
                for (int k = 0; k < 3; ++k) {
                    deps.push_back("t" + std::to_string(l - 1) + "_" +
                                   std::to_string((w + k * 31) % kWidth));
                }
            }
            tasks.push_back(task("t" + std::to_string(l) + "_" + std::to_string(w), deps));
        }
    }
    Dag dag(workflow(std::move(tasks)));
    EXPECT_EQ(dag.size(), static_cast<std::size_t>(kLayers * kWidth));
    EXPECT_EQ(dag.roots().size(), static_cast<std::size_t>(kWidth));
    EXPECT_EQ(dag.topological_order().size(), dag.size());
}

TEST(WorkflowJson, ParsesDefaultsAndOverrides) {
    const char* doc = R"({
        "name": "etl",
        "defaults": {"retry": {"max_attempts": 4, "initial_backoff_ms": 50}, "timeout_ms": 1000},
        "tasks": [
            {"id": "extract", "handler": "shell", "payload": {"command": "echo hi"}},
            {"id": "load", "handler": "noop", "depends_on": ["extract"],
             "retry": {"max_attempts": 7}, "timeout_ms": 5}
        ]
    })";
    WorkflowSpec spec = parse_workflow_json(doc);
    EXPECT_EQ(spec.name, "etl");
    ASSERT_EQ(spec.tasks.size(), 2U);
    EXPECT_EQ(spec.tasks[0].retry.max_attempts, 4U);
    EXPECT_EQ(spec.tasks[0].retry.initial_backoff.count(), 50);
    EXPECT_EQ(spec.tasks[0].timeout.count(), 1000);
    EXPECT_EQ(spec.tasks[0].payload, R"({"command":"echo hi"})");
    EXPECT_EQ(spec.tasks[1].retry.max_attempts, 7U);
    EXPECT_EQ(spec.tasks[1].retry.initial_backoff.count(), 50); // inherited
    EXPECT_EQ(spec.tasks[1].timeout.count(), 5);
    EXPECT_THAT(spec.tasks[1].depends_on, ElementsAre("extract"));

    // Round trip.
    WorkflowSpec again = parse_workflow_json(workflow_to_json(spec));
    ASSERT_EQ(again.tasks.size(), 2U);
    EXPECT_EQ(again.tasks[0].payload, spec.tasks[0].payload);
    EXPECT_EQ(again.tasks[1].retry, spec.tasks[1].retry);
    EXPECT_EQ(again.tasks[1].depends_on, spec.tasks[1].depends_on);
}

TEST(WorkflowJson, RejectsMalformedDocuments) {
    EXPECT_THROW((void)parse_workflow_json("{"), ValidationError);
    EXPECT_THROW((void)parse_workflow_json("[]"), ValidationError);
    EXPECT_THROW((void)parse_workflow_json(R"({"tasks": []})"), ValidationError);
    EXPECT_THROW((void)parse_workflow_json(R"({"name": "x", "tasks": {}})"), ValidationError);
    EXPECT_THROW((void)parse_workflow_json(R"({"name": "x", "tasks": [{"id": 5}]})"),
                 ValidationError);
}

} // namespace
} // namespace flowd
