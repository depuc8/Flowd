// Runs the store contract suite and the distributed worker scenarios
// against a real DynamoDB endpoint (normally DynamoDB Local).
//
//   FLOWD_DYNAMODB_ENDPOINT=http://localhost:8000 ./flowd_integration_tests
//
// Every test gets its own freshly created table, which is deleted again
// afterwards. Without FLOWD_DYNAMODB_ENDPOINT all tests are skipped. The
// suite refuses to run against anything but a local endpoint so that it can
// never create resources in a real AWS account.

#include "../common/task_store_contract.hpp"
#include "../common/worker_scenarios.hpp"

#include "flowd/core/error.hpp"
#include "flowd/core/workflow.hpp"
#include "flowd/store/dynamodb_task_store.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <random>

namespace flowd::testing {
namespace {

std::string endpoint() {
    const char* e = std::getenv("FLOWD_DYNAMODB_ENDPOINT");
    return e == nullptr ? std::string{} : std::string(e);
}

bool is_local(const std::string& url) {
    return url.starts_with("http://localhost") || url.starts_with("http://127.0.0.1") ||
           url.starts_with("http://dynamodb-local") || url.starts_with("http://[::1]");
}

class AwsEnvironment : public ::testing::Environment {
public:
    void SetUp() override { session_ = std::make_unique<AwsSdkSession>(); }
    void TearDown() override { session_.reset(); }

private:
    std::unique_ptr<AwsSdkSession> session_;
};

[[maybe_unused]] auto* const g_env = ::testing::AddGlobalTestEnvironment(new AwsEnvironment);

// Owns a throw-away table for the lifetime of one test.
class ScratchTableStore final : public TaskStore {
public:
    explicit ScratchTableStore(std::uint32_t shards) {
        static std::atomic<int> counter{0};
        DynamoDbOptions o;
        o.endpoint = endpoint();
        o.use_dummy_credentials = true;
        o.region = "us-east-1";
        o.shard_count = shards;
        o.table_name = "flowd_it_" + std::to_string(std::random_device{}() % 1'000'000) + "_" +
                       std::to_string(counter++);
        store_ = std::make_unique<DynamoDbTaskStore>(o);
        store_->create_table();
    }
    ~ScratchTableStore() override {
        try {
            store_->delete_table();
        } catch (...) {
        }
    }

    std::uint32_t shard_count() const override { return store_->shard_count(); }
    void create_workflow(const WorkflowRecord& w, const std::vector<TaskRecord>& t) override {
        store_->create_workflow(w, t);
    }
    std::optional<WorkflowRecord> get_workflow(std::string_view id) override {
        return store_->get_workflow(id);
    }
    std::vector<TaskRecord> list_tasks(std::string_view wf) override { return store_->list_tasks(wf); }
    std::optional<TaskRecord> get_task(const TaskKey& k) override { return store_->get_task(k); }
    std::vector<TaskKey> find_ready(std::uint32_t s, TimestampMs now, std::size_t l) override {
        return store_->find_ready(s, now, l);
    }
    std::vector<TaskRecord> find_expired_leases(std::uint32_t s, TimestampMs now,
                                                std::size_t l) override {
        return store_->find_expired_leases(s, now, l);
    }
    std::vector<TaskRecord> find_unpropagated(std::uint32_t s, TimestampMs b,
                                              std::size_t l) override {
        return store_->find_unpropagated(s, b, l);
    }
    std::optional<TaskRecord> try_claim(const TaskKey& k, std::string_view w, TimestampMs now,
                                        TimestampMs exp) override {
        return store_->try_claim(k, w, now, exp);
    }
    bool renew_lease(const TaskKey& k, std::string_view w, std::uint32_t a,
                     TimestampMs exp) override {
        return store_->renew_lease(k, w, a, exp);
    }
    bool complete(const TaskKey& k, std::string_view w, std::uint32_t a, std::string_view o,
                  TimestampMs now) override {
        return store_->complete(k, w, a, o, now);
    }
    bool fail(const TaskKey& k, std::string_view w, std::uint32_t a,
              const FailureTransition& t) override {
        return store_->fail(k, w, a, t);
    }
    bool expire_lease(const TaskKey& k, std::uint32_t a, const FailureTransition& t) override {
        return store_->expire_lease(k, a, t);
    }
    bool resolve_dependency(const TaskKey& c, std::string_view p, TimestampMs now) override {
        return store_->resolve_dependency(c, p, now);
    }
    std::optional<TaskRecord> fail_upstream(const TaskKey& c, std::string_view e,
                                            TimestampMs now) override {
        return store_->fail_upstream(c, e, now);
    }
    void mark_propagated(const TaskKey& k) override { store_->mark_propagated(k); }

private:
    std::unique_ptr<DynamoDbTaskStore> store_;
};

bool configured() {
    const std::string e = endpoint();
    return !e.empty() && is_local(e);
}

} // namespace

// The parameters always exist so that test discovery does not depend on the
// environment; without DynamoDB Local every test reports itself as skipped.
std::unique_ptr<TaskStore> scratch_store(std::uint32_t shards) {
    if (!configured()) {
        return nullptr;
    }
    return std::make_unique<ScratchTableStore>(shards);
}

std::vector<StoreFactory> store_factories() {
    return {
        StoreFactory{"DynamoDbSharded", [] { return scratch_store(8); }},
        StoreFactory{"DynamoDbSingleShard", [] { return scratch_store(1); }},
    };
}

std::vector<ScenarioStore> scenario_stores() {
    return {ScenarioStore{"DynamoDb", [] { return scratch_store(4); }, 3}};
}

TEST(DynamoDbIntegration, EndpointIsConfigured) {
    const std::string e = endpoint();
    if (e.empty()) {
        GTEST_SKIP() << "FLOWD_DYNAMODB_ENDPOINT not set; start DynamoDB Local with "
                        "scripts/dynamodb-local.sh start";
    }
    ASSERT_TRUE(is_local(e)) << "integration tests only run against a local endpoint, got " << e;
}

TEST(DynamoDbIntegration, LargeWorkflowUsesBatchedCreation) {
    if (!configured()) {
        GTEST_SKIP() << "DynamoDB Local not configured";
    }
    // > 99 tasks takes the non-transactional path (conditional META put +
    // BatchWriteItem). Verify every task landed and only roots are ready.
    ScratchTableStore store(4);
    std::vector<TaskSpec> tasks;
    for (int i = 0; i < 260; ++i) {
        TaskSpec t{.id = "t" + std::to_string(i), .handler = "noop"};
        if (i >= 10) {
            t.depends_on = {"t" + std::to_string(i % 10)};
        }
        tasks.push_back(t);
    }
    const Dag dag(WorkflowSpec{"big", tasks});
    const TimestampMs now = 1'000;
    store.create_workflow(WorkflowRecord{.workflow_id = "big", .name = "big", .task_count = 260},
                          make_task_records(dag, "big", store.shard_count(), now));
    EXPECT_EQ(store.list_tasks("big").size(), 260U);
    std::size_t ready = 0;
    for (std::uint32_t s = 0; s < store.shard_count(); ++s) {
        ready += store.find_ready(s, now, 1000).size();
    }
    EXPECT_EQ(ready, 10U);
    EXPECT_THROW(store.create_workflow(WorkflowRecord{.workflow_id = "big", .name = "big"},
                                       make_task_records(dag, "big", store.shard_count(), now)),
                 AlreadyExists);
}

} // namespace flowd::testing
