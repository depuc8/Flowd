#include "task_store_contract.hpp"

#include "flowd/core/error.hpp"
#include "flowd/core/workflow.hpp"

#include <gmock/gmock.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

namespace flowd::testing {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

void TaskStoreContract::SetUp() {
    store_ = GetParam().make();
    ASSERT_NE(store_, nullptr);
}

std::string TaskStoreContract::workflow_id(const std::string& suffix) const {
    static std::atomic<int> counter{0};
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string name = std::string(info->name()) + "-" + suffix + "-" + std::to_string(counter++) +
                       "-" + std::to_string(std::random_device{}() % 100000);
    std::ranges::replace(name, '/', '-');
    return name;
}

namespace {

constexpr TimestampMs kT0 = 1'700'000'000'000;

TaskSpec spec(std::string id, std::vector<std::string> deps = {}) {
    TaskSpec t{.id = std::move(id), .handler = "noop", .payload = R"({"x":1})",
               .depends_on = std::move(deps)};
    t.retry.max_attempts = 3;
    return t;
}

// a -> c, b -> c, c -> d
std::vector<TaskRecord> diamond_records(const std::string& wf, std::uint32_t shards) {
    Dag dag(WorkflowSpec{"diamond", {spec("a"), spec("b"), spec("c", {"a", "b"}), spec("d", {"c"})}});
    return make_task_records(dag, wf, shards, kT0);
}

void create(TaskStore& store, const std::string& wf, const std::vector<TaskRecord>& tasks) {
    store.create_workflow(WorkflowRecord{.workflow_id = wf,
                                         .name = "diamond",
                                         .task_count = tasks.size(),
                                         .created_at = kT0,
                                         .definition = "{}"},
                          tasks);
}

std::vector<std::string> all_ready(TaskStore& store, const std::string& wf, TimestampMs now) {
    std::vector<std::string> ids;
    for (std::uint32_t s = 0; s < store.shard_count(); ++s) {
        for (const TaskKey& k : store.find_ready(s, now, 1000)) {
            if (k.workflow_id == wf) {
                ids.push_back(k.task_id);
            }
        }
    }
    std::ranges::sort(ids);
    return ids;
}

template <typename F>
std::vector<std::string> collect(TaskStore& store, const std::string& wf, F&& find) {
    std::vector<std::string> ids;
    for (std::uint32_t s = 0; s < store.shard_count(); ++s) {
        for (const TaskRecord& r : find(s)) {
            if (r.workflow_id == wf) {
                ids.push_back(r.task_id);
            }
        }
    }
    std::ranges::sort(ids);
    return ids;
}

} // namespace

TEST_P(TaskStoreContract, CreatesWorkflowAndExposesRootsAsReady) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));

    auto meta = store_->get_workflow(wf);
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->name, "diamond");
    EXPECT_EQ(meta->task_count, 4U);
    EXPECT_FALSE(store_->get_workflow(wf + "-missing").has_value());

    auto tasks = store_->list_tasks(wf);
    ASSERT_EQ(tasks.size(), 4U);
    std::vector<std::string> ids;
    for (const auto& t : tasks) {
        ids.push_back(t.task_id);
        EXPECT_EQ(t.state, TaskState::Pending);
        EXPECT_EQ(t.attempt, 0U);
    }
    EXPECT_THAT(ids, UnorderedElementsAre("a", "b", "c", "d"));

    auto c = store_->get_task({wf, "c"});
    ASSERT_TRUE(c.has_value());
    EXPECT_THAT(c->depends_on, ElementsAre("a", "b"));
    EXPECT_THAT(c->pending_deps, ElementsAre("a", "b"));
    EXPECT_THAT(c->children, ElementsAre("d"));
    EXPECT_EQ(c->payload, R"({"x":1})");
    EXPECT_EQ(c->retry.max_attempts, 3U);

    EXPECT_THAT(all_ready(*store_, wf, kT0), ElementsAre("a", "b"));
    EXPECT_THROW(create(*store_, wf, diamond_records(wf, store_->shard_count())), AlreadyExists);
}

TEST_P(TaskStoreContract, ClaimIsExclusiveAndRecordsLease) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));

    auto claimed = store_->try_claim({wf, "a"}, "w1", kT0, kT0 + 1000);
    ASSERT_TRUE(claimed.has_value());
    EXPECT_EQ(claimed->state, TaskState::Running);
    EXPECT_EQ(claimed->attempt, 1U);
    EXPECT_EQ(claimed->lease_owner, "w1");
    EXPECT_EQ(claimed->lease_expires_at, kT0 + 1000);

    EXPECT_FALSE(store_->try_claim({wf, "a"}, "w2", kT0, kT0 + 1000).has_value());
    EXPECT_FALSE(store_->try_claim({wf, "c"}, "w2", kT0, kT0 + 1000).has_value()) << "blocked";
    EXPECT_FALSE(store_->try_claim({wf, "nope"}, "w2", kT0, kT0 + 1000).has_value());
    EXPECT_THAT(all_ready(*store_, wf, kT0), ElementsAre("b"));

    auto expired = [&](TimestampMs now) {
        return collect(*store_, wf,
                       [&](std::uint32_t s) { return store_->find_expired_leases(s, now, 100); });
    };
    EXPECT_THAT(expired(kT0 + 1000), IsEmpty());
    EXPECT_THAT(expired(kT0 + 1001), ElementsAre("a"));
}

TEST_P(TaskStoreContract, ConcurrentClaimsHaveExactlyOneWinner) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    constexpr int kThreads = 16;
    std::atomic<int> winners{0};
    std::barrier sync(kThreads);
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            sync.arrive_and_wait();
            if (store_->try_claim({wf, "a"}, "w" + std::to_string(i), kT0, kT0 + 5000)) {
                winners.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(winners.load(), 1);
    EXPECT_EQ(store_->get_task({wf, "a"})->attempt, 1U);
}

TEST_P(TaskStoreContract, LeaseOperationsAreFencedByOwnerAndAttempt) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    ASSERT_TRUE(store_->try_claim({wf, "a"}, "w1", kT0, kT0 + 1000));

    EXPECT_FALSE(store_->renew_lease({wf, "a"}, "w2", 1, kT0 + 5000));
    EXPECT_FALSE(store_->renew_lease({wf, "a"}, "w1", 2, kT0 + 5000));
    EXPECT_TRUE(store_->renew_lease({wf, "a"}, "w1", 1, kT0 + 5000));
    EXPECT_EQ(store_->get_task({wf, "a"})->lease_expires_at, kT0 + 5000);

    EXPECT_FALSE(store_->complete({wf, "a"}, "w2", 1, "x", kT0 + 10));
    EXPECT_FALSE(store_->complete({wf, "a"}, "w1", 7, "x", kT0 + 10));
    const FailureTransition retry{TaskState::Retrying, kT0 + 50, "err", kT0 + 10};
    EXPECT_FALSE(store_->fail({wf, "a"}, "w2", 1, retry));

    ASSERT_TRUE(store_->complete({wf, "a"}, "w1", 1, "result", kT0 + 20));
    auto a = store_->get_task({wf, "a"});
    EXPECT_EQ(a->state, TaskState::Succeeded);
    EXPECT_EQ(a->output, "result");
    EXPECT_TRUE(a->needs_propagation);
    EXPECT_TRUE(a->lease_owner.empty());
    // A terminal task accepts nothing any more.
    EXPECT_FALSE(store_->complete({wf, "a"}, "w1", 1, "again", kT0 + 30));
    EXPECT_FALSE(store_->renew_lease({wf, "a"}, "w1", 1, kT0 + 9000));
    EXPECT_FALSE(store_->fail({wf, "a"}, "w1", 1, retry));
}

TEST_P(TaskStoreContract, FailedAttemptBecomesClaimableAfterBackoff) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    ASSERT_TRUE(store_->try_claim({wf, "a"}, "w1", kT0, kT0 + 1000));
    ASSERT_TRUE(store_->fail({wf, "a"}, "w1", 1,
                             FailureTransition{TaskState::Retrying, kT0 + 500, "boom", kT0 + 100}));
    auto a = store_->get_task({wf, "a"});
    EXPECT_EQ(a->state, TaskState::Retrying);
    EXPECT_EQ(a->last_error, "boom");
    EXPECT_EQ(a->available_at, kT0 + 500);

    EXPECT_THAT(all_ready(*store_, wf, kT0 + 499), ElementsAre("b"));
    EXPECT_FALSE(store_->try_claim({wf, "a"}, "w2", kT0 + 499, kT0 + 2000)) << "still backing off";
    EXPECT_THAT(all_ready(*store_, wf, kT0 + 500), ElementsAre("a", "b"));

    auto second = store_->try_claim({wf, "a"}, "w2", kT0 + 500, kT0 + 2000);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->attempt, 2U);
    EXPECT_EQ(second->lease_owner, "w2");

    ASSERT_TRUE(store_->fail({wf, "a"}, "w2", 2,
                             FailureTransition{TaskState::Failed, 0, "fatal", kT0 + 600}));
    a = store_->get_task({wf, "a"});
    EXPECT_EQ(a->state, TaskState::Failed);
    EXPECT_TRUE(a->needs_propagation);
    EXPECT_THAT(all_ready(*store_, wf, kT0 + 10'000), ElementsAre("b"));
}

TEST_P(TaskStoreContract, ExpiredLeaseIsReclaimedAndStaleOwnerIsFenced) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    ASSERT_TRUE(store_->try_claim({wf, "a"}, "crashed", kT0, kT0 + 1000));

    const FailureTransition reap{TaskState::Retrying, kT0 + 1001, "lease expired", kT0 + 1001};
    const FailureTransition early{TaskState::Retrying, kT0 + 999, "lease expired", kT0 + 999};
    EXPECT_FALSE(store_->expire_lease({wf, "a"}, 1, early)) << "lease still valid";
    EXPECT_FALSE(store_->expire_lease({wf, "a"}, 2, reap)) << "wrong attempt";
    ASSERT_TRUE(store_->expire_lease({wf, "a"}, 1, reap));
    EXPECT_FALSE(store_->expire_lease({wf, "a"}, 1, reap)) << "already reaped";

    auto again = store_->try_claim({wf, "a"}, "healthy", kT0 + 1001, kT0 + 5000);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->attempt, 2U);

    // The original owner wakes up and tries to commit: rejected.
    EXPECT_FALSE(store_->complete({wf, "a"}, "crashed", 1, "stale", kT0 + 1002));
    EXPECT_FALSE(store_->renew_lease({wf, "a"}, "crashed", 1, kT0 + 9000));
    EXPECT_TRUE(store_->complete({wf, "a"}, "healthy", 2, "fresh", kT0 + 1003));
    EXPECT_EQ(store_->get_task({wf, "a"})->output, "fresh");
}

TEST_P(TaskStoreContract, RenewedLeaseIsNotReaped) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    ASSERT_TRUE(store_->try_claim({wf, "a"}, "w1", kT0, kT0 + 1000));
    ASSERT_TRUE(store_->renew_lease({wf, "a"}, "w1", 1, kT0 + 3000));
    EXPECT_FALSE(store_->expire_lease(
        {wf, "a"}, 1, FailureTransition{TaskState::Retrying, kT0 + 2000, "x", kT0 + 2000}));
    EXPECT_EQ(store_->get_task({wf, "a"})->state, TaskState::Running);
}

TEST_P(TaskStoreContract, ResolveDependencyIsIdempotentAndReleasesChildOnce) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    EXPECT_FALSE(store_->resolve_dependency({wf, "c"}, "a", kT0 + 10));
    EXPECT_FALSE(store_->resolve_dependency({wf, "c"}, "a", kT0 + 11)) << "repeat is a no-op";
    EXPECT_THAT(store_->get_task({wf, "c"})->pending_deps, ElementsAre("b"));
    EXPECT_FALSE(store_->try_claim({wf, "c"}, "w", kT0 + 12, kT0 + 1000));

    EXPECT_TRUE(store_->resolve_dependency({wf, "c"}, "b", kT0 + 20));
    EXPECT_FALSE(store_->resolve_dependency({wf, "c"}, "b", kT0 + 21));
    EXPECT_FALSE(store_->resolve_dependency({wf, "c"}, "a", kT0 + 22));
    EXPECT_THAT(store_->get_task({wf, "c"})->pending_deps, IsEmpty());
    EXPECT_THAT(all_ready(*store_, wf, kT0 + 20), ElementsAre("a", "b", "c"));
    EXPECT_TRUE(store_->try_claim({wf, "c"}, "w", kT0 + 20, kT0 + 1000).has_value());
    EXPECT_FALSE(store_->resolve_dependency({wf, "c"}, "b", kT0 + 23)) << "running stays running";
}

TEST_P(TaskStoreContract, ConcurrentResolutionReleasesChildExactlyOnce) {
    const std::string wf = workflow_id();
    std::vector<TaskSpec> tasks;
    std::vector<std::string> parents;
    for (int i = 0; i < 8; ++i) {
        parents.push_back("p" + std::to_string(i));
        tasks.push_back(spec(parents.back()));
    }
    tasks.push_back(spec("child", parents));
    Dag dag(WorkflowSpec{"fan-in", tasks});
    create(*store_, wf, make_task_records(dag, wf, store_->shard_count(), kT0));

    std::atomic<int> releases{0};
    std::vector<std::thread> threads;
    for (const auto& p : parents) {
        // Every parent resolves twice (normal path + repair) concurrently.
        for (int rep = 0; rep < 2; ++rep) {
            threads.emplace_back([&, p] {
                if (store_->resolve_dependency({wf, "child"}, p, kT0 + 5)) {
                    releases.fetch_add(1);
                }
            });
        }
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(releases.load(), 1);
    EXPECT_TRUE(store_->get_task({wf, "child"})->pending_deps.empty());
}

TEST_P(TaskStoreContract, UpstreamFailureOnlyAffectsPendingTasks) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    auto failed = store_->fail_upstream({wf, "c"}, "upstream task 'a' failed", kT0 + 5);
    ASSERT_TRUE(failed.has_value());
    EXPECT_EQ(failed->state, TaskState::Failed);
    EXPECT_THAT(failed->children, ElementsAre("d"));
    EXPECT_TRUE(failed->needs_propagation);
    EXPECT_FALSE(store_->fail_upstream({wf, "c"}, "again", kT0 + 6).has_value());

    ASSERT_TRUE(store_->try_claim({wf, "a"}, "w", kT0, kT0 + 1000));
    EXPECT_FALSE(store_->fail_upstream({wf, "a"}, "x", kT0 + 7).has_value()) << "running task";
    EXPECT_EQ(store_->get_task({wf, "c"})->last_error, "upstream task 'a' failed");
}

TEST_P(TaskStoreContract, UnpropagatedTasksAreDiscoverableUntilMarked) {
    const std::string wf = workflow_id();
    create(*store_, wf, diamond_records(wf, store_->shard_count()));
    ASSERT_TRUE(store_->try_claim({wf, "a"}, "w", kT0, kT0 + 1000));
    ASSERT_TRUE(store_->complete({wf, "a"}, "w", 1, "", kT0 + 100));
    ASSERT_TRUE(store_->fail_upstream({wf, "d"}, "x", kT0 + 200).has_value());

    auto unpropagated = [&](TimestampMs before) {
        return collect(*store_, wf,
                       [&](std::uint32_t s) { return store_->find_unpropagated(s, before, 100); });
    };
    EXPECT_THAT(unpropagated(kT0 + 100), IsEmpty());
    EXPECT_THAT(unpropagated(kT0 + 101), ElementsAre("a"));
    EXPECT_THAT(unpropagated(kT0 + 10'000), ElementsAre("a", "d"));
    store_->mark_propagated({wf, "a"});
    store_->mark_propagated({wf, "a"});
    EXPECT_THAT(unpropagated(kT0 + 10'000), ElementsAre("d"));
    EXPECT_FALSE(store_->get_task({wf, "a"})->needs_propagation);
}

TEST_P(TaskStoreContract, LargeWorkflowIsDistributedOverShards) {
    const std::string wf = workflow_id();
    std::vector<TaskSpec> tasks;
    for (int i = 0; i < 300; ++i) {
        tasks.push_back(spec("t" + std::to_string(i)));
    }
    Dag dag(WorkflowSpec{"wide", tasks});
    create(*store_, wf, make_task_records(dag, wf, store_->shard_count(), kT0));
    EXPECT_EQ(store_->list_tasks(wf).size(), 300U);
    EXPECT_EQ(all_ready(*store_, wf, kT0).size(), 300U);
    if (store_->shard_count() > 1) {
        std::uint32_t non_empty = 0;
        for (std::uint32_t s = 0; s < store_->shard_count(); ++s) {
            non_empty += store_->find_ready(s, kT0, 1).empty() ? 0U : 1U;
        }
        EXPECT_GT(non_empty, 1U);
    }
}

INSTANTIATE_TEST_SUITE_P(Stores, TaskStoreContract, ::testing::ValuesIn(store_factories()),
                         factory_name);

} // namespace flowd::testing
