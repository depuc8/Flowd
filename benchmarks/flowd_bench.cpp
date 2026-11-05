// Flowd benchmark suite. Prints Markdown tables; every number is measured
// on the machine running the binary.
//
//   flowd_bench [--quick] [--only queue,pool,dag,exec,workers,dynamodb]
//
// The dynamodb section needs a build with FLOWD_WITH_DYNAMODB and
// FLOWD_DYNAMODB_ENDPOINT pointing at DynamoDB Local.

#include "flowd/client/workflow_client.hpp"
#include "flowd/concurrency/lockfree_queue.hpp"
#include "flowd/concurrency/thread_pool.hpp"
#include "flowd/core/log.hpp"
#include "flowd/engine/dag_executor.hpp"
#include "flowd/store/in_memory_task_store.hpp"
#include "flowd/worker/worker.hpp"

#if FLOWD_BENCH_DYNAMODB
#include "flowd/store/dynamodb_task_store.hpp"
#endif

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <format>
#include <fstream>
#include <functional>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace flowd;
using Clock = std::chrono::steady_clock;

template <typename... Args>
void out(std::format_string<Args...> fmt, Args&&... args) {
    std::fputs(std::format(fmt, std::forward<Args>(args)...).c_str(), stdout);
    std::fflush(stdout);
}

double seconds_since(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

// Median of `reps` runs of `f`, which returns elapsed seconds.
double median_of(int reps, const std::function<double()>& f) {
    std::vector<double> v;
    v.reserve(static_cast<std::size_t>(reps));
    for (int i = 0; i < reps; ++i) {
        v.push_back(f());
    }
    std::ranges::sort(v);
    return v[v.size() / 2];
}

std::string cpu_model() {
    std::ifstream in("/proc/cpuinfo");
    std::string line;
    while (std::getline(in, line)) {
        if (line.starts_with("model name")) {
            return line.substr(line.find(':') + 2);
        }
    }
    return "unknown";
}

// Baseline for the queue benchmark.
template <typename T>
class MutexQueue {
public:
    void push(T v) {
        std::lock_guard lock(m_);
        q_.push_back(std::move(v));
    }
    std::optional<T> try_pop() {
        std::lock_guard lock(m_);
        if (q_.empty()) {
            return std::nullopt;
        }
        T v = std::move(q_.front());
        q_.pop_front();
        return v;
    }

private:
    std::mutex m_;
    std::deque<T> q_;
};

template <typename Queue>
double queue_run(int producers, int consumers, std::uint64_t total) {
    Queue q;
    const std::uint64_t per_producer = total / static_cast<std::uint64_t>(producers);
    const std::uint64_t expected = per_producer * static_cast<std::uint64_t>(producers);
    std::atomic<std::uint64_t> consumed{0};
    std::barrier sync(producers + consumers + 1);
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(producers) + static_cast<std::size_t>(consumers));
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&] {
            sync.arrive_and_wait();
            for (std::uint64_t i = 0; i < per_producer; ++i) {
                q.push(i);
            }
        });
    }
    for (int c = 0; c < consumers; ++c) {
        threads.emplace_back([&] {
            sync.arrive_and_wait();
            while (consumed.load(std::memory_order_relaxed) < expected) {
                if (q.try_pop()) {
                    consumed.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    sync.arrive_and_wait();
    const auto start = Clock::now();
    for (auto& t : threads) {
        t.join();
    }
    return seconds_since(start);
}

void bench_queue(bool quick) {
    const std::uint64_t ops = quick ? 400'000 : 2'000'000;
    out("\n### Queue: LockFreeQueue vs std::mutex + std::deque\n\n");
    out("{} push+pop pairs per run, median of 3 runs.\n\n", ops);
    out("| producers x consumers | lock-free (M ops/s) | mutex (M ops/s) | lock-free / mutex |\n");
    out("|---|---:|---:|---:|\n");
    for (auto [p, c] : {std::pair{1, 1}, std::pair{2, 2}, std::pair{4, 4}, std::pair{8, 8}}) {
        const double lf = median_of(3, [&] {
            return queue_run<concurrency::LockFreeQueue<std::uint64_t>>(p, c, ops);
        });
        const double mx = median_of(3, [&] { return queue_run<MutexQueue<std::uint64_t>>(p, c, ops); });
        const double lf_rate = static_cast<double>(ops) / lf / 1e6;
        const double mx_rate = static_cast<double>(ops) / mx / 1e6;
        out("| {} x {} | {:.2f} | {:.2f} | {:.2f}x |\n", p, c, lf_rate, mx_rate, lf_rate / mx_rate);
    }
}

void bench_pool(bool quick) {
    const int jobs = quick ? 200'000 : 1'000'000;
    out("\n### Thread pool: empty job throughput\n\n");
    out("{} jobs submitted by 4 producer threads, median of 3 runs (submit to last completion).\n\n",
        jobs);
    out("| pool threads | jobs/s |\n|---:|---:|\n");
    for (std::size_t threads : {1U, 2U, 4U, 8U, 16U}) {
        const double t = median_of(3, [&] {
            std::atomic<int> done{0};
            concurrency::ThreadPool pool(threads);
            const auto start = Clock::now();
            std::vector<std::thread> producers;
            producers.reserve(4);
            for (int p = 0; p < 4; ++p) {
                producers.emplace_back([&] {
                    for (int i = 0; i < jobs / 4; ++i) {
                        pool.submit([&done] { done.fetch_add(1, std::memory_order_relaxed); });
                    }
                });
            }
            for (auto& p : producers) {
                p.join();
            }
            pool.shutdown();
            return seconds_since(start);
        });
        out("| {} | {:.0f} |\n", threads, static_cast<double>(jobs) / t);
    }
}

TaskSpec noop(std::string id, std::vector<std::string> deps = {}) {
    return TaskSpec{.id = std::move(id), .handler = "noop", .depends_on = std::move(deps)};
}

WorkflowSpec make_shape(const std::string& shape, int n) {
    WorkflowSpec spec{.name = shape};
    spec.tasks.reserve(static_cast<std::size_t>(n));
    if (shape == "independent") {
        for (int i = 0; i < n; ++i) {
            spec.tasks.push_back(noop("t" + std::to_string(i)));
        }
    } else if (shape == "chain") {
        for (int i = 0; i < n; ++i) {
            spec.tasks.push_back(i == 0 ? noop("t0") : noop("t" + std::to_string(i), {"t" + std::to_string(i - 1)}));
        }
    } else { // layered: width 100, each task depends on 3 tasks of the previous layer
        const int width = 100;
        for (int i = 0; i < n; ++i) {
            const int layer = i / width;
            const int pos = i % width;
            std::set<std::string> deps;
            if (layer > 0) {
                for (int k = 0; k < 3; ++k) {
                    deps.insert("t" + std::to_string((layer - 1) * width + (pos + k * 37) % width));
                }
            }
            spec.tasks.push_back(noop("t" + std::to_string(i), {deps.begin(), deps.end()}));
        }
    }
    return spec;
}

void bench_dag(bool quick) {
    HandlerRegistry registry;
    register_builtin_handlers(registry);
    const std::size_t threads = std::max(1U, std::thread::hardware_concurrency());
    concurrency::ThreadPool pool(threads);
    TimerService timers;
    DagExecutor executor(pool, timers, registry);

    out("\n### DAG executor: scheduling throughput (no-op tasks)\n\n");
    out("In-process engine on {} threads. Time from start to last task completion, median of 3 runs; "
        "DAG construction excluded.\n\n",
        threads);
    out("| shape | tasks | edges | time (ms) | tasks/s |\n|---|---:|---:|---:|---:|\n");
    std::vector<int> sizes = quick ? std::vector<int>{1'000, 10'000} : std::vector<int>{1'000, 10'000, 100'000};
    for (const std::string shape : {"independent", "layered", "chain"}) {
        for (int n : sizes) {
            if (shape == "chain" && n > 10'000 && quick) {
                continue;
            }
            const Dag dag(make_shape(shape, n));
            std::size_t edges = 0;
            for (Dag::Index i = 0; i < dag.size(); ++i) {
                edges += dag.parents(i).size();
            }
            const double t = median_of(3, [&] {
                WorkflowResult r = executor.run(dag);
                if (!r.ok()) {
                    std::abort();
                }
                return std::chrono::duration<double>(r.elapsed).count();
            });
            out("| {} | {} | {} | {:.1f} | {:.0f} |\n", shape, n, edges, t * 1e3,
                static_cast<double>(n) / t);
        }
    }
}

void bench_exec(bool quick) {
    // Tasks that block for 1 ms (I/O-like). Throughput should scale with the
    // number of threads until the machine saturates.
    HandlerRegistry registry;
    register_builtin_handlers(registry);
    const int n = quick ? 2'000 : 10'000;
    out("\n### Concurrent execution: {} independent tasks that each sleep 1 ms\n\n", n);
    out("| pool threads | time (ms) | tasks/s | speed-up vs 1 thread (ideal = threads) |\n");
    out("|---:|---:|---:|---:|\n");
    WorkflowSpec spec{.name = "sleepers"};
    for (int i = 0; i < n; ++i) {
        spec.tasks.push_back(TaskSpec{.id = "s" + std::to_string(i), .handler = "sleep", .payload = R"({"ms":1})"});
    }
    double base = 0;
    for (std::size_t threads : {1U, 16U, 64U, 256U, 1024U, 4096U}) {
        concurrency::ThreadPool pool(threads);
        TimerService timers;
        DagExecutor executor(pool, timers, registry);
        // Sequentially this takes ~1 ms per task, so the single-thread
        // baseline runs a tenth of the tasks.
        WorkflowSpec sub = spec;
        const int run_n = threads == 1 ? n / 10 : n;
        sub.tasks.resize(static_cast<std::size_t>(run_n));
        const Dag d(sub);
        WorkflowResult r = executor.run(d);
        const double t = std::chrono::duration<double>(r.elapsed).count();
        const double rate = static_cast<double>(run_n) / t;
        if (threads == 1) {
            base = rate;
        }
        out("| {} | {:.0f}{} | {:.0f} | {:.1f}x |\n", threads, t * 1e3,
            threads == 1 ? std::format(" ({} tasks)", run_n) : "", rate, rate / base);
    }
}

void bench_workers(bool quick) {
    // The full distributed protocol (claim, lease, fenced commit, dependency
    // propagation) against the in-memory store: measures coordination
    // overhead independent of network latency.
    HandlerRegistry registry;
    register_builtin_handlers(registry);
    const int n = quick ? 5'000 : 20'000;
    out("\n### Distributed workers over the in-memory store ({} tasks)\n\n", n);
    out("Each worker: 4 executor threads, max 16 tasks in flight. Submission excluded.\n\n");
    out("| workload | workers | time (ms) | tasks/s | claim conflicts |\n|---|---:|---:|---:|---:|\n");
    for (const std::string shape : {"independent", "layered"}) {
        const Dag dag(make_shape(shape, n));
        for (int workers : {1, 2, 4, 8}) {
            InMemoryTaskStore store(16);
            WorkflowClient client(store);
            const std::string id = client.submit(dag.spec(), "bench");
            std::vector<std::unique_ptr<Worker>> pool;
            const auto start = Clock::now();
            for (int w = 0; w < workers; ++w) {
                WorkerOptions o;
                o.worker_id = "w" + std::to_string(w);
                o.threads = 4;
                o.max_in_flight = 16;
                o.min_poll_interval = std::chrono::milliseconds{1};
                o.max_poll_interval = std::chrono::milliseconds{5};
                pool.push_back(std::make_unique<Worker>(store, registry, o));
                pool.back()->start();
            }
            WorkflowSummary s = client.wait(id, std::chrono::minutes{10}, std::chrono::milliseconds{2});
            const double t = seconds_since(start);
            std::uint64_t conflicts = 0;
            for (auto& w : pool) {
                w->stop();
                conflicts += w->metrics().claim_conflicts;
            }
            if (s.status != WorkflowSummary::Status::Succeeded) {
                std::abort();
            }
            out("| {} | {} | {:.0f} | {:.0f} | {} |\n", shape, workers, t * 1e3,
                static_cast<double>(n) / t, conflicts);
        }
    }
}

#if FLOWD_BENCH_DYNAMODB
void bench_dynamodb(bool quick) {
    // Same protocol over DynamoDB Local. DynamoDB Local runs on one machine
    // (SQLite-backed), so absolute numbers say little about AWS; the point is
    // the per-task request cost and how workers share a real network store.
    const char* endpoint = std::getenv("FLOWD_DYNAMODB_ENDPOINT");
    if (endpoint == nullptr) {
        out("\n### Distributed workers over DynamoDB Local\n\nskipped: FLOWD_DYNAMODB_ENDPOINT not set\n");
        return;
    }
    HandlerRegistry registry;
    register_builtin_handlers(registry);
    const int n = quick ? 300 : 2'000;
    out("\n### Distributed workers over DynamoDB Local ({} tasks, endpoint {})\n\n", n, endpoint);
    out("Each worker: 8 executor threads, max 32 in flight. `submit` = workflow creation; "
        "`run` = first claim to workflow completion.\n\n");
    out("| workload | workers | submit (ms) | run (ms) | tasks/s | claim conflicts |\n");
    out("|---|---:|---:|---:|---:|---:|\n");
    flowd::AwsSdkSession sdk;
    for (const std::string shape : {"independent", "layered"}) {
        const Dag dag(make_shape(shape, n));
        for (int workers : {1, 2, 4}) {
            DynamoDbOptions o;
            o.endpoint = endpoint;
            o.use_dummy_credentials = true;
            o.region = "us-east-1";
            o.table_name = "flowd_bench_" + std::to_string(workers) + "_" + shape;
            DynamoDbTaskStore store(o);
            store.delete_table();
            store.create_table();
            WorkflowClient client(store);
            auto start = Clock::now();
            const std::string id = client.submit(dag.spec(), "bench");
            const double submit = seconds_since(start);
            std::vector<std::unique_ptr<Worker>> pool;
            start = Clock::now();
            for (int w = 0; w < workers; ++w) {
                WorkerOptions wo;
                wo.worker_id = "w" + std::to_string(w);
                wo.threads = 8;
                wo.max_in_flight = 32;
                wo.min_poll_interval = std::chrono::milliseconds{5};
                wo.max_poll_interval = std::chrono::milliseconds{50};
                pool.push_back(std::make_unique<Worker>(store, registry, wo));
                pool.back()->start();
            }
            WorkflowSummary s = client.wait(id, std::chrono::minutes{10}, std::chrono::milliseconds{20});
            const double t = seconds_since(start);
            std::uint64_t conflicts = 0;
            for (auto& w : pool) {
                w->stop();
                conflicts += w->metrics().claim_conflicts;
            }
            store.delete_table();
            if (s.status != WorkflowSummary::Status::Succeeded) {
                std::abort();
            }
            out("| {} | {} | {:.0f} | {:.0f} | {:.0f} | {} |\n", shape, workers, submit * 1e3, t * 1e3,
                static_cast<double>(n) / t, conflicts);
        }
    }
}
#endif

} // namespace

int main(int argc, char** argv) {
    bool quick = false;
    std::string only = "queue,pool,dag,exec,workers,dynamodb";
    for (int i = 1; i < argc; ++i) {
        std::string_view a = argv[i];
        if (a == "--quick") {
            quick = true;
        } else if (a == "--only" && i + 1 < argc) {
            only = argv[++i];
        } else {
            std::fprintf(stderr, "usage: flowd_bench [--quick] [--only queue,pool,dag,exec,workers,dynamodb]\n");
            return 2;
        }
    }
    flowd::log::set_level(flowd::log::Level::Error);
    auto enabled = [&](std::string_view name) { return only.find(name) != std::string::npos; };

    out("## Flowd benchmarks\n\nCPU: {} ({} hardware threads){}\n", cpu_model(),
        std::thread::hardware_concurrency(), quick ? ", quick mode" : "");
    if (enabled("queue")) {
        bench_queue(quick);
    }
    if (enabled("pool")) {
        bench_pool(quick);
    }
    if (enabled("dag")) {
        bench_dag(quick);
    }
    if (enabled("exec")) {
        bench_exec(quick);
    }
    if (enabled("workers")) {
        bench_workers(quick);
    }
#if FLOWD_BENCH_DYNAMODB
    if (enabled("dynamodb")) {
        bench_dynamodb(quick);
    }
#endif
    return 0;
}
