// flowd command line: run workflows in-process, or submit them to a shared
// DynamoDB table and execute them with any number of worker processes.

#include "flowd/client/workflow_client.hpp"
#include "flowd/concurrency/thread_pool.hpp"
#include "flowd/core/error.hpp"
#include "flowd/core/log.hpp"
#include "flowd/core/workflow_json.hpp"
#include "flowd/engine/dag_executor.hpp"
#include "flowd/engine/handler.hpp"
#include "flowd/engine/timer_service.hpp"
#include "flowd/store/in_memory_task_store.hpp"
#include "flowd/worker/worker.hpp"

#if FLOWD_HAS_DYNAMODB
#include "flowd/store/dynamodb_task_store.hpp"
#endif

#include <algorithm>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <cstdio>
#include <format>
#include <sstream>
#include <string>
#include <vector>

#include <pthread.h>

namespace {

using namespace flowd;
using std::chrono::milliseconds;

constexpr int kExitOk = 0;
constexpr int kExitWorkflowFailed = 1;
constexpr int kExitUsage = 2;
constexpr int kExitError = 3;

constexpr const char* kUsage = R"(usage: flowd <command> [options]

Commands
  run <workflow.json>       Execute a workflow in this process (thread pool engine).
  local <workflow.json>     Execute a workflow with N in-process workers sharing an
                            in-memory store (exercises the distributed protocol).
  init-table                Create the DynamoDB table and index if missing.
  delete-table --yes        Delete the DynamoDB table and everything in it.
  submit <workflow.json>    Persist a workflow to DynamoDB for workers to execute.
  worker                    Claim and execute tasks from DynamoDB until SIGINT/SIGTERM.
  status <workflow-id>      Show workflow progress.
  version                   Print the version.

Common options
  --log-level LEVEL         debug | info | warn | error (default: info)
  --threads N               Executor threads (default: hardware concurrency)

local options
  --workers N               In-process workers (default: 3)

Store options (DynamoDB; environment variable in brackets)
  --table NAME              Table name [FLOWD_TABLE] (default: flowd)
  --region REGION           AWS region [AWS_REGION]
  --endpoint URL            Endpoint override, e.g. http://localhost:8000 [FLOWD_DYNAMODB_ENDPOINT]
  --local                   Use dummy credentials (DynamoDB Local only; needs --endpoint)
  --shards N                Index shards; must match across all processes [FLOWD_SHARDS] (default: 8)

submit options
  --id ID                   Workflow id (default: generated)
  --wait                    Block until the workflow finishes
  --timeout SECONDS         Maximum time to wait (default: 3600)

worker options
  --id ID                   Worker id (default: <host>-<pid>-<random>)
  --max-in-flight N         Tasks claimed at once (default: threads)
  --lease-ms MS             Lease duration (default: 30000)
  --heartbeat-ms MS         Lease renewal interval (default: 10000)
  --drain-timeout-ms MS     Grace period for running tasks on shutdown (default: 30000)

status options
  --tasks                   List every task

Exit status: 0 success, 1 workflow failed, 2 usage error, 3 runtime error.
)";

// std::print is C++23; these are the C++20 equivalents used by the CLI.
template <typename... Args>
void print(std::format_string<Args...> fmt, Args&&... args) {
    std::fputs(std::format(fmt, std::forward<Args>(args)...).c_str(), stdout);
}
template <typename... Args>
void println(std::format_string<Args...> fmt, Args&&... args) {
    print(fmt, std::forward<Args>(args)...);
    std::fputc('\n', stdout);
}
template <typename... Args>
void eprintln(std::format_string<Args...> fmt, Args&&... args) {
    std::fputs((std::format(fmt, std::forward<Args>(args)...) + "\n").c_str(), stderr);
}

class UsageError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Minimal "--flag value" / "--switch" parser.
class Args {
public:
    Args(int argc, char** argv, std::initializer_list<std::string_view> switches) {
        for (int i = 0; i < argc; ++i) {
            std::string_view a = argv[i];
            if (a.starts_with("--")) {
                std::string name(a.substr(2));
                if (auto eq = name.find('='); eq != std::string::npos) {
                    options_[name.substr(0, eq)] = name.substr(eq + 1);
                } else if (std::ranges::find(switches, std::string_view(name)) != switches.end()) {
                    options_[name] = "true";
                } else if (i + 1 < argc) {
                    options_[name] = argv[++i];
                } else {
                    throw UsageError("missing value for --" + name);
                }
            } else {
                positional_.emplace_back(a);
            }
        }
    }

    [[nodiscard]] std::optional<std::string> get(const std::string& name, const char* env = nullptr) {
        used_.push_back(name);
        if (auto it = options_.find(name); it != options_.end()) {
            return it->second;
        }
        if (env != nullptr) {
            // Only read during argument parsing, before any thread starts.
            if (const char* v = std::getenv(env); v != nullptr && *v != '\0') { // NOLINT(concurrency-mt-unsafe)
                return std::string(v);
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] std::string get_or(const std::string& name, std::string fallback,
                                     const char* env = nullptr) {
        return get(name, env).value_or(std::move(fallback));
    }

    [[nodiscard]] bool flag(const std::string& name) { return get(name).has_value(); }

    template <typename T>
    [[nodiscard]] T number(const std::string& name, T fallback, const char* env = nullptr) {
        auto v = get(name, env);
        if (!v) {
            return fallback;
        }
        T out{};
        auto [ptr, ec] = std::from_chars(v->data(), v->data() + v->size(), out);
        if (ec != std::errc{} || ptr != v->data() + v->size()) {
            throw UsageError("--" + name + " expects a number, got '" + *v + "'");
        }
        return out;
    }

    [[nodiscard]] const std::vector<std::string>& positional() const { return positional_; }

    void reject_unknown() const {
        for (const auto& [name, value] : options_) {
            if (std::ranges::find(used_, name) == used_.end()) {
                throw UsageError("unknown option --" + name);
            }
        }
    }

private:
    std::map<std::string, std::string> options_;
    std::vector<std::string> positional_;
    std::vector<std::string> used_;
};

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw Error("cannot read '" + path + "'");
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::size_t default_threads() {
    return std::max<std::size_t>(1, std::thread::hardware_concurrency());
}

void print_summary(const WorkflowSummary& s) {
    println("status={} total={} succeeded={} failed={} running={} retrying={} pending={}",
                 to_string(s.status), s.total, s.succeeded, s.failed, s.running, s.retrying,
                 s.pending);
}

void print_tasks(std::vector<TaskRecord> tasks) {
    std::ranges::sort(tasks, {}, &TaskRecord::task_id);
    println("{:<32} {:<10} {:>7}  {:<28} {}", "TASK", "STATE", "ATTEMPT", "LEASE OWNER",
                 "LAST ERROR");
    for (const TaskRecord& t : tasks) {
        std::string err = t.last_error.substr(0, t.last_error.find('\n'));
        if (err.size() > 80) {
            err.resize(77);
            err += "...";
        }
        println("{:<32} {:<10} {:>7}  {:<28} {}", t.task_id, to_string(t.state), t.attempt,
                     t.lease_owner, err);
    }
}

// Blocks SIGINT/SIGTERM in every thread (call before starting any) and
// returns the first one received.
class SignalWaiter {
public:
    SignalWaiter() {
        sigemptyset(&set_);
        sigaddset(&set_, SIGINT);
        sigaddset(&set_, SIGTERM);
        pthread_sigmask(SIG_BLOCK, &set_, nullptr);
    }
    int wait() {
        int sig = 0;
        sigwait(&set_, &sig);
        return sig;
    }

private:
    sigset_t set_{};
};

HandlerRegistry make_registry() {
    HandlerRegistry registry;
    register_builtin_handlers(registry);
    return registry;
}

// ---- in-process commands -------------------------------------------------------

int cmd_run(Args& args) {
    if (args.positional().size() != 1) {
        throw UsageError("run expects exactly one workflow file");
    }
    const auto threads = args.number<std::size_t>("threads", default_threads());
    args.reject_unknown();

    const Dag dag(parse_workflow_json(read_file(args.positional()[0])));
    HandlerRegistry registry = make_registry();
    concurrency::ThreadPool pool(threads);
    TimerService timers;
    DagExecutor executor(pool, timers, registry);
    log::info("running '{}' ({} tasks) on {} threads", dag.spec().name, dag.size(), threads);
    WorkflowResult result = executor.run(dag, dag.spec().name);

    println("{:<32} {:<10} {:>7}  {}", "TASK", "STATE", "ATTEMPT", "OUTPUT / ERROR");
    for (Dag::Index i : dag.topological_order()) {
        const TaskOutcome& t = result.tasks[i];
        std::string text = t.state == TaskState::Succeeded ? t.output : t.error;
        std::ranges::replace(text, '\n', ' ');
        if (text.size() > 80) {
            text.resize(77);
            text += "...";
        }
        println("{:<32} {:<10} {:>7}  {}", dag.task(i).id, to_string(t.state), t.attempts,
                     text);
    }
    println("{} in {:.3f}s: {} succeeded, {} failed", result.ok() ? "SUCCEEDED" : "FAILED",
                 std::chrono::duration<double>(result.elapsed).count(), result.succeeded,
                 result.failed);
    return result.ok() ? kExitOk : kExitWorkflowFailed;
}

int cmd_local(Args& args) {
    if (args.positional().size() != 1) {
        throw UsageError("local expects exactly one workflow file");
    }
    const auto workers = args.number<std::size_t>("workers", 3);
    const auto threads = args.number<std::size_t>("threads", 4);
    args.reject_unknown();

    const WorkflowSpec spec = parse_workflow_json(read_file(args.positional()[0]));
    HandlerRegistry registry = make_registry();
    InMemoryTaskStore store;
    WorkflowClient client(store);
    const std::string id = client.submit(spec);
    log::info("submitted {} to the in-memory store; starting {} workers", id, workers);

    std::vector<std::unique_ptr<Worker>> pool;
    for (std::size_t i = 0; i < workers; ++i) {
        WorkerOptions o;
        o.worker_id = "local-" + std::to_string(i);
        o.threads = threads;
        o.lease_duration = milliseconds{5'000};
        o.heartbeat_interval = milliseconds{1'000};
        o.maintenance_interval = milliseconds{500};
        o.propagation_grace = milliseconds{2'000};
        pool.push_back(std::make_unique<Worker>(store, registry, o));
        pool.back()->start();
    }
    const WorkflowSummary s = client.wait(id, std::chrono::hours{24}, milliseconds{20});
    for (auto& w : pool) {
        w->stop();
        const auto m = w->metrics();
        println("{}: claimed={} succeeded={} retried={} failed={} conflicts={}", w->id(),
                     m.claimed, m.succeeded, m.retried, m.failed, m.claim_conflicts);
    }
    print_tasks(store.list_tasks(id));
    print_summary(s);
    return s.status == WorkflowSummary::Status::Succeeded ? kExitOk : kExitWorkflowFailed;
}

// ---- DynamoDB commands -----------------------------------------------------------

#if FLOWD_HAS_DYNAMODB

DynamoDbOptions store_options(Args& args) {
    DynamoDbOptions o;
    o.table_name = args.get_or("table", "flowd", "FLOWD_TABLE");
    o.region = args.get_or("region", "", "AWS_REGION");
    o.endpoint = args.get_or("endpoint", "", "FLOWD_DYNAMODB_ENDPOINT");
    o.use_dummy_credentials = args.flag("local");
    o.shard_count = args.number<std::uint32_t>("shards", 8, "FLOWD_SHARDS");
    if (o.use_dummy_credentials && o.endpoint.empty()) {
        throw UsageError("--local requires --endpoint (DynamoDB Local)");
    }
    return o;
}

int cmd_init_table(Args& args) {
    DynamoDbOptions o = store_options(args);
    args.reject_unknown();
    AwsSdkSession sdk;
    DynamoDbTaskStore store(o);
    const bool created = store.create_table();
    println("table '{}' {}", o.table_name, created ? "created" : "already exists");
    return kExitOk;
}

int cmd_delete_table(Args& args) {
    DynamoDbOptions o = store_options(args);
    const bool confirmed = args.flag("yes");
    args.reject_unknown();
    if (!confirmed) {
        throw UsageError("delete-table permanently deletes all workflows; pass --yes to confirm");
    }
    AwsSdkSession sdk;
    DynamoDbTaskStore store(o);
    store.delete_table();
    println("table '{}' deleted", o.table_name);
    return kExitOk;
}

int cmd_submit(Args& args) {
    if (args.positional().size() != 1) {
        throw UsageError("submit expects exactly one workflow file");
    }
    DynamoDbOptions o = store_options(args);
    auto id = args.get("id");
    const bool wait = args.flag("wait");
    const auto timeout = args.number<std::int64_t>("timeout", 3600);
    args.reject_unknown();

    const WorkflowSpec spec = parse_workflow_json(read_file(args.positional()[0]));
    AwsSdkSession sdk;
    DynamoDbTaskStore store(o);
    WorkflowClient client(store);
    const std::string workflow_id = client.submit(spec, id);
    println("{}", workflow_id);
    if (!wait) {
        return kExitOk;
    }
    const WorkflowSummary s = client.wait(workflow_id, std::chrono::seconds{timeout},
                                          milliseconds{1'000});
    print_summary(s);
    if (s.status == WorkflowSummary::Status::Running) {
        return kExitError;
    }
    return s.status == WorkflowSummary::Status::Succeeded ? kExitOk : kExitWorkflowFailed;
}

int cmd_worker(Args& args) {
    DynamoDbOptions o = store_options(args);
    WorkerOptions w;
    w.worker_id = args.get_or("id", "");
    w.threads = args.number<std::size_t>("threads", default_threads());
    w.max_in_flight = args.number<std::size_t>("max-in-flight", 0);
    w.lease_duration = milliseconds{args.number<std::int64_t>("lease-ms", 30'000)};
    w.heartbeat_interval = milliseconds{args.number<std::int64_t>("heartbeat-ms", 10'000)};
    const milliseconds drain{args.number<std::int64_t>("drain-timeout-ms", 30'000)};
    args.reject_unknown();
    if (w.heartbeat_interval * 2 > w.lease_duration) {
        throw UsageError("--heartbeat-ms must be at most half of --lease-ms");
    }

    SignalWaiter signals; // before any thread exists
    AwsSdkSession sdk;
    DynamoDbTaskStore store(o);
    HandlerRegistry registry = make_registry();
    Worker worker(store, registry, w);
    worker.start();
    const int sig = signals.wait();
    log::info("received signal {}, draining", sig);
    worker.stop(drain);
    const auto m = worker.metrics();
    log::info("claimed={} succeeded={} retried={} failed={} leases_lost={} reaped={} repaired={} "
              "store_errors={}",
              m.claimed, m.succeeded, m.retried, m.failed, m.leases_lost, m.leases_reaped,
              m.propagations_repaired, m.store_errors);
    return kExitOk;
}

int cmd_status(Args& args) {
    if (args.positional().size() != 1) {
        throw UsageError("status expects a workflow id");
    }
    DynamoDbOptions o = store_options(args);
    const bool tasks = args.flag("tasks");
    args.reject_unknown();
    AwsSdkSession sdk;
    DynamoDbTaskStore store(o);
    const auto wf = store.get_workflow(args.positional()[0]);
    if (!wf) {
        throw Error("unknown workflow '" + args.positional()[0] + "'");
    }
    std::vector<TaskRecord> records = store.list_tasks(wf->workflow_id);
    println("workflow {} ({})", wf->workflow_id, wf->name);
    if (tasks) {
        print_tasks(records);
    }
    const WorkflowSummary s = summarize(records);
    print_summary(s);
    return s.status == WorkflowSummary::Status::Failed ? kExitWorkflowFailed : kExitOk;
}

#else

int no_dynamodb(Args&) {
    throw Error("this flowd binary was built without DynamoDB support "
                "(configure with -DFLOWD_WITH_DYNAMODB=ON)");
}

#endif

} // namespace

int main(int argc, char** argv) {
    if (argc < 2 || std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h" ||
        std::string_view(argv[1]) == "help") {
        print("{}", kUsage);
        return argc < 2 ? kExitUsage : kExitOk;
    }
    const std::string command = argv[1];
    try {
        Args args(argc - 2, argv + 2, {"wait", "local", "tasks", "yes"});
        log::Level level = log::Level::Info;
        if (auto l = args.get("log-level", "FLOWD_LOG_LEVEL"); l && !log::parse_level(*l, level)) {
            throw UsageError("invalid log level '" + *l + "'");
        }
        log::set_level(level);

        if (command == "version") {
            println("flowd {}", FLOWD_VERSION);
            return kExitOk;
        }
        if (command == "run") {
            return cmd_run(args);
        }
        if (command == "local") {
            return cmd_local(args);
        }
#if FLOWD_HAS_DYNAMODB
        if (command == "init-table") {
            return cmd_init_table(args);
        }
        if (command == "delete-table") {
            return cmd_delete_table(args);
        }
        if (command == "submit") {
            return cmd_submit(args);
        }
        if (command == "worker") {
            return cmd_worker(args);
        }
        if (command == "status") {
            return cmd_status(args);
        }
#else
        if (command == "init-table" || command == "delete-table" || command == "submit" || command == "worker" ||
            command == "status") {
            return no_dynamodb(args);
        }
#endif
        throw UsageError("unknown command '" + command + "'");
    } catch (const UsageError& e) {
        eprintln("flowd: {}\n\n{}", e.what(), kUsage);
        return kExitUsage;
    } catch (const std::exception& e) {
        eprintln("flowd: {}", e.what());
        return kExitError;
    }
}
