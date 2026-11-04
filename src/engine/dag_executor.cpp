#include "flowd/engine/dag_executor.hpp"

#include "flowd/core/error.hpp"

#include <atomic>
#include <random>
#include <stop_token>

namespace flowd {

struct DagExecutor::Run {
    struct Node {
        std::atomic<std::uint32_t> remaining_parents{0};
        std::atomic<TaskState> state{TaskState::Pending};
        // Written only by the thread running the current attempt (or by the
        // CAS winner that fails the task as an upstream casualty) and read
        // after the run completed; the terminal counter orders the accesses.
        std::uint32_t attempts = 0;
        std::string output;
        std::string error;
    };

    std::shared_ptr<const Dag> dag;
    std::string workflow_id;
    std::unique_ptr<Node[]> nodes;
    std::atomic<std::size_t> terminal{0};
    std::promise<WorkflowResult> done;
    std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
};

namespace {

void transition(std::atomic<TaskState>& state, TaskState from, TaskState to) {
    if (!is_valid_transition(from, to)) {
        throw InvalidTransition(std::string(to_string(from)) + " -> " + std::string(to_string(to)));
    }
    TaskState expected = from;
    if (!state.compare_exchange_strong(expected, to, std::memory_order_acq_rel)) {
        throw InvalidTransition("expected " + std::string(to_string(from)) + " but task is " +
                                std::string(to_string(expected)));
    }
}

} // namespace

DagExecutor::DagExecutor(concurrency::ThreadPool& pool, TimerService& timers,
                         const HandlerRegistry& handlers, Options options)
    : pool_(pool), timers_(timers), handlers_(handlers),
      rng_state_(options.seed != 0 ? options.seed : std::random_device{}()) {}

double DagExecutor::next_random() {
    // splitmix64 over an atomic counter: cheap, thread-safe and good enough
    // for spreading out retries.
    std::uint64_t z = rng_state_.fetch_add(0x9e3779b97f4a7c15ULL, std::memory_order_relaxed);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
}

std::future<WorkflowResult> DagExecutor::start(std::shared_ptr<const Dag> dag,
                                               std::string workflow_id) {
    auto run = std::make_shared<Run>();
    run->dag = std::move(dag);
    run->workflow_id = std::move(workflow_id);
    const std::size_t n = run->dag->size();
    run->nodes = std::make_unique<Run::Node[]>(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        run->nodes[i].remaining_parents.store(static_cast<std::uint32_t>(run->dag->parents(i).size()),
                                              std::memory_order_relaxed);
    }
    auto future = run->done.get_future();
    // Copy the roots first: once the first root is scheduled the run may
    // complete (and be released) concurrently.
    const std::vector<Dag::Index> roots(run->dag->roots().begin(), run->dag->roots().end());
    for (Dag::Index r : roots) {
        schedule(run, r);
    }
    return future;
}

WorkflowResult DagExecutor::run(const Dag& dag, std::string workflow_id) {
    // Non-owning alias: the caller's Dag outlives this blocking call.
    std::shared_ptr<const Dag> alias(std::shared_ptr<const Dag>{}, &dag);
    return start(std::move(alias), std::move(workflow_id)).get();
}

void DagExecutor::schedule(const std::shared_ptr<Run>& run, std::uint32_t index) {
    pool_.submit([this, run, index] { execute(run, index); });
}

void DagExecutor::execute(const std::shared_ptr<Run>& run, std::uint32_t index) {
    Run::Node& node = run->nodes[index];
    const TaskSpec& spec = run->dag->task(index);

    const TaskState from = node.attempts == 0 ? TaskState::Pending : TaskState::Retrying;
    transition(node.state, from, TaskState::Running);
    node.attempts += 1;

    std::stop_source stop;
    struct Deadline {
        std::stop_source stop;
        std::atomic<bool> fired{false};
    };
    std::shared_ptr<Deadline> deadline;
    TimerService::TimerId timer = 0;
    if (spec.timeout.count() > 0) {
        deadline = std::make_shared<Deadline>();
        deadline->stop = stop;
        timer = timers_.schedule_after(spec.timeout, [deadline] {
            deadline->fired.store(true, std::memory_order_release);
            deadline->stop.request_stop();
        });
    }

    TaskContext ctx{.workflow_id = run->workflow_id,
                    .task_id = spec.id,
                    .payload = spec.payload,
                    .attempt = node.attempts,
                    .timeout = spec.timeout,
                    .stop = stop.get_token()};
    TaskResult result = handlers_.invoke(spec.handler, ctx);

    if (deadline) {
        timers_.cancel(timer);
        if (deadline->fired.load(std::memory_order_acquire)) {
            result = TaskResult::failure("attempt timed out after " +
                                         std::to_string(spec.timeout.count()) + " ms");
        }
    }

    if (result.ok) {
        node.output = std::move(result.output);
        node.error.clear();
        transition(node.state, TaskState::Running, TaskState::Succeeded);
        on_success(run, index);
        return;
    }

    node.error = std::move(result.error);
    if (result.retryable && spec.retry.can_retry(node.attempts)) {
        transition(node.state, TaskState::Running, TaskState::Retrying);
        const auto delay = spec.retry.backoff(node.attempts, next_random());
        if (delay.count() == 0) {
            schedule(run, index);
        } else {
            timers_.schedule_after(delay, [this, run, index] { schedule(run, index); });
        }
        return;
    }

    transition(node.state, TaskState::Running, TaskState::Failed);
    fail_descendants(run, index);
}

void DagExecutor::on_success(const std::shared_ptr<Run>& run, std::uint32_t index) {
    // Schedule children before publishing our own completion: once the
    // terminal counter reaches n the run may be torn down.
    for (Dag::Index child : run->dag->children(index)) {
        if (run->nodes[child].remaining_parents.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            schedule(run, child);
        }
    }
    mark_terminal(run, 1);
}

void DagExecutor::fail_descendants(const std::shared_ptr<Run>& run, std::uint32_t index) {
    std::size_t newly_failed = 1; // the task itself
    const std::string& failed_id = run->dag->task(index).id;
    std::vector<Dag::Index> stack(run->dag->children(index).begin(), run->dag->children(index).end());
    while (!stack.empty()) {
        const Dag::Index v = stack.back();
        stack.pop_back();
        TaskState expected = TaskState::Pending;
        // Descendants of a failed task can never have started, so they are
        // PENDING. Losing the CAS means another failing path already claimed
        // this subtree.
        if (!run->nodes[v].state.compare_exchange_strong(expected, TaskState::Failed,
                                                         std::memory_order_acq_rel)) {
            continue;
        }
        run->nodes[v].error = "upstream task '" + failed_id + "' failed";
        ++newly_failed;
        stack.insert(stack.end(), run->dag->children(v).begin(), run->dag->children(v).end());
    }
    mark_terminal(run, newly_failed);
}

void DagExecutor::mark_terminal(const std::shared_ptr<Run>& run, std::size_t count) {
    const std::size_t n = run->dag->size();
    if (run->terminal.fetch_add(count, std::memory_order_acq_rel) + count != n) {
        return;
    }
    WorkflowResult result;
    result.tasks.resize(n);
    for (std::size_t i = 0; i < n; ++i) {
        Run::Node& node = run->nodes[i];
        TaskOutcome& out = result.tasks[i];
        out.state = node.state.load(std::memory_order_acquire);
        out.attempts = node.attempts;
        out.output = std::move(node.output);
        out.error = std::move(node.error);
        (out.state == TaskState::Succeeded ? result.succeeded : result.failed) += 1;
    }
    result.elapsed = std::chrono::steady_clock::now() - run->started;
    run->done.set_value(std::move(result));
}

} // namespace flowd
