#include "flowd/worker/worker.hpp"

#include "flowd/core/log.hpp"

#include <algorithm>
#include <array>
#include <format>
#include <random>
#include <vector>

#include <unistd.h>

namespace flowd {
namespace {

using SteadyClock = std::chrono::steady_clock;

std::uint64_t random_seed() {
    std::random_device rd;
    return (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
}

} // namespace

std::string default_worker_id() {
    std::array<char, 256> host{};
    if (::gethostname(host.data(), host.size() - 1) != 0) {
        std::ranges::copy(std::string_view("unknown"), host.begin());
    }
    std::mt19937 rng(static_cast<std::uint32_t>(random_seed()));
    return std::format("{}-{}-{:04x}", host.data(), ::getpid(), rng() & 0xffffU);
}

Worker::Worker(TaskStore& store, const HandlerRegistry& handlers, WorkerOptions options,
               Clock& clock)
    : store_(store), handlers_(handlers), options_(std::move(options)), clock_(clock),
      rng_(options_.seed != 0 ? options_.seed : random_seed()) {
    if (options_.worker_id.empty()) {
        options_.worker_id = default_worker_id();
    }
    options_.threads = std::max<std::size_t>(1, options_.threads);
    if (options_.max_in_flight == 0) {
        options_.max_in_flight = options_.threads;
    }
    options_.claim_batch = std::max<std::size_t>(1, options_.claim_batch);
    if (options_.heartbeat_interval >= options_.lease_duration) {
        options_.heartbeat_interval = options_.lease_duration / 3;
    }
}

Worker::~Worker() {
    if (started_.load() && !killed_.load()) {
        stop();
    }
}

void Worker::start() {
    if (started_.exchange(true)) {
        return;
    }
    pool_ = std::make_unique<concurrency::ThreadPool>(options_.threads);
    poller_ = std::jthread([this](std::stop_token s) { poll_loop(std::move(s)); });
    heartbeat_ = std::jthread([this](std::stop_token s) { heartbeat_loop(std::move(s)); });
    maintenance_ = std::jthread([this](std::stop_token s) { maintenance_loop(std::move(s)); });
    log::info("worker {} started: threads={} max_in_flight={} lease={}ms heartbeat={}ms",
              options_.worker_id, options_.threads, options_.max_in_flight,
              options_.lease_duration.count(), options_.heartbeat_interval.count());
}

void Worker::stop(std::chrono::milliseconds drain_timeout) {
    if (!started_.load() || killed_.load()) {
        return;
    }
    draining_.store(true);
    poller_.request_stop();
    if (poller_.joinable()) {
        poller_.join();
    }

    {
        std::unique_lock lock(mutex_);
        if (!cv_.wait_for(lock, drain_timeout, [this] { return in_flight_.empty(); })) {
            log::warn("worker {}: cancelling {} task(s) still running after {} ms drain timeout",
                      options_.worker_id, in_flight_.size(), drain_timeout.count());
            for (auto& [key, task] : in_flight_) {
                task->stop.request_stop();
            }
        }
        // Handlers are expected to honour cancellation; heartbeats keep the
        // leases alive until their results are committed.
        cv_.wait(lock, [this] { return in_flight_.empty(); });
    }
    pool_->shutdown();
    heartbeat_.request_stop();
    maintenance_.request_stop();
    if (heartbeat_.joinable()) {
        heartbeat_.join();
    }
    if (maintenance_.joinable()) {
        maintenance_.join();
    }
    log::info("worker {} stopped", options_.worker_id);
}

void Worker::kill() {
    if (!started_.load() || killed_.exchange(true)) {
        return;
    }
    poller_.request_stop();
    heartbeat_.request_stop();
    maintenance_.request_stop();
    {
        std::lock_guard lock(mutex_);
        for (auto& [key, task] : in_flight_) {
            task->stop.request_stop();
        }
    }
    for (std::jthread* t : {&poller_, &heartbeat_, &maintenance_}) {
        if (t->joinable()) {
            t->join();
        }
    }
    pool_->shutdown();
    log::warn("worker {} killed (simulated crash)", options_.worker_id);
}

Worker::Metrics Worker::metrics() const {
    return Metrics{.claimed = claimed_.load(),
                   .claim_conflicts = claim_conflicts_.load(),
                   .succeeded = succeeded_.load(),
                   .retried = retried_.load(),
                   .failed = failed_.load(),
                   .lease_renewals = lease_renewals_.load(),
                   .leases_lost = leases_lost_.load(),
                   .leases_reaped = leases_reaped_.load(),
                   .propagations_repaired = propagations_repaired_.load(),
                   .store_errors = store_errors_.load()};
}

std::size_t Worker::in_flight() const {
    std::lock_guard lock(mutex_);
    return in_flight_.size();
}

TimestampMs Worker::lease_deadline() const {
    return clock_.now_ms() + options_.lease_duration.count();
}

void Worker::count_store_error(std::string_view what, const std::exception& e) {
    store_errors_.fetch_add(1, std::memory_order_relaxed);
    log::warn("worker {}: {} failed: {}", options_.worker_id, what, e.what());
}

void Worker::interruptible_wait(std::stop_token& stop, std::chrono::milliseconds d) {
    std::mutex m;
    std::condition_variable_any cv;
    std::unique_lock lock(m);
    (void)cv.wait_for(lock, stop, d, [] { return false; });
}

// ---- claiming ---------------------------------------------------------------

void Worker::poll_loop(std::stop_token stop) {
    const std::uint32_t shards = std::max<std::uint32_t>(1, store_.shard_count());
    std::uint32_t next_shard = 0;
    {
        std::lock_guard lock(mutex_);
        next_shard = static_cast<std::uint32_t>(rng_() % shards);
    }
    auto backoff = options_.min_poll_interval;

    while (!stop.stop_requested()) {
        std::size_t capacity = 0;
        {
            std::unique_lock lock(mutex_);
            if (!cv_.wait(lock, stop,
                          [this] { return in_flight_.size() < options_.max_in_flight; })) {
                break;
            }
            capacity = options_.max_in_flight - in_flight_.size();
        }

        std::size_t claimed = 0;
        for (std::uint32_t i = 0; i < shards && claimed < capacity && !stop.stop_requested(); ++i) {
            claimed += claim_from((next_shard + i) % shards, capacity - claimed);
        }
        next_shard = (next_shard + 1) % shards;

        if (claimed == 0) {
            interruptible_wait(stop, backoff);
            backoff = std::min(backoff * 2, options_.max_poll_interval);
        } else {
            backoff = options_.min_poll_interval;
        }
    }
}

std::size_t Worker::claim_from(std::uint32_t shard, std::size_t capacity) {
    std::size_t claimed = 0;
    try {
        std::vector<TaskKey> candidates =
            store_.find_ready(shard, clock_.now_ms(), std::min(capacity, options_.claim_batch));
        // Workers polling the same shard would otherwise all race for the
        // oldest task first.
        {
            std::lock_guard lock(mutex_);
            std::ranges::shuffle(candidates, rng_);
        }
        for (const TaskKey& key : candidates) {
            if (!should_write() || claimed >= capacity) {
                break;
            }
            std::optional<TaskRecord> record =
                store_.try_claim(key, options_.worker_id, clock_.now_ms(), lease_deadline());
            if (!record) {
                claim_conflicts_.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            claimed_.fetch_add(1, std::memory_order_relaxed);
            auto task = std::make_shared<InFlight>();
            const auto now = SteadyClock::now();
            if (record->timeout.count() > 0) {
                task->deadline = now + record->timeout;
            }
            task->next_renewal = now + options_.heartbeat_interval;
            task->record = std::move(*record);
            log::debug("worker {} claimed {}/{} attempt {}", options_.worker_id, key.workflow_id,
                       key.task_id, task->record.attempt);
            {
                std::lock_guard lock(mutex_);
                in_flight_.emplace(key, task);
            }
            pool_->submit([this, task] { execute(task); });
            ++claimed;
        }
    } catch (const std::exception& e) {
        count_store_error("claim", e);
    }
    return claimed;
}

// ---- execution ----------------------------------------------------------------

FailureTransition Worker::failure_transition(const TaskRecord& record, bool retryable,
                                             std::string error) {
    const TimestampMs now = clock_.now_ms();
    if (retryable && record.retry.can_retry(record.attempt)) {
        double r = 0.0;
        {
            std::lock_guard lock(mutex_);
            r = std::uniform_real_distribution<double>(0.0, 1.0)(rng_);
        }
        const auto delay = record.retry.backoff(record.attempt, r);
        return FailureTransition{.next = TaskState::Retrying,
                                 .available_at = now + delay.count(),
                                 .error = std::move(error),
                                 .now = now};
    }
    return FailureTransition{.next = TaskState::Failed, .error = std::move(error), .now = now};
}

void Worker::execute(std::shared_ptr<InFlight> task) {
    const TaskRecord& r = task->record;
    const TaskKey key = r.key();

    TaskContext ctx{.workflow_id = r.workflow_id,
                    .task_id = r.task_id,
                    .payload = r.payload,
                    .attempt = r.attempt,
                    .timeout = r.timeout,
                    .stop = task->stop.get_token()};
    TaskResult result = handlers_.invoke(r.handler, ctx);

    auto release = [&] {
        {
            std::lock_guard lock(mutex_);
            in_flight_.erase(key);
        }
        cv_.notify_all();
    };

    if (!should_write()) {
        release(); // crashed: the result is lost, the lease will expire
        return;
    }
    if (task->lease_lost.load(std::memory_order_acquire)) {
        leases_lost_.fetch_add(1, std::memory_order_relaxed);
        log::warn("worker {}: discarding result of {}/{} attempt {}: lease lost",
                  options_.worker_id, key.workflow_id, key.task_id, r.attempt);
        release();
        return;
    }
    if (task->timed_out.load(std::memory_order_acquire)) {
        result = TaskResult::failure(std::format("attempt timed out after {} ms", r.timeout.count()));
    }

    try {
        if (result.ok) {
            if (store_.complete(key, options_.worker_id, r.attempt, result.output,
                                clock_.now_ms())) {
                succeeded_.fetch_add(1, std::memory_order_relaxed);
                TaskRecord done = r;
                done.state = TaskState::Succeeded;
                release();
                propagate_success(done);
                return;
            }
            leases_lost_.fetch_add(1, std::memory_order_relaxed);
            log::warn("worker {}: completion of {}/{} attempt {} rejected (lease taken over)",
                      options_.worker_id, key.workflow_id, key.task_id, r.attempt);
        } else {
            FailureTransition t = failure_transition(r, result.retryable, result.error);
            const TaskState next = t.next;
            if (store_.fail(key, options_.worker_id, r.attempt, t)) {
                if (next == TaskState::Retrying) {
                    retried_.fetch_add(1, std::memory_order_relaxed);
                    log::info("task {}/{} attempt {} failed, retrying: {}", key.workflow_id,
                              key.task_id, r.attempt, result.error);
                } else {
                    failed_.fetch_add(1, std::memory_order_relaxed);
                    log::warn("task {}/{} failed permanently after {} attempt(s): {}",
                              key.workflow_id, key.task_id, r.attempt, result.error);
                    TaskRecord done = r;
                    done.state = TaskState::Failed;
                    release();
                    propagate_failure(done);
                    return;
                }
            } else {
                leases_lost_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    } catch (const std::exception& e) {
        // Nothing was committed (or we cannot tell). The lease will expire
        // and the reaper hands the task to another attempt.
        count_store_error("commit", e);
    }
    release();
}

// ---- dependency propagation ---------------------------------------------------

void Worker::propagate(const TaskRecord& record) {
    if (record.state == TaskState::Succeeded) {
        propagate_success(record);
    } else if (record.state == TaskState::Failed) {
        propagate_failure(record);
    }
}

void Worker::propagate_success(const TaskRecord& record) {
    if (!should_write()) {
        return;
    }
    try {
        for (const std::string& child : record.children) {
            store_.resolve_dependency(TaskKey{record.workflow_id, child}, record.task_id,
                                      clock_.now_ms());
        }
        store_.mark_propagated(record.key());
    } catch (const std::exception& e) {
        // needs_propagation stays set; the repairer finishes the job.
        count_store_error("propagation", e);
    }
}

void Worker::propagate_failure(const TaskRecord& record) {
    if (!should_write()) {
        return;
    }
    try {
        std::vector<TaskRecord> stack{record};
        while (!stack.empty() && should_write()) {
            TaskRecord current = std::move(stack.back());
            stack.pop_back();
            const std::string reason = std::format("upstream task '{}' failed", current.task_id);
            for (const std::string& child : current.children) {
                if (auto failed = store_.fail_upstream(TaskKey{current.workflow_id, child}, reason,
                                                       clock_.now_ms())) {
                    stack.push_back(std::move(*failed));
                }
            }
            store_.mark_propagated(current.key());
        }
    } catch (const std::exception& e) {
        count_store_error("failure propagation", e);
    }
}

// ---- heartbeats and timeouts --------------------------------------------------

void Worker::heartbeat_loop(std::stop_token stop) {
    const auto tick = std::clamp(options_.heartbeat_interval / 4, std::chrono::milliseconds{5},
                                 std::chrono::milliseconds{250});
    while (!stop.stop_requested()) {
        interruptible_wait(stop, tick);
        if (stop.stop_requested() || !should_write()) {
            break;
        }
        std::vector<std::shared_ptr<InFlight>> snapshot;
        {
            std::lock_guard lock(mutex_);
            snapshot.reserve(in_flight_.size());
            for (auto& [key, task] : in_flight_) {
                snapshot.push_back(task);
            }
        }
        const auto now = SteadyClock::now();
        for (auto& task : snapshot) {
            if (task->lease_lost.load(std::memory_order_relaxed)) {
                continue;
            }
            if (task->deadline != SteadyClock::time_point{} && now >= task->deadline &&
                !task->timed_out.exchange(true)) {
                log::info("task {}/{} attempt {} hit its {} ms timeout", task->record.workflow_id,
                          task->record.task_id, task->record.attempt,
                          task->record.timeout.count());
                task->stop.request_stop();
            }
            if (now < task->next_renewal) {
                continue;
            }
            try {
                if (store_.renew_lease(task->record.key(), options_.worker_id, task->record.attempt,
                                       lease_deadline())) {
                    lease_renewals_.fetch_add(1, std::memory_order_relaxed);
                    task->next_renewal = now + options_.heartbeat_interval;
                } else {
                    // Rejected: either the task already finished (benign; the
                    // executor is about to drop it) or another worker reaped
                    // the lease. Cancel the attempt; if it already committed,
                    // the flag is ignored.
                    task->lease_lost.store(true, std::memory_order_release);
                    task->stop.request_stop();
                }
            } catch (const std::exception& e) {
                count_store_error("lease renewal", e);
                task->next_renewal = now + tick;
            }
        }
    }
}

// ---- crash recovery -----------------------------------------------------------

void Worker::maintenance_loop(std::stop_token stop) {
    while (!stop.stop_requested()) {
        interruptible_wait(stop, options_.maintenance_interval);
        if (stop.stop_requested() || !should_write()) {
            break;
        }
        reap_expired_leases();
        repair_propagation();
    }
}

void Worker::reap_expired_leases() {
    for (std::uint32_t shard = 0; shard < store_.shard_count(); ++shard) {
        try {
            for (TaskRecord& rec : store_.find_expired_leases(shard, clock_.now_ms(), 100)) {
                if (!should_write()) {
                    return;
                }
                FailureTransition t = failure_transition(
                    rec, /*retryable=*/true,
                    std::format("lease of worker '{}' expired (attempt {})", rec.lease_owner,
                                rec.attempt));
                const TaskState next = t.next;
                if (!store_.expire_lease(rec.key(), rec.attempt, t)) {
                    continue; // renewed, completed or reaped by someone else
                }
                leases_reaped_.fetch_add(1, std::memory_order_relaxed);
                log::warn("worker {} reaped expired lease of {}/{} (owner {}, attempt {}) -> {}",
                          options_.worker_id, rec.workflow_id, rec.task_id, rec.lease_owner,
                          rec.attempt, to_string(next));
                if (next == TaskState::Failed) {
                    failed_.fetch_add(1, std::memory_order_relaxed);
                    rec.state = TaskState::Failed;
                    propagate_failure(rec);
                }
            }
        } catch (const std::exception& e) {
            count_store_error("lease reaping", e);
        }
    }
}

void Worker::repair_propagation() {
    const TimestampMs cutoff = clock_.now_ms() - options_.propagation_grace.count();
    for (std::uint32_t shard = 0; shard < store_.shard_count(); ++shard) {
        try {
            for (const TaskRecord& rec : store_.find_unpropagated(shard, cutoff, 100)) {
                if (!should_write()) {
                    return;
                }
                log::info("worker {} repairing propagation of {}/{} ({})", options_.worker_id,
                          rec.workflow_id, rec.task_id, to_string(rec.state));
                propagate(rec);
                propagations_repaired_.fetch_add(1, std::memory_order_relaxed);
            }
        } catch (const std::exception& e) {
            count_store_error("propagation repair", e);
        }
    }
}

} // namespace flowd
