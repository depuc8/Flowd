#include "flowd/store/in_memory_task_store.hpp"

#include "flowd/core/error.hpp"

#include <algorithm>

namespace flowd {

InMemoryTaskStore::InMemoryTaskStore(std::uint32_t shard_count)
    : shard_count_(std::max<std::uint32_t>(1, shard_count)), ready_(shard_count_),
      leases_(shard_count_), propagate_(shard_count_) {}

InMemoryTaskStore::Entry* InMemoryTaskStore::find_locked(const TaskKey& key) {
    auto it = tasks_.find(key);
    return it == tasks_.end() ? nullptr : &it->second;
}

std::set<InMemoryTaskStore::IndexKey>& InMemoryTaskStore::index_set(Index index,
                                                                    std::uint32_t shard) {
    switch (index) {
    case Index::Ready:
        return ready_[shard];
    case Index::Lease:
        return leases_[shard];
    case Index::Propagate:
    case Index::None:
        break;
    }
    return propagate_[shard];
}

void InMemoryTaskStore::set_index_locked(Entry& e, Index index, TimestampMs time) {
    const std::uint32_t shard = e.record.shard % shard_count_;
    if (e.index != Index::None) {
        index_set(e.index, shard).erase(IndexKey{e.index_time, e.record.key()});
    }
    e.index = index;
    e.index_time = time;
    if (index != Index::None) {
        index_set(index, shard).insert(IndexKey{time, e.record.key()});
    }
}

bool InMemoryTaskStore::reject_locked() {
    ++rejected_;
    return false;
}

std::uint64_t InMemoryTaskStore::rejected_transitions() const {
    std::lock_guard lock(mutex_);
    return rejected_;
}

void InMemoryTaskStore::create_workflow(const WorkflowRecord& workflow,
                                        const std::vector<TaskRecord>& tasks) {
    std::lock_guard lock(mutex_);
    if (workflows_.contains(workflow.workflow_id)) {
        throw AlreadyExists("workflow '" + workflow.workflow_id + "' already exists");
    }
    workflows_.emplace(workflow.workflow_id, workflow);
    for (const TaskRecord& t : tasks) {
        Entry& e = tasks_[t.key()];
        e.record = t;
        e.record.shard %= shard_count_;
        if (t.state == TaskState::Pending && t.pending_deps.empty()) {
            set_index_locked(e, Index::Ready, t.available_at);
        }
    }
}

std::optional<WorkflowRecord> InMemoryTaskStore::get_workflow(std::string_view id) {
    std::lock_guard lock(mutex_);
    auto it = workflows_.find(id);
    if (it == workflows_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<TaskRecord> InMemoryTaskStore::list_tasks(std::string_view workflow_id) {
    std::lock_guard lock(mutex_);
    std::vector<TaskRecord> out;
    for (auto it = tasks_.lower_bound(TaskKey{std::string(workflow_id), ""});
         it != tasks_.end() && it->first.workflow_id == workflow_id; ++it) {
        out.push_back(it->second.record);
    }
    return out;
}

std::optional<TaskRecord> InMemoryTaskStore::get_task(const TaskKey& key) {
    std::lock_guard lock(mutex_);
    if (Entry* e = find_locked(key)) {
        return e->record;
    }
    return std::nullopt;
}

std::vector<TaskKey> InMemoryTaskStore::find_ready(std::uint32_t shard, TimestampMs now,
                                                   std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::vector<TaskKey> out;
    for (const auto& [time, key] : ready_.at(shard % shard_count_)) {
        if (time > now || out.size() >= limit) {
            break;
        }
        out.push_back(key);
    }
    return out;
}

std::vector<TaskRecord> InMemoryTaskStore::find_expired_leases(std::uint32_t shard,
                                                               TimestampMs now,
                                                               std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::vector<TaskRecord> out;
    for (const auto& [time, key] : leases_.at(shard % shard_count_)) {
        if (time >= now || out.size() >= limit) {
            break;
        }
        out.push_back(tasks_.at(key).record);
    }
    return out;
}

std::vector<TaskRecord> InMemoryTaskStore::find_unpropagated(std::uint32_t shard,
                                                             TimestampMs finished_before,
                                                             std::size_t limit) {
    std::lock_guard lock(mutex_);
    std::vector<TaskRecord> out;
    for (const auto& [time, key] : propagate_.at(shard % shard_count_)) {
        if (time >= finished_before || out.size() >= limit) {
            break;
        }
        out.push_back(tasks_.at(key).record);
    }
    return out;
}

std::optional<TaskRecord> InMemoryTaskStore::try_claim(const TaskKey& key,
                                                       std::string_view worker_id, TimestampMs now,
                                                       TimestampMs lease_expires_at) {
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(key);
    if (e == nullptr || e->index != Index::Ready || e->record.available_at > now) {
        reject_locked();
        return std::nullopt;
    }
    TaskRecord& r = e->record;
    // The ready index only ever holds PENDING (no pending deps) or RETRYING.
    if (!is_valid_transition(r.state, TaskState::Running) || !r.pending_deps.empty()) {
        throw StoreError("corrupt ready index entry for " + key.workflow_id + "/" + key.task_id);
    }
    r.state = TaskState::Running;
    r.attempt += 1;
    r.lease_owner = std::string(worker_id);
    r.lease_expires_at = lease_expires_at;
    r.updated_at = now;
    set_index_locked(*e, Index::Lease, lease_expires_at);
    return r;
}

bool InMemoryTaskStore::renew_lease(const TaskKey& key, std::string_view worker_id,
                                    std::uint32_t attempt, TimestampMs lease_expires_at) {
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(key);
    if (e == nullptr || e->record.state != TaskState::Running || e->record.attempt != attempt ||
        e->record.lease_owner != worker_id) {
        return reject_locked();
    }
    e->record.lease_expires_at = lease_expires_at;
    set_index_locked(*e, Index::Lease, lease_expires_at);
    return true;
}

bool InMemoryTaskStore::complete(const TaskKey& key, std::string_view worker_id,
                                 std::uint32_t attempt, std::string_view output, TimestampMs now) {
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(key);
    if (e == nullptr || e->record.state != TaskState::Running || e->record.attempt != attempt ||
        e->record.lease_owner != worker_id) {
        return reject_locked();
    }
    TaskRecord& r = e->record;
    r.state = TaskState::Succeeded;
    r.output = std::string(output);
    r.last_error.clear();
    r.lease_owner.clear();
    r.lease_expires_at = 0;
    r.needs_propagation = true;
    r.finished_at = now;
    r.updated_at = now;
    set_index_locked(*e, Index::Propagate, now);
    return true;
}

void InMemoryTaskStore::apply_failure_locked(Entry& e, const FailureTransition& t) {
    TaskRecord& r = e.record;
    r.state = t.next;
    r.last_error = t.error;
    r.lease_owner.clear();
    r.lease_expires_at = 0;
    r.updated_at = t.now;
    if (t.next == TaskState::Retrying) {
        r.available_at = t.available_at;
        set_index_locked(e, Index::Ready, t.available_at);
    } else {
        r.needs_propagation = true;
        r.finished_at = t.now;
        set_index_locked(e, Index::Propagate, t.now);
    }
}

bool InMemoryTaskStore::fail(const TaskKey& key, std::string_view worker_id, std::uint32_t attempt,
                             const FailureTransition& transition) {
    if (transition.next != TaskState::Retrying && transition.next != TaskState::Failed) {
        throw InvalidTransition("fail() must move to RETRYING or FAILED");
    }
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(key);
    if (e == nullptr || e->record.state != TaskState::Running || e->record.attempt != attempt ||
        e->record.lease_owner != worker_id) {
        return reject_locked();
    }
    apply_failure_locked(*e, transition);
    return true;
}

bool InMemoryTaskStore::expire_lease(const TaskKey& key, std::uint32_t attempt,
                                     const FailureTransition& transition) {
    if (transition.next != TaskState::Retrying && transition.next != TaskState::Failed) {
        throw InvalidTransition("expire_lease() must move to RETRYING or FAILED");
    }
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(key);
    if (e == nullptr || e->record.state != TaskState::Running || e->record.attempt != attempt ||
        e->record.lease_expires_at >= transition.now) {
        return reject_locked();
    }
    apply_failure_locked(*e, transition);
    return true;
}

bool InMemoryTaskStore::resolve_dependency(const TaskKey& child, std::string_view parent_id,
                                           TimestampMs now) {
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(child);
    if (e == nullptr) {
        return false;
    }
    TaskRecord& r = e->record;
    std::erase(r.pending_deps, parent_id);
    if (r.pending_deps.empty() && r.state == TaskState::Pending && e->index == Index::None) {
        r.available_at = now;
        r.updated_at = now;
        set_index_locked(*e, Index::Ready, now);
        return true;
    }
    return false;
}

std::optional<TaskRecord> InMemoryTaskStore::fail_upstream(const TaskKey& child,
                                                           std::string_view error,
                                                           TimestampMs now) {
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(child);
    if (e == nullptr || e->record.state != TaskState::Pending) {
        reject_locked();
        return std::nullopt;
    }
    TaskRecord& r = e->record;
    r.state = TaskState::Failed;
    r.last_error = std::string(error);
    r.needs_propagation = true;
    r.finished_at = now;
    r.updated_at = now;
    set_index_locked(*e, Index::Propagate, now);
    return r;
}

void InMemoryTaskStore::mark_propagated(const TaskKey& key) {
    std::lock_guard lock(mutex_);
    Entry* e = find_locked(key);
    if (e == nullptr || !e->record.needs_propagation) {
        return;
    }
    e->record.needs_propagation = false;
    set_index_locked(*e, Index::None, 0);
}

} // namespace flowd
