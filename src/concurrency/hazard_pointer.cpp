#include "flowd/concurrency/hazard_pointer.hpp"

#include <algorithm>
#include <stdexcept>

namespace flowd::concurrency {

// Per-thread view of the domain: the acquired record plus the list of nodes
// this thread retired. Destroyed at thread exit, which hands the record back
// for reuse and passes still-protected nodes to the domain as orphans.
struct HazardDomain::ThreadState {
    HazardDomain* domain = nullptr;
    Record* record = nullptr;
    std::vector<Retired> retired;

    ~ThreadState() {
        if (domain != nullptr) {
            domain->release_thread(*this);
        }
    }
};

HazardDomain& HazardDomain::global() {
    // Never destroyed: lock-free structures with static storage duration may
    // retire nodes during their own destruction at exit, and thread_local
    // ThreadState destructors of late-exiting threads still reference the
    // domain. The OS reclaims the memory on process exit.
    static auto* domain = new HazardDomain();
    return *domain;
}

HazardDomain::ThreadState& HazardDomain::local() {
    thread_local ThreadState state;
    if (state.domain == nullptr) {
        state.domain = this;
        state.record = acquire_record();
    }
    return state;
}

HazardDomain::Record* HazardDomain::acquire_record() {
    // Reuse a record released by an exited thread if possible.
    for (Record* r = records_.load(std::memory_order_acquire); r != nullptr; r = r->next) {
        bool expected = false;
        if (!r->active.load(std::memory_order_relaxed) &&
            r->active.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return r;
        }
    }
    auto* r = new Record();
    r->active.store(true, std::memory_order_relaxed);
    Record* head = records_.load(std::memory_order_relaxed);
    do {
        r->next = head;
    } while (!records_.compare_exchange_weak(head, r, std::memory_order_acq_rel,
                                             std::memory_order_relaxed));
    record_count_.fetch_add(1, std::memory_order_relaxed);
    return r;
}

std::atomic<const void*>& HazardDomain::slot(std::size_t slot) {
    if (slot >= kSlotsPerThread) {
        throw std::out_of_range("hazard slot index out of range");
    }
    return local().record->slots[slot];
}

void HazardDomain::retire(void* ptr, Deleter deleter) {
    ThreadState& state = local();
    state.retired.push_back(Retired{ptr, deleter});
    pending_.fetch_add(1, std::memory_order_relaxed);
    const std::size_t threshold =
        std::max<std::size_t>(64, 2 * kSlotsPerThread * record_count_.load(std::memory_order_relaxed));
    if (state.retired.size() >= threshold) {
        scan(state.retired);
    }
}

void HazardDomain::scan() {
    scan(local().retired);
}

void HazardDomain::scan(std::vector<Retired>& retired) {
    // Adopt nodes left behind by exited threads so they are not leaked.
    if (orphans_.load(std::memory_order_relaxed) != nullptr) {
        for (OrphanBatch* b = orphans_.exchange(nullptr, std::memory_order_acq_rel); b != nullptr;) {
            retired.insert(retired.end(), b->nodes.begin(), b->nodes.end());
            OrphanBatch* next = b->next;
            delete b;
            b = next;
        }
    }
    if (retired.empty()) {
        return;
    }

    std::vector<const void*> hazards;
    hazards.reserve(record_count_.load(std::memory_order_relaxed) * kSlotsPerThread);
    for (Record* r = records_.load(std::memory_order_acquire); r != nullptr; r = r->next) {
        for (auto& s : r->slots) {
            if (const void* p = s.load(std::memory_order_seq_cst); p != nullptr) {
                hazards.push_back(p);
            }
        }
    }
    std::ranges::sort(hazards);

    // Protected nodes stay at the front; the rest is detached before any
    // deleter runs so that a deleter which itself retires nodes cannot
    // invalidate the vector being processed.
    auto unprotected = std::ranges::partition(retired, [&](const Retired& r) {
        return std::ranges::binary_search(hazards, static_cast<const void*>(r.ptr));
    });
    std::vector<Retired> to_free(unprotected.begin(), unprotected.end());
    retired.erase(unprotected.begin(), unprotected.end());
    for (const Retired& r : to_free) {
        r.deleter(r.ptr);
    }
    pending_.fetch_sub(to_free.size(), std::memory_order_relaxed);
}

void HazardDomain::release_thread(ThreadState& state) {
    for (auto& s : state.record->slots) {
        s.store(nullptr, std::memory_order_release);
    }
    scan(state.retired);
    if (!state.retired.empty()) {
        auto* batch = new OrphanBatch{std::move(state.retired), nullptr};
        OrphanBatch* head = orphans_.load(std::memory_order_relaxed);
        do {
            batch->next = head;
        } while (!orphans_.compare_exchange_weak(head, batch, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed));
    }
    state.record->active.store(false, std::memory_order_release);
    state.record = nullptr;
    state.domain = nullptr;
}

} // namespace flowd::concurrency
