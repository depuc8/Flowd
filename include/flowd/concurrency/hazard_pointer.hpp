#pragma once

#include <atomic>
#include <cstddef>
#include <vector>

namespace flowd::concurrency {

// Hazard pointers (M. Michael, "Hazard Pointers: Safe Memory Reclamation for
// Lock-Free Objects", IEEE TPDS 2004).
//
// A thread that is about to dereference a node of a lock-free structure
// publishes the node's address in one of its hazard slots and then re-checks
// that the node is still reachable. A thread that unlinks a node retires it
// instead of deleting it; retired nodes are only freed once no hazard slot
// holds their address. This prevents both use-after-free and the ABA problem
// for the protected pointers.
//
// Progress: protect(), retire() and record acquisition are lock-free. The
// periodic scan is wait-free with respect to other threads (it only reads
// their slots) and bounded by O(R log H) for R retired and H hazard pointers.
//
// Memory ordering: hazard publication, the re-validation load, the unlinking
// CAS in the data structure and the scan's hazard loads are all seq_cst. That
// gives the store->load ordering the algorithm requires on both sides; a
// cheaper asymmetric-fence scheme is possible but harder to verify with TSan.
class HazardDomain {
public:
    static constexpr std::size_t kSlotsPerThread = 2;

    struct Record {
        std::atomic<const void*> slots[kSlotsPerThread] = {};
        std::atomic<bool> active{false};
        Record* next = nullptr; // immutable once the record is published
    };

    using Deleter = void (*)(void*);

    // Process-wide domain shared by all lock-free structures. It is the only
    // instance: per-thread state is bound to it for the thread's lifetime.
    static HazardDomain& global();

    HazardDomain(const HazardDomain&) = delete;
    HazardDomain& operator=(const HazardDomain&) = delete;

    // Hazard slot `slot` of the calling thread.
    std::atomic<const void*>& slot(std::size_t slot);

    // Defers `deleter(ptr)` until no hazard pointer references `ptr`. `ptr`
    // must already be unreachable for threads that have not protected it.
    void retire(void* ptr, Deleter deleter);

    // Frees every retired node of the calling thread that is not protected.
    // Exposed for tests; retire() calls it automatically.
    void scan();

    // Number of nodes retired but not yet freed (approximate, for tests).
    [[nodiscard]] std::size_t pending_reclamation() const noexcept {
        return pending_.load(std::memory_order_relaxed);
    }

private:
    HazardDomain() = default;
    ~HazardDomain() = default;

    struct Retired {
        void* ptr;
        Deleter deleter;
    };
    struct OrphanBatch {
        std::vector<Retired> nodes;
        OrphanBatch* next = nullptr;
    };
    struct ThreadState;

    Record* acquire_record();
    void release_thread(ThreadState& state);
    ThreadState& local();
    void scan(std::vector<Retired>& retired);

    std::atomic<Record*> records_{nullptr};
    std::atomic<std::size_t> record_count_{0};
    std::atomic<OrphanBatch*> orphans_{nullptr};
    std::atomic<std::size_t> pending_{0};
};

// RAII owner of one hazard slot of the calling thread.
class HazardGuard {
public:
    explicit HazardGuard(std::size_t slot) : slot_(&HazardDomain::global().slot(slot)) {}
    HazardGuard(const HazardGuard&) = delete;
    HazardGuard& operator=(const HazardGuard&) = delete;
    ~HazardGuard() { reset(); }

    // Loads `src` and publishes it until the published value is confirmed to
    // still be current. The returned node is safe to dereference until the
    // guard is reset or re-pointed.
    template <typename T>
    T* protect(const std::atomic<T*>& src) noexcept {
        T* p = src.load(std::memory_order_relaxed);
        for (;;) {
            slot_->store(p, std::memory_order_seq_cst);
            T* q = src.load(std::memory_order_seq_cst);
            if (q == p) {
                return p;
            }
            p = q;
        }
    }

    // Publishes `p` without validation; the caller must validate reachability.
    void set(const void* p) noexcept { slot_->store(p, std::memory_order_seq_cst); }

    void reset() noexcept { slot_->store(nullptr, std::memory_order_release); }

private:
    std::atomic<const void*>* slot_;
};

} // namespace flowd::concurrency
