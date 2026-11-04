#pragma once

#include "flowd/concurrency/hazard_pointer.hpp"

#include <atomic>
#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace flowd::concurrency {

// Unbounded multi-producer / multi-consumer FIFO queue.
//
// Algorithm: Michael & Scott, "Simple, Fast, and Practical Non-Blocking and
// Blocking Concurrent Queue Algorithms" (PODC 1996), with hazard pointers for
// memory reclamation instead of tagged pointers.
//
// Guarantees
//  * Lock-free: push() and try_pop() never block. A thread that is suspended
//    at any point cannot prevent other threads from completing their
//    operations; whenever a CAS fails, some other thread's operation has made
//    progress (the lagging tail is helped forward by whoever observes it).
//    Node allocation goes through the global allocator, so the end-to-end
//    guarantee is lock-free modulo `operator new`.
//  * Linearizable: push() linearizes at the CAS that links the new node to
//    the last node, try_pop() at the CAS that advances head (or, when it
//    returns empty, at the load of head->next that observed nullptr).
//  * FIFO per producer: values pushed by one thread are popped in push order.
//  * Each pushed value is popped exactly once.
//  * No ABA and no use-after-free: a node is only freed once no thread holds
//    a hazard pointer to it.
//  * Memory ordering: everything a producer wrote before push() happens-before
//    everything the consumer that pops that value does afterwards.
//
// The destructor is not thread-safe: no other thread may access the queue
// while it is being destroyed. Remaining values are destroyed with it.
template <typename T>
class LockFreeQueue {
public:
    LockFreeQueue() {
        Node* dummy = new Node();
        head_.store(dummy, std::memory_order_relaxed);
        tail_.store(dummy, std::memory_order_relaxed);
    }

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    ~LockFreeQueue() {
        Node* n = head_.load(std::memory_order_relaxed);
        while (n != nullptr) {
            Node* next = n->next.load(std::memory_order_relaxed);
            delete n;
            n = next;
        }
    }

    void push(T value) {
        Node* node = new Node(std::move(value));
        HazardGuard tail_guard(0);
        for (;;) {
            Node* tail = tail_guard.protect(tail_);
            Node* next = tail->next.load(std::memory_order_acquire);
            if (tail != tail_.load(std::memory_order_acquire)) {
                continue;
            }
            if (next != nullptr) {
                // Tail is lagging behind; help the other producer finish.
                tail_.compare_exchange_weak(tail, next, std::memory_order_release,
                                            std::memory_order_relaxed);
                continue;
            }
            Node* expected = nullptr;
            if (tail->next.compare_exchange_weak(expected, node, std::memory_order_release,
                                                 std::memory_order_relaxed)) {
                // Linearization point passed. Failure here only means another
                // thread already advanced the tail for us.
                tail_.compare_exchange_strong(tail, node, std::memory_order_release,
                                              std::memory_order_relaxed);
                return;
            }
        }
    }

    [[nodiscard]] std::optional<T> try_pop() {
        HazardGuard head_guard(0);
        HazardGuard next_guard(1);
        for (;;) {
            Node* head = head_guard.protect(head_);
            Node* tail = tail_.load(std::memory_order_acquire);
            Node* next = head->next.load(std::memory_order_acquire);
            next_guard.set(next);
            // Re-validating head after publishing `next` guarantees `next` was
            // not retired yet: it can only be retired after head moves past it.
            if (head != head_.load(std::memory_order_seq_cst)) {
                continue;
            }
            if (next == nullptr) {
                return std::nullopt;
            }
            if (head == tail) {
                // A push linked `next` but has not swung the tail yet.
                tail_.compare_exchange_weak(tail, next, std::memory_order_release,
                                            std::memory_order_relaxed);
                continue;
            }
            if (head_.compare_exchange_strong(head, next, std::memory_order_seq_cst,
                                              std::memory_order_relaxed)) {
                // `next` is the new dummy. Only the thread that won the CAS
                // ever touches its value, and the hazard pointer keeps the
                // node alive while we move it out.
                // Every node except the dummy carries a value.
                std::optional<T> out(std::in_place,
                                     std::move(*next->value)); // NOLINT(bugprone-unchecked-optional-access)
                next->value.reset();
                head_guard.reset();
                next_guard.reset();
                HazardDomain::global().retire(head, &Node::destroy);
                return out;
            }
        }
    }

    // Snapshot that may be stale by the time it is used.
    [[nodiscard]] bool empty_approx() const {
        HazardGuard head_guard(0);
        Node* head = head_guard.protect(head_);
        return head->next.load(std::memory_order_acquire) == nullptr;
    }

private:
    struct Node {
        Node() = default;
        explicit Node(T&& v) : value(std::move(v)) {}
        static void destroy(void* p) { delete static_cast<Node*>(p); }

        std::atomic<Node*> next{nullptr};
        std::optional<T> value;
    };

    // head_ and tail_ live on separate cache lines so producers and consumers
    // do not invalidate each other's line on every operation.
    alignas(64) std::atomic<Node*> head_;
    alignas(64) std::atomic<Node*> tail_;
};

} // namespace flowd::concurrency
