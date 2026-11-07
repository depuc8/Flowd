# Benchmarks

Every number below was measured with `flowd_bench` (Release build, GCC 16.2,
`-O3`) on 2026-11-04 on:

- Intel Core i5-12500H laptop (4 performance + 8 efficiency cores, 16
  hardware threads), 16 GB RAM, Fedora 44, kernel 7.2.13
- DynamoDB Local (latest `amazon/dynamodb-local` image) in Podman on the same
  machine

The machine is a laptop with heterogeneous cores and was not isolated
(background load average ~1-2), so expect run-to-run variation of roughly
+-10-30%, and more for the very short runs. Two full runs are compared where
they differ notably. Reproduce with:

```bash
cmake --preset release && cmake --build --preset release -j
./build/release/benchmarks/flowd_bench             # ~25 s
# with the DynamoDB section:
cmake --preset dynamodb && cmake --build --preset dynamodb -j
FLOWD_DYNAMODB_ENDPOINT=http://localhost:8000 ./build/dynamodb/benchmarks/flowd_bench
```

## 1. Queue performance

2,000,000 push+pop pairs, consumers spin on `try_pop`, median of 3 runs.

| producers x consumers | LockFreeQueue (M ops/s) | mutex + deque (M ops/s) | ratio |
|---|---:|---:|---:|
| 1 x 1 | 4.74 | 7.90 | 0.60x |
| 2 x 2 | 6.58 | 6.06 | 1.09x |
| 4 x 4 | 4.54 | 3.98 | 1.14x |
| 8 x 8 | 1.78 | 3.77 | 0.47x |

Interpretation. The lock-free queue is **not** uniformly faster. It wins
slightly at 2x2 and 4x4, and loses with a single producer/consumer pair and
when all 16 hardware threads spin on it. The reasons:

- one heap allocation per element (glibc malloc plus cross-thread frees),
  whereas `std::deque` allocates in blocks;
- two `seq_cst` hazard-pointer stores (`xchg` on x86) per pop;
- at 8x8, 16 spinning threads on 16 hardware threads (8 of them E-cores)
  bounce the head/tail cache lines constantly, while the mutex version's
  losers park in the kernel and stop generating traffic.

What the lock-free queue buys is the progress guarantee: a preempted or
descheduled thread can never block other submitters or workers (no
convoying, no priority inversion). A per-thread node pool would remove most
of the allocation cost and is the obvious next optimization.

## 2. Thread pool

1,000,000 empty jobs submitted by 4 threads; time from first submit until the
pool has drained (median of 3).

| pool threads | jobs/s (run 1) | jobs/s (run 2) |
|---:|---:|---:|
| 1 | 5.38 M | 5.29 M |
| 2 | 4.78 M | 4.63 M |
| 4 | 4.88 M | 4.83 M |
| 8 | 4.40 M | 3.21 M |
| 16 | 3.90 M | 3.91 M |

Empty jobs measure pure queue and dispatch overhead (~200-300 ns per job).
More threads do not help here because the shared head/tail lines are the
bottleneck. Real tasks are many orders of magnitude longer than the dispatch
cost.

## 3. Task scheduling throughput (in-process DAG engine)

No-op tasks on 16 threads; time from `start()` to the last task completing,
excluding DAG construction (median of 3). `layered` = width 100, each task
depends on 3 tasks of the previous layer; `chain` = a single path.

| shape | tasks | edges | time (ms) | tasks/s |
|---|---:|---:|---:|---:|
| independent | 1,000 | 0 | 1.1 | 0.95 M |
| independent | 10,000 | 0 | 7.8 | 1.29 M |
| independent | 100,000 | 0 | 78.4 | 1.28 M |
| layered | 1,000 | 2,700 | 0.5 | 1.94 M |
| layered | 10,000 | 29,700 | 4.7 | 2.13 M |
| layered | 100,000 | 299,700 | 52.1 | 1.92 M |
| chain | 1,000 | 999 | 1.0 | 1.03 M |
| chain | 10,000 | 9,999 | 9.7 | 1.03 M |
| chain | 100,000 | 99,999 | 94.7 | 1.06 M |

**Scaling with task count:** throughput is flat from 1k to 100k tasks for
every shape, so scheduling cost per task is constant (O(1) per task plus
O(1) per edge). A chain, which is fully sequential, still completes about
1 M dependent hops per second (~1 us per hop, including the wake-up of the
next worker). Layered graphs are faster than independent tasks because
children are scheduled from the worker threads themselves, spreading the
submissions over 16 producers.

## 4. Task execution throughput: concurrent blocking tasks

10,000 independent tasks that each sleep 1 ms (I/O-like). The 1-thread row
runs 1,000 tasks.

| pool threads | time (ms) | tasks/s | speed-up vs 1 thread |
|---:|---:|---:|---:|
| 1 | 1129 | 886 | 1.0x |
| 16 | 678 | 14,751 | 16.7x |
| 64 | 171 | 58,633 | 66.2x |
| 256 | 43 | 230,579 | 260x |
| 1,024 | 13 / 20 (two runs) | 780k / 500k | 565-874x |
| 4,096 | 62 / 33 (two runs) | 162k / 300k | 178-338x |

Up to 1,024 threads, i.e. 1,024 tasks blocked concurrently, scaling is
essentially linear. At 4,096 threads throughput drops below the 1,024 result:
thread creation, scheduler load and contention on the shared queue dominate
when 4,096 threads compete for 16 cores to run 10,000 tasks. **Measured
capacity on this machine: about 1,000 concurrently executing blocking tasks
per process before returns diminish.** Thousands of tasks can be queued or in
flight (the 100k-task DAGs above, 5,000 tasks across four workers in the
tests), but concurrency beyond about 1k blocking threads is better obtained
by adding worker processes or machines.

## 5. Concurrent worker performance (distributed protocol)

### Over the in-memory store

20,000 no-op tasks; each worker has 4 executor threads and at most 16
tasks in flight. This measures the coordination protocol itself (claim,
lease, fenced commit, propagation, mark propagated) without network latency.

| workload | workers | time (ms) | tasks/s | claim conflicts |
|---|---:|---:|---:|---:|
| independent | 1 | 188 | 106k | 0 |
| independent | 2 | 203 | 99k | 518 |
| independent | 4 | 217 | 92k | 1,898 |
| independent | 8 | 233 | 86k | 5,692 |
| layered | 1 | 324 | 62k | 0 |
| layered | 2 | 319 | 63k | 2,013 |
| layered | 4 | 353 | 57k | 5,953 |
| layered | 8 | 386 | 52k | 10,510 |

Adding workers does not increase throughput here. All operations of
`InMemoryTaskStore` serialize on one mutex, and no-op tasks make the
workload purely coordination-bound. Conflicts grow with the number of
workers because they race for the same ready tasks (they are rejected
cheaply, as designed). This is a property of the test store, not of the
protocol.

### Over DynamoDB Local

2,000 no-op tasks; each worker has 8 threads and at most 32 tasks in flight.

| workload | workers | submit (ms) | run (ms) | tasks/s | claim conflicts |
|---|---:|---:|---:|---:|---:|
| independent | 1 | 287 | 2,704 | 740 | 0 |
| independent | 2 | 308 | 2,490 | 803 | 51 |
| independent | 4 | 339 | 2,831 | 707 | 890 |
| layered | 1 | 305 | 5,165 | 387 | 0 |
| layered | 2 | 330 | 5,707 | 350 | 457 |
| layered | 4 | 356 | 6,370 | 314 | 2,765 |

DynamoDB Local is a single JVM backed by SQLite on the same laptop, so it is
the bottleneck at roughly 3,000-4,000 requests/s. A no-op task costs about 4
requests (claim, commit, mark propagated, plus the discovery query's share);
in the layered graph each edge adds 1-2 more, which is why it is about half
as fast. **These numbers do not predict throughput on AWS DynamoDB**, where
the table scales horizontally and the limits are per-partition throughput
(sharded index partitions spread that load) and request latency
(single-digit milliseconds). Measuring against real DynamoDB would require
creating billable resources and was not done.

## Summary

| Metric | Measured |
|---|---|
| In-process scheduling throughput | 0.95-2.1 M tasks/s, flat from 1k to 100k tasks |
| Dependent-hop latency (chain) | ~1 us |
| Thread pool dispatch | 3.2-5.4 M empty jobs/s |
| Concurrent blocking tasks per process | linear to ~1,024 threads; declines at 4,096 |
| Distributed protocol, in-memory store | 52k-106k tasks/s (store-mutex bound) |
| Distributed protocol, DynamoDB Local | 310-800 tasks/s (DynamoDB Local bound) |
| Lock-free vs mutex queue | 0.47x-1.14x depending on contention |
