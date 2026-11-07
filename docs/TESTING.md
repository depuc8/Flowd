# Testing

All numbers and results below were produced by running the commands shown on
the development machine (Fedora 44, kernel 7.2, Intel i5-12500H, 16 hardware
threads, GCC 16.2, Clang 22.1, DynamoDB Local in a Podman container) on
2026-11-04. CI repeats the unit, sanitizer, static analysis and DynamoDB Local
jobs on every push (see `.github/workflows/ci.yml`).

## Test suites

| Suite | Binary / script | What it covers |
|---|---|---|
| Unit | `flowd_unit_tests` (80 tests) | State machine table, retry/backoff, DAG validation and cycle detection, JSON parsing, lock-free queue (FIFO, move-only, MPMC exactly-once, reclamation), thread pool (drain, shutdown, exceptions, recursive submission, lost wake-ups), built-in handlers (shell output, exit codes, timeouts killing process trees, cancellation), timer service, in-process DAG executor (diamond order, retries, timeouts, failure propagation, 10k-task DAG, concurrent workflows) |
| Store contract | `tests/common/task_store_contract.cpp` | 12 tests of the `TaskStore` semantics: exclusive claims (16 racing threads, exactly one winner), fencing of renew/complete/fail by owner and attempt, backoff visibility, lease expiry and stale-owner rejection, idempotent dependency resolution (16 concurrent resolvers release a child exactly once), upstream failure, propagation index |
| Worker scenarios | `tests/common/worker_scenarios.cpp` | 11 end-to-end scenarios with real `Worker` instances: dependency order, 5 workers with no duplicate execution, retry backoff timing, failure propagation, crashed worker recovery, zombie worker fencing, commit failure recovery, propagation repair after a crash, attempt limit for crash-looping tasks, graceful drain, 5,000 tasks across 4 workers |
| Integration | `flowd_integration_tests` (38 tests) | The contract suite and worker scenarios against DynamoDB Local (scratch table per test), plus batched creation of a 260-task workflow |
| Multi-process crash | `tests/integration/crash_recovery.sh` | 3 `flowd worker` processes; one is `SIGKILL`ed while holding leases; survivors must reap, re-execute and finish a 60-task workflow, then exit cleanly on SIGTERM |

The contract suite and the scenarios are written once and instantiated per
store (`InMemorySharded`, `InMemorySingleShard`, `DynamoDbSharded`,
`DynamoDbSingleShard`), so both stores are held to identical semantics.
Without `FLOWD_DYNAMODB_ENDPOINT` the DynamoDB instances report themselves as
skipped. They refuse to run against any non-local endpoint.

## Commands

```bash
cmake --preset debug && cmake --build --preset debug -j && ctest --preset debug
cmake --preset tsan  && cmake --build --preset tsan  -j && ctest --preset tsan
cmake --preset asan  && cmake --build --preset asan  -j && ctest --preset asan

# Concurrency stress: repeat the concurrent suites under TSan
TSAN_OPTIONS=halt_on_error=1 build/tsan/tests/flowd_unit_tests \
  --gtest_filter='LockFreeQueue.*:ThreadPool.*:DagExecutorTest.*:Stores/*' --gtest_repeat=20

# DynamoDB Local
scripts/bootstrap-deps.sh
scripts/dynamodb-local.sh start
export FLOWD_DYNAMODB_ENDPOINT=http://localhost:8000
cmake --preset dynamodb-debug && cmake --build --preset dynamodb-debug -j
ctest --test-dir build/dynamodb-debug                 # unit + integration + crash script
tests/integration/crash_recovery.sh build/dynamodb-debug/apps/flowd
```

## Results

| Configuration | Result |
|---|---|
| GCC 16 Debug (`-Werror`) | 80/80 passed |
| GCC 16 Release (`-Werror`) | 80/80 passed |
| Clang 22 ThreadSanitizer | 80/80 passed, 0 TSan reports |
| Clang 22 AddressSanitizer + UBSan (leak detection on) | 80/80 passed, 0 reports |
| TSan stress: 55 concurrent tests x 20 repetitions | 20/20 runs passed, 0 TSan reports |
| Debug repeat of worker scenarios x 25 | 25/25 passed |
| DynamoDB build, all tests with DynamoDB Local | 118/118 passed (80 unit + 38 integration incl. crash script) |
| Integration suite x 3 repetitions | 3/3 passed (37 tests each) |
| `crash_recovery.sh` x 4 runs | 4/4 PASS; each run re-executed the 4 tasks the killed worker was holding |
| clang-tidy 23 (`.clang-tidy` checks) on `src/ apps/ benchmarks/` | 0 findings |
| cppcheck 2.22 (warning, performance, portability) | 0 findings |
| shellcheck on all scripts | 0 findings |
| cfn-lint on `deploy/ec2/cloudformation.yaml` | 0 findings |

Example crash test output:

```text
submitted crash-recovery-20261104T173234-455e51
killed victim (pid 91667) while it held leases
status=SUCCEEDED total=60 succeeded=60 failed=0 running=0 retrying=0 pending=0
recovered 4 task(s) from the killed worker
PASS
```

## ThreadSanitizer

TSan builds use Clang (the GCC sanitizer runtimes are not installed on the
development machine; Clang ships its own). `FLOWD_SANITIZER=thread` applies
`-fsanitize=thread` globally, so GoogleTest and the fetched dependencies are
instrumented too. To confirm the instrumentation was active, a deliberately
racy two-thread program built with the same flags was reported immediately
(`WARNING: ThreadSanitizer: data race`).

What the TSan runs exercise:

- `LockFreeQueue`: 4 producers x 4 consumers x 50,000 values with exactly-once
  and per-producer FIFO checks; 8 threads doing interleaved push/pop with
  hazard-pointer reclamation, which checks that freed nodes have a
  happens-before edge to every earlier access.
- `ThreadPool`: 8 concurrent producers, recursive fan-out of 16,383 jobs,
  shutdown while jobs are queued, repeated idle and wake-up cycles.
- `DagExecutor`: 10,000-task DAGs, 64 failing parents racing to fail shared
  children (20 rounds), 16 concurrent workflows on one pool.
- Workers: up to 5 workers with poller, heartbeat, maintenance and executor
  threads; kill and stop while tasks are in flight; lease loss racing with
  completion.

Findings during development: none in Flowd code. One false negative was
avoided by design. The hazard-pointer implementation uses `seq_cst`
operations instead of `atomic_thread_fence`, because TSan does not model
fences.

Limitations: the integration tests (AWS SDK) are not run under TSan, because
the SDK and libcurl are not instrumented and would produce false positives.
The worker's concurrency logic is identical across stores and is covered
under TSan through the in-memory store.

## Bugs found by the tests

- `interruptible_sleep` reported success after cancellation (predicate
  semantics of `condition_variable_any::wait_for`). Caught by
  `BuiltinHandlers.SleepAndShellObserveCancellation`.
- DynamoDB Local rejected the dummy access key because it contained a hyphen;
  newer releases accept only alphanumeric key IDs.
- Every CLI invocation off EC2 spent about 6 s probing the EC2 metadata service
  for a region; the probe is now skipped when the region is known. Found by
  the crash-recovery script's polling loop timing out.
- The crash script mis-detected matches because `grep -q` closing the pipe
  made the producer fail under `pipefail`.
- GoogleTest discovery at build time omitted the DynamoDB parameters; the
  suites now always instantiate them and skip at run time.
