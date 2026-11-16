```text
  o---o     _____   _                        _
  |\  |    |  ___| | |  ___  __      __   __| |
  | o-o    | |_    | | / _ \ \ \ /\ / /  / _` |
  |/  |    |  _|   | || (_) | \ V  V /  | (_| |
  o---o    |_|     |_| \___/   \_/\_/    \__,_|

  distributed workflow scheduler for C++20
```

[![CI](https://github.com/depuc8/Flowd/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/depuc8/Flowd/actions/workflows/ci.yml)

Flowd runs workflows made of dependent tasks (a DAG). Tasks start once their
dependencies succeed, failures are retried with exponential backoff, and the
work is spread over any number of workers that coordinate only through
conditional writes to DynamoDB. Kill a worker mid-task and the others pick the
task up.

## Highlights

- **DAG workflows** with validation and cycle detection
- **Task state machine:** `PENDING -> RUNNING -> RETRYING -> SUCCEEDED / FAILED`
- **Lock-free work queue** and thread pool for in-process execution
- **Distributed workers** with atomic claims, leases and heartbeats
- **Crash recovery:** expired leases are reclaimed, interrupted work is repaired
- **Runs without AWS:** in-memory store for development, DynamoDB Local for tests
- Tested under ThreadSanitizer and AddressSanitizer, CI on every push

## Quick start

Requires CMake 3.25+ and a C++20 compiler (GCC 13+ or Clang 17+).

```bash
cmake --preset release && cmake --build --preset release -j
./build/release/apps/flowd run examples/etl.json
```

```text
TASK                             STATE      ATTEMPT  OUTPUT / ERROR
extract-orders                   SUCCEEDED        1  extracting orders
extract-customers                SUCCEEDED        1  extracting customers
validate                         SUCCEEDED        2  succeeded on attempt 2
transform                        SUCCEEDED        1  transform attempt 1 key=nightly-etl/transform
load-warehouse                   SUCCEEDED        1
refresh-dashboards               SUCCEEDED        1  dashboards refreshed
notify                           SUCCEEDED        1
SUCCEEDED in 0.456s: 7 succeeded, 0 failed
```

`validate` is set to fail once, so it was retried. To run the same workflow
through the full distributed protocol (claims, leases, fenced commits) with
three in-process workers:

```bash
./build/release/apps/flowd local examples/etl.json --workers 3
```

## A workflow

```json
{
  "name": "nightly-etl",
  "defaults": {"retry": {"max_attempts": 3, "initial_backoff_ms": 200}},
  "tasks": [
    {"id": "extract", "handler": "shell", "payload": {"command": "./extract.sh"}},
    {"id": "load", "handler": "shell", "depends_on": ["extract"],
     "payload": {"command": "./load.sh"}}
  ]
}
```

Built-in handlers: `shell`, `sleep`, `echo`, `fail`, `noop`. You can also
register your own in C++. Details in the [usage guide](docs/USAGE.md).

## How it works

```text
  flowd submit ---> +-------------------+  <--- claim / renew / commit ---  flowd worker  (x N)
                    |  DynamoDB table   |                                   poller -> queue -> threads
  flowd status <--- |  + "sched" index  |  <--- reap expired leases -----   heartbeat, maintenance
                    +-------------------+
```

- A worker **claims** a ready task with one conditional write, so only one
  worker wins. The claim comes with a **lease** that the worker keeps renewing.
- If a worker dies, its lease expires and any other worker **reclaims** the
  task. A late result from the old worker is rejected by an attempt-number
  fence, so at most one result is ever committed.
- Delivery is **at least once**: a crash between finishing and committing
  means the task runs again. Make handlers idempotent; each gets a stable
  `FLOWD_IDEMPOTENCY_KEY`.

## Commands

| Command | What it does |
|---|---|
| `flowd run <wf.json>` | Run a workflow in this process |
| `flowd local <wf.json>` | Run it with in-process workers and the in-memory store |
| `flowd init-table` | Create the DynamoDB table |
| `flowd submit <wf.json>` | Store a workflow for workers to execute |
| `flowd worker` | Claim and run tasks until stopped |
| `flowd status <id>` | Show progress |

Running against DynamoDB (local or AWS) and deploying to EC2 are covered in
the [usage guide](docs/USAGE.md).

## Tests and benchmarks

```bash
ctest --preset debug       # unit tests (also: --preset tsan, --preset asan)
./build/release/benchmarks/flowd_bench --quick
```

On a 16-thread laptop the in-process engine schedules about 1 to 2 million
no-op tasks per second, flat from 1k to 100k tasks. Full results are in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md) and test results (including
sanitizer runs and the `kill -9` crash test) in [docs/TESTING.md](docs/TESTING.md).

## Limitations

- At-least-once execution, not exactly-once
- Several DynamoDB writes per task, so it suits tasks that take milliseconds or more
- No cancellation, priorities or dynamic DAGs yet

The full list is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#12-trade-offs-and-limitations).

## Docs

- [Usage guide](docs/USAGE.md): workflow format, DynamoDB, EC2 deployment, library use
- [Architecture](docs/ARCHITECTURE.md): state machine, schema, crash recovery, concurrency
- [Testing](docs/TESTING.md) and [benchmarks](docs/BENCHMARKS.md)
