# Usage guide

Everything you need to define workflows, run them against DynamoDB, deploy
workers to EC2 and embed Flowd in your own program. For how it works inside,
see [ARCHITECTURE.md](ARCHITECTURE.md).

- [Workflow definitions](#workflow-definitions)
- [Running with DynamoDB](#running-with-dynamodb)
- [AWS deployment (EC2)](#aws-deployment-ec2)
- [Using Flowd as a library](#using-flowd-as-a-library)
- [Building with DynamoDB support](#building-with-dynamodb-support)

## Workflow definitions

```json
{
  "name": "nightly-etl",
  "defaults": {
    "retry": {"max_attempts": 3, "initial_backoff_ms": 200, "multiplier": 2.0,
              "max_backoff_ms": 5000, "jitter": 0.2},
    "timeout_ms": 60000
  },
  "tasks": [
    {"id": "extract", "handler": "shell", "payload": {"command": "./extract.sh"}},
    {"id": "load", "handler": "shell", "depends_on": ["extract"],
     "retry": {"max_attempts": 5}, "payload": {"command": "./load.sh"}}
  ]
}
```

Task ids must match `[A-Za-z0-9._-]{1,128}`. Task-level `retry` fields
override individual fields of `defaults.retry`.

| Handler | Payload | Behaviour |
|---|---|---|
| `shell` | `{"command": "..."}` | `/bin/sh -c`; non-zero exit is a retryable failure; stdout+stderr captured (64 KiB); process group killed on timeout. Environment: `FLOWD_WORKFLOW_ID`, `FLOWD_TASK_ID`, `FLOWD_ATTEMPT`, `FLOWD_IDEMPOTENCY_KEY`. |
| `sleep` | `{"ms": N}` | Interruptible sleep. |
| `echo` | `{"message": "..."}` | Succeeds with the message as output. |
| `fail` | `{"times": N}` or `{"fatal": true}` | Fails the first N attempts, or fails permanently. |
| `noop` | none | Succeeds immediately. |

Tasks may run more than once (at-least-once delivery), so handlers with side
effects should be idempotent. Use `FLOWD_IDEMPOTENCY_KEY` (`workflow/task`,
stable across attempts) as the key, and `FLOWD_ATTEMPT` (strictly increasing)
as a fencing token downstream.

## Running with DynamoDB

Locally, without an AWS account:

```bash
scripts/dynamodb-local.sh start            # docker/podman, or the Java build
F="./build/dynamodb/apps/flowd"
COMMON="--endpoint http://localhost:8000 --local"

$F init-table $COMMON
$F worker $COMMON --id worker-1 --threads 8 &
$F worker $COMMON --id worker-2 --threads 8 &
ID=$($F submit examples/etl.json $COMMON)
$F status "$ID" --tasks $COMMON
$F submit examples/etl.json $COMMON --wait     # blocks until finished
```

Against AWS, drop `--endpoint` and `--local`. Credentials come from the
standard AWS provider chain (environment, profile, or EC2 instance profile);
Flowd never stores credentials. Options can also be set through
`FLOWD_TABLE`, `AWS_REGION`, `FLOWD_DYNAMODB_ENDPOINT` and `FLOWD_SHARDS`.

| Command | Description |
|---|---|
| `flowd run <wf.json>` | In-process execution. |
| `flowd local <wf.json> [--workers N]` | Distributed protocol with in-process workers and the in-memory store. |
| `flowd init-table` / `delete-table --yes` | Create or delete the table and index. |
| `flowd submit <wf.json> [--id ID] [--wait]` | Persist a workflow. |
| `flowd worker [--threads N] [--lease-ms] [--heartbeat-ms]` | Run a worker until SIGINT/SIGTERM (graceful drain). |
| `flowd status <id> [--tasks]` | Show progress. |

Exit status: 0 success, 1 workflow failed, 2 usage error, 3 runtime error.
All workers and submitters must use the same `--shards` value.

## AWS deployment (EC2)

`deploy/ec2/` contains everything needed to run workers on EC2. **Deploying
creates billable resources; nothing in this repository deploys
automatically.**

| File | Purpose |
|---|---|
| `cloudformation.yaml` | DynamoDB table (on-demand, PITR, SSE) with the `sched` index; least-privilege IAM role; IMDSv2-only launch template; Auto Scaling group with CPU target tracking. |
| `build-release.sh` | Builds `dist/flowd-<version>-<arch>.tar.gz` (binary + SDK libraries + install files). |
| `install-worker.sh` | Installs the release as the `flowd-worker` systemd service. |
| `flowd-worker.service`, `worker.env.example` | Hardened unit with graceful SIGTERM drain. |
| `deploy.sh` | Uploads the tarball to S3 and deploys the stack. Dry run unless `--yes`. |

```bash
scripts/bootstrap-deps.sh && deploy/ec2/build-release.sh   # build on the target arch
deploy/ec2/deploy.sh --stack flowd --bucket <artifact-bucket> \
    --tarball dist/flowd-0.1.0-aarch64.tar.gz \
    --vpc vpc-... --subnets subnet-a,subnet-b --region eu-west-1     # prints the plan
deploy/ec2/deploy.sh ... --yes                                       # actually deploys
```

Then submit from anywhere with DynamoDB access:
`flowd submit wf.json --table flowd --region eu-west-1`. Workers use the
instance profile, so there are no keys on the instances. Keep chrony enabled,
because leases compare wall-clock time across instances (see
[ARCHITECTURE.md section 11](ARCHITECTURE.md#11-time-and-clocks)).

## Using Flowd as a library

```cpp
#include "flowd/client/workflow_client.hpp"
#include "flowd/store/dynamodb_task_store.hpp"
#include "flowd/worker/worker.hpp"

flowd::HandlerRegistry handlers;
flowd::register_builtin_handlers(handlers);
handlers.add("resize-image", [](const flowd::TaskContext& ctx) {
    // ctx.payload (JSON), ctx.attempt, ctx.idempotency_key(), ctx.stop
    if (ctx.stop.stop_requested()) return flowd::TaskResult::failure("cancelled");
    return flowd::TaskResult::success("done");
});

flowd::AwsSdkSession sdk;
flowd::DynamoDbTaskStore store({.table_name = "flowd", .region = "eu-west-1"});
flowd::Worker worker(store, handlers, {.threads = 16});
worker.start();
// ...
worker.stop();
```

For single-machine use without persistence, `DagExecutor` runs a `Dag`
directly on a `ThreadPool`.

## Building with DynamoDB support

DynamoDB support needs the AWS SDK for C++ (core + dynamodb). The bootstrap
script builds it into `./.deps` (plus libcurl, if no development package is
installed):

```bash
scripts/bootstrap-deps.sh                     # ~15 min, once
cmake --preset dynamodb && cmake --build --preset dynamodb -j
```

On Ubuntu, install `libcurl4-openssl-dev libssl-dev zlib1g-dev` first.
Integration tests need DynamoDB Local and are described in
[TESTING.md](TESTING.md).
