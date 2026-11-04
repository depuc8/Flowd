#!/usr/bin/env bash
# End-to-end crash recovery test with real processes:
#   1. create a scratch table in DynamoDB Local and submit a 60-task workflow
#   2. start three `flowd worker` processes
#   3. SIGKILL one worker while it holds leases
#   4. the survivors must reap the expired leases and finish the workflow
#
# Usage: crash_recovery.sh <path-to-flowd>
# Requires FLOWD_DYNAMODB_ENDPOINT pointing at DynamoDB Local; exits 77
# (skipped) otherwise.
set -euo pipefail

FLOWD="${1:?usage: crash_recovery.sh <flowd binary>}"
ENDPOINT="${FLOWD_DYNAMODB_ENDPOINT:-}"
if [[ -z "$ENDPOINT" ]]; then
    echo "SKIP: FLOWD_DYNAMODB_ENDPOINT is not set"
    exit 77
fi
case "$ENDPOINT" in
    http://localhost* | http://127.0.0.1* | http://dynamodb-local*) ;;
    *) echo "refusing to run against non-local endpoint $ENDPOINT" >&2; exit 1 ;;
esac

TABLE="flowd_crash_$$_${RANDOM}"
COMMON=(--endpoint "$ENDPOINT" --local --table "$TABLE" --shards 4)
WORK="$(mktemp -d)"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    "$FLOWD" delete-table --yes "${COMMON[@]}" --log-level error >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    for f in "$WORK"/*.log; do
        echo "---- $f" >&2
        tail -n 30 "$f" >&2
    done
    exit 1
}

# 20 roots of 400 ms each, 40 dependents of 100 ms each.
{
    echo '{"name": "crash-recovery", "defaults": {"retry": {"max_attempts": 5, "initial_backoff_ms": 100}},'
    echo ' "tasks": ['
    for i in $(seq 0 19); do
        echo "  {\"id\": \"root-$i\", \"handler\": \"sleep\", \"payload\": {\"ms\": 400}},"
    done
    for i in $(seq 0 39); do
        sep=","
        [[ $i -eq 39 ]] && sep=""
        echo "  {\"id\": \"child-$i\", \"handler\": \"sleep\", \"depends_on\": [\"root-$((i % 20))\"], \"payload\": {\"ms\": 100}}$sep"
    done
    echo ']}'
} >"$WORK/workflow.json"

"$FLOWD" init-table "${COMMON[@]}" --log-level warn
WF="$("$FLOWD" submit "$WORK/workflow.json" "${COMMON[@]}" --log-level warn)"
echo "submitted $WF"

start_worker() {
    "$FLOWD" worker "${COMMON[@]}" --id "$1" --threads 4 --lease-ms 2000 --heartbeat-ms 500 \
        --log-level info >"$WORK/$1.log" 2>&1 &
    PIDS+=("$!")
}
start_worker victim
VICTIM_PID="${PIDS[0]}"

tasks_table() {
    # Captured in a variable: piping into `grep -q` would SIGPIPE flowd and,
    # with pipefail, report a match as a failure.
    "$FLOWD" status "$WF" --tasks "${COMMON[@]}" --log-level error || true
}

# Wait until the victim holds leases, then start the survivors and kill it.
victim_running=0
for _ in $(seq 1 100); do
    if grep -q "RUNNING .*victim" <<<"$(tasks_table)"; then
        victim_running=1
        break
    fi
    sleep 0.1
done
((victim_running)) || fail "victim never claimed a task"
start_worker survivor-1
start_worker survivor-2
kill -9 "$VICTIM_PID"
echo "killed victim (pid $VICTIM_PID) while it held leases"

deadline=$((SECONDS + 120))
status=""
while ((SECONDS < deadline)); do
    status="$("$FLOWD" status "$WF" "${COMMON[@]}" --log-level error || true)"
    if grep -qE "status=(SUCCEEDED|FAILED)" <<<"$status"; then
        break
    fi
    sleep 0.5
done
echo "$status"
grep -q "status=SUCCEEDED" <<<"$status" || fail "workflow did not succeed"

TASKS="$(tasks_table)"
# Tasks the victim was running must have been re-executed (attempt >= 2).
retried="$(awk '$2 == "SUCCEEDED" && $3 >= 2 {n++} END {print n + 0}' <<<"$TASKS")"
((retried >= 1)) || fail "no task was retried after the crash"
grep -h "reaped expired lease" "$WORK"/survivor-*.log >/dev/null || fail "survivors never reaped a lease"
echo "recovered $retried task(s) from the killed worker"

# Survivors must shut down cleanly on SIGTERM.
for pid in "${PIDS[@]:1}"; do
    kill -TERM "$pid"
done
for pid in "${PIDS[@]:1}"; do
    wait "$pid" || fail "worker $pid did not exit cleanly"
done
PIDS=()
echo "PASS"
