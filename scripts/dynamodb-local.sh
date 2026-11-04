#!/usr/bin/env bash
# Starts/stops DynamoDB Local for development and integration tests. No AWS
# account or credentials are involved.
#
#   scripts/dynamodb-local.sh start   # http://localhost:8000
#   scripts/dynamodb-local.sh stop
#   scripts/dynamodb-local.sh status
#
# Uses docker or podman (image amazon/dynamodb-local) when available, and
# otherwise the official DynamoDB Local Java distribution, downloaded once
# into .dynamodb-local/ (requires Java 17+).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR="$ROOT/.dynamodb-local"
PORT="${DYNAMODB_LOCAL_PORT:-8000}"
NAME="flowd-dynamodb-local"
JAR_URL="https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/DynamoDBLocal_latest.tar.gz"

runtime() {
    if [[ -n "${CONTAINER_RUNTIME:-}" ]]; then
        echo "$CONTAINER_RUNTIME"
    elif command -v docker >/dev/null 2>&1; then
        echo docker
    elif command -v podman >/dev/null 2>&1; then
        echo podman
    fi
}

wait_ready() {
    for _ in $(seq 1 60); do
        if curl -s -o /dev/null "http://localhost:$PORT"; then
            echo "DynamoDB Local listening on http://localhost:$PORT"
            echo "export FLOWD_DYNAMODB_ENDPOINT=http://localhost:$PORT"
            return 0
        fi
        sleep 0.5
    done
    echo "DynamoDB Local did not start" >&2
    return 1
}

start() {
    local rt
    rt="$(runtime)"
    if [[ -n "$rt" && "${DYNAMODB_LOCAL_MODE:-}" != "java" ]]; then
        "$rt" rm -f "$NAME" >/dev/null 2>&1 || true
        "$rt" run -d --name "$NAME" -p "$PORT:8000" docker.io/amazon/dynamodb-local:latest \
            -jar DynamoDBLocal.jar -inMemory -sharedDb >/dev/null
    else
        command -v java >/dev/null 2>&1 || { echo "need docker, podman or java" >&2; exit 1; }
        if [[ ! -f "$DIR/DynamoDBLocal.jar" ]]; then
            mkdir -p "$DIR"
            echo "downloading DynamoDB Local into $DIR"
            curl -fsSL "$JAR_URL" | tar -xz -C "$DIR"
        fi
        (cd "$DIR" && nohup java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar \
            -inMemory -sharedDb -port "$PORT" >"$DIR/server.log" 2>&1 &
         echo $! >"$DIR/server.pid")
    fi
    wait_ready
}

stop() {
    local rt
    rt="$(runtime)"
    if [[ -n "$rt" ]]; then
        "$rt" rm -f "$NAME" >/dev/null 2>&1 || true
    fi
    if [[ -f "$DIR/server.pid" ]]; then
        kill "$(cat "$DIR/server.pid")" 2>/dev/null || true
        rm -f "$DIR/server.pid"
    fi
    echo "stopped"
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    status) curl -s -o /dev/null "http://localhost:$PORT" && echo "running on :$PORT" || echo "not running" ;;
    *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
