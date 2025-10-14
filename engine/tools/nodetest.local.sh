#!/usr/bin/env bash
set -euo pipefail

# Simple local node smoke test for Spt v5
# - Builds the local distribution if needed
# - Launches the node (`--run-node`) on a given port
# - Probes key endpoints (/health, /ready, /status, /metrics/json, /logs)
# - Starts a short run via POST /run, then stops it via DELETE /run using ETag
# - Prints responses; cleans up the Java process on exit

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT="${PORT:-9999}"
RUN_SCRIPT="$REPO_ROOT/bundle/build/dist/run.sh"
JAVA_OPTS="${JAVA_OPTS:-}"
KEEP_RUNNING=false

usage() {
  cat <<EOF
Usage: PORT=9999 tools/nodetest.local.sh

Environment variables:
  PORT       API port to bind (default: 9999)
  JAVA_OPTS  Extra JVM options (optional)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# Parse flags (only --run-node is recognized to keep node running after tests)
for arg in "$@"; do
  case "$arg" in
    --run-node)
      KEEP_RUNNING=true
      ;;
    *)
      ;;
  esac
done

log() { printf "[%s] %s\n" "$(date -u +%H:%M:%S)" "$*"; }

# Ensure distribution exists
if [[ ! -x "$RUN_SCRIPT" ]]; then
  log "Building local distribution (run script missing)..."
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi

tmpdir=$(mktemp -d)
pidfile="$tmpdir/spt.pid"
out="$tmpdir/node.out"

cleanup() {
  if [[ "$KEEP_RUNNING" != "true" ]]; then
    # Graceful shutdown via API
    log "Requesting graceful shutdown via POST /shutdown..."
    curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/shutdown" || true
    # Wait up to 30s for node to stop
    for i in $(seq 1 30); do
      code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/status" || true)
      if [[ "$code" != "200" ]]; then
        log "Node stopped."
        break
      fi
      sleep 1
      [[ $i -eq 30 ]] && log "WARN: Node still responding after 30s; it may still be lingering."
    done
  else
    if [[ -s "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
      log "Leaving node running (PID $(cat "$pidfile")) on port $PORT"
    fi
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT INT TERM

log "Starting node on port $PORT..."
( cd "$REPO_ROOT" && env JAVA_OPTS="$JAVA_OPTS" "$RUN_SCRIPT" --run-port="$PORT" --run-node >/dev/null 2>"$out" & echo $! >"$pidfile" )

# Wait for readiness via /ready (fallback: /metrics)
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/ready" || true)
  if [[ "$code" == "200" ]]; then
    break
  fi
  if [[ "$code" == "404" ]]; then
    # Older builds: fall back to /metrics
    if curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/metrics" | grep -q '^200$'; then
      break
    fi
  fi
  sleep 1
  if [[ $i -eq 30 ]]; then
    log "Timeout waiting for node readiness"; echo "--- recent logs ---"; tail -n 100 "$out" || true; exit 1
  fi
done
log "Node is ready. Probing endpoints..."

log "/health"
curl -sS "http://127.0.0.1:$PORT/health" | sed 's/^/  /'

log "/ready"
curl -sSI "http://127.0.0.1:$PORT/ready" | sed 's/^/  /'

log "/status"
curl -sS "http://127.0.0.1:$PORT/status" | sed 's/^/  /'

log "/metrics/json"
curl -sS "http://127.0.0.1:$PORT/metrics/json" | sed 's/^/  /'

log "HEAD /logs/any/NoSuchLogger (expect 404)"
curl -sSI "http://127.0.0.1:$PORT/logs/any/NoSuchLogger" | sed 's/^/  /'

log "/logs/any/index.json (expect items: [])"
curl -sS "http://127.0.0.1:$PORT/logs/any/index.json" | sed 's/^/  /'

log "POST /run (start default scenario)"
etag=$(curl -sS -X POST -D - "http://127.0.0.1:$PORT/run" -o "$tmpdir/post.out" | awk 'BEGIN{IGNORECASE=1}/^ETag:/ {print $2}' | tr -d '\r')
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/run")
if [[ -z "$etag" ]]; then
  log "WARN: No ETag returned from POST /run; raw headers:"; \
  curl -sS -X POST -D - "http://127.0.0.1:$PORT/run" -o /dev/null | sed 's/^/  /'
else
  log "Started run with ETag (run_id): $etag"
fi

sleep 1
log "/status (should be RUNNING)"
curl -sS "http://127.0.0.1:$PORT/status" | sed 's/^/  /'

if [[ -n "$etag" ]]; then
  log "DELETE /run (stop run using If-Match: $etag)"
  curl -sS -X DELETE -H "If-Match: $etag" -D - -o /dev/null "http://127.0.0.1:$PORT/run" | sed 's/^/  /'
else
  log "Skipping DELETE /run (no ETag)"
fi

sleep 1
log "/status (should be STOPPED during linger window)"
curl -sS "http://127.0.0.1:$PORT/status" | sed 's/^/  /'

if [[ "$KEEP_RUNNING" == "true" ]]; then
  log "Done. Node will continue running. To stop the node: curl -X POST http://127.0.0.1:$PORT/shutdown"
else
  log "Done. Node shutdown requested."
fi
