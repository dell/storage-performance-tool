#!/usr/bin/env bash
set -euo pipefail

# Distributed mock test using already-running worker nodes.
# - Does NOT start/stop workers; expects tools/nodes-up.sh has launched them in --run-node mode.
# - Starts an entry container (local or on PRIMARY_HOST) pointing at WORKER_HOSTS via RMI.
# - Always runs dummy-mock with fixed 1MB objects for 5 minutes.
# - Probes /metrics/json on entry and each worker while the run is active.
#
# Usage:
#   tools/nodes-testmock.sh [options]
#
# Options (override .env):
#   --entry local|remote       Where to run the entry (default: remote)
#   --primary HOST             Override PRIMARY_HOST
#   --workers CSV              Override WORKER_HOSTS (CSV of [user@]host)
#   --image IMG                Docker image (default: $SPT_IMAGE or built-in)
#   --probe-seconds N          Probe window (default: 30)
#   --probe-interval SEC       Probe cadence seconds (default: 1)
#   --keep-logs                Save entry logs under tools/logs
#   -h|--help                  Show help

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Defaults from env
ENTRY_MODE=${ENTRY_MODE:-remote}                 # local|remote
PRIMARY_HOST=${PRIMARY_HOST:-}
WORKER_HOSTS=${WORKER_HOSTS:-${HOSTS:-}}
RUN_PORT=${RUN_PORT:-9999}
RMI_PORT=${RMI_PORT:-1099}
SPT_IMAGE=${SPT_IMAGE:-${SPT_IMAGE:-"dcr.octo.dell.com/spt/spt"}}
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}
ENTRY_CONTAINER_NAME=${ENTRY_CONTAINER_NAME:-spt-primary}

# Fixed concurrency (do not override)
THREADS=8
# Fixed workload parameters (do not override)
FIXED_OBJECT_SIZE="1MB"
FIXED_DURATION="5m"

KEEP_LOGS=${KEEP_LOGS:-false}
LOG_ROOT=${LOG_ROOT:-"$ROOT_DIR/tools/logs"}

PROBE_SECONDS=${PROBE_SECONDS:-60}
PROBE_INTERVAL=${PROBE_INTERVAL:-1}

usage() {
  sed -n '1,120p' "$0" | sed 's/^# \{0,1\}//' | awk 'NR>2{print} NR==2{print}'
}

# Args
while [ $# -gt 0 ]; do
  case "$1" in
    --entry) ENTRY_MODE="${2:-}"; shift 2 ;;
    --primary) PRIMARY_HOST="${2:-}"; shift 2 ;;
    --workers) WORKER_HOSTS="${2:-}"; shift 2 ;;
    --image) SPT_IMAGE="${2:-}"; shift 2 ;;
    --probe-seconds) PROBE_SECONDS="${2:-}"; shift 2 ;;
    --probe-interval) PROBE_INTERVAL="${2:-}"; shift 2 ;;
    --keep-logs) KEEP_LOGS=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [ -z "$WORKER_HOSTS" ]; then
  echo "ERROR: WORKER_HOSTS (or HOSTS) must be set in .env or via --workers." >&2
  exit 2
fi

# Safety: ensure PRIMARY_HOST is not included among workers (by hostname)
if [ -n "$PRIMARY_HOST" ]; then
  _p_no_user="${PRIMARY_HOST#*@}"
  IFS=',' read -r -a _wh <<<"$WORKER_HOSTS"
  for _h in "${_wh[@]}"; do
    [ -z "$_h" ] && continue
    if [ "${_h#*@}" = "$_p_no_user" ]; then
      echo "ERROR: PRIMARY_HOST ($PRIMARY_HOST) must not appear in WORKER_HOSTS ($WORKER_HOSTS)." >&2
      exit 2
    fi
  done
fi

# Build worker RMI addresses from WORKER_HOSTS (strip user@)
IFS=',' read -r -a HOST_ARR <<<"$WORKER_HOSTS"
NODE_ADDRS=()
WORKER_NAMES=()
for t in "${HOST_ARR[@]}"; do
  t="${t//[$'\n\t\r ']}"; [ -z "$t" ] && continue
  name="$t"; [[ "$name" == *"@"* ]] && name="${name#*@}"
  NODE_ADDRS+=("${name}:${RMI_PORT}")
  WORKER_NAMES+=("$name")
done
NODE_ADDRS_CSV=$(IFS=','; echo "${NODE_ADDRS[*]}")

echo "=== Distributed Mock Test (reusing workers) ==="
echo "Entry: ${ENTRY_MODE}  Primary: ${PRIMARY_HOST:-local}"
echo "Workers: ${WORKER_NAMES[*]}"
echo "Image: $SPT_IMAGE"
echo "Workload: dummy-mock concurrency=$THREADS size=$FIXED_OBJECT_SIZE duration=$FIXED_DURATION (fixed)"
echo

mask_args_for_log() {
  printf '%s ' "$@"
}

MOCK_ARGS=(
  "--load-step-node-addrs=$NODE_ADDRS_CSV"
  "--storage-driver-type=dummy-mock"
  "--storage-driver-limit-concurrency=$THREADS"
  "--item-data-size=$FIXED_OBJECT_SIZE"
  "--load-op-type=create"
)

# Always fixed 5 minute duration
MOCK_ARGS+=("--load-step-limit-time=$FIXED_DURATION")

run_entry_local() {
  echo "> docker run [local] $(mask_args_for_log "$SPT_IMAGE" "${MOCK_ARGS[@]}")"
  if [ "$KEEP_LOGS" = true ]; then
    mkdir -p "$LOG_ROOT"
    docker run --rm --network host "$SPT_IMAGE" "${MOCK_ARGS[@]}" \
      2>&1 | tee -a "$LOG_ROOT/entry-mock.$(date -u +%Y%m%d-%H%M%S).log"
  else
    docker run --rm --network host "$SPT_IMAGE" "${MOCK_ARGS[@]}"
  fi
}

run_entry_remote() {
  local target="$PRIMARY_HOST"
  if [ -z "$target" ]; then
    echo "ERROR: PRIMARY_HOST must be set for remote entry." >&2
    return 2
  fi
  echo "> [remote $target] docker run [entry] $(mask_args_for_log "$SPT_IMAGE" "${MOCK_ARGS[@]}")"
  if [ "$KEEP_LOGS" = true ]; then
    mkdir -p "$LOG_ROOT"
    ssh ${SSH_OPTS:-} "$target" ENTRY_CONTAINER_NAME="$ENTRY_CONTAINER_NAME" SPT_IMAGE="$SPT_IMAGE" bash -s -- "${MOCK_ARGS[@]}" <<'EOS' | tee -a "$LOG_ROOT/entry-mock.$(date -u +%Y%m%d-%H%M%S).log"
set -euo pipefail
docker rm -f "$ENTRY_CONTAINER_NAME" >/dev/null 2>&1 || true
docker run --rm --name "$ENTRY_CONTAINER_NAME" --network host "$SPT_IMAGE" "$@"
EOS
  else
    ssh ${SSH_OPTS:-} "$target" ENTRY_CONTAINER_NAME="$ENTRY_CONTAINER_NAME" SPT_IMAGE="$SPT_IMAGE" bash -s -- "${MOCK_ARGS[@]}" <<'EOS'
set -euo pipefail
docker rm -f "$ENTRY_CONTAINER_NAME" >/dev/null 2>&1 || true
docker run --rm --name "$ENTRY_CONTAINER_NAME" --network host "$SPT_IMAGE" "$@"
EOS
  fi
}

# Probe loop: entry + workers
probe_loop() {
  local secs="$1"; shift || true
  local interval="$1"; shift || true
  local end_ts; end_ts=$(awk -v now="$(date +%s)" -v s="$secs" 'BEGIN{print now + s}')
  local has_jq=0; command -v jq >/dev/null 2>&1 && has_jq=1
  while :; do
    local now; now=$(date +%s)
    [ "$now" -ge "$end_ts" ] && break
    local ts; ts=$(date -u +%H:%M:%S)

    # Entry first (if remote provided); fall back to localhost for local entry
    local entry_host="${PRIMARY_HOST#*@}"
    if [ "$ENTRY_MODE" = "local" ] || [ -z "$entry_host" ]; then entry_host="127.0.0.1"; fi
    local e_body; e_body=$(curl -m 2 -sf "http://${entry_host}:${RUN_PORT}/metrics/json" || true)
    if [ $has_jq -eq 1 ] && [ -n "$e_body" ]; then
      local e_len; e_len=$(echo "$e_body" | jq 'length' 2>/dev/null || echo 0)
      echo "[$ts] entry(${entry_host}) metrics.length=${e_len}"
    else
      echo "[$ts] entry(${entry_host}) metrics.len=${#e_body}"
    fi

    for name in "${WORKER_NAMES[@]}"; do
      local body; body=$(curl -m 2 -sf "http://${name}:${RUN_PORT}/metrics/json" || true)
      if [ $has_jq -eq 1 ] && [ -n "$body" ]; then
        local mlen; mlen=$(echo "$body" | jq 'length' 2>/dev/null || echo 0)
        echo "[$ts] worker(${name}) metrics.length=${mlen}"
      else
        echo "[$ts] worker(${name}) metrics.len=${#body}"
      fi
    done
    sleep "$interval"
  done
}

echo "=== Probing for ${PROBE_SECONDS}s every ${PROBE_INTERVAL}s ==="
probe_loop "$PROBE_SECONDS" "$PROBE_INTERVAL" &
PROBE_PID=$!

entry_rc=0
case "$ENTRY_MODE" in
  local)  run_entry_local  || entry_rc=$? ;;
  remote) run_entry_remote || entry_rc=$? ;;
  *) echo "ERROR: --entry must be local|remote" >&2; entry_rc=2 ;;
esac

if ps -p "$PROBE_PID" >/dev/null 2>&1; then
  wait "$PROBE_PID" || true
fi

echo
echo "=== Done (entry rc=$entry_rc) ==="
exit "$entry_rc"
