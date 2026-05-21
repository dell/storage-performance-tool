#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"
RUN_SCRIPT="$ENGINE_ROOT/bundle/build/dist/run.sh"

declare -A _caller_env
for _v in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_AUTH_VERSION \
          HOSTS WORKER_HOSTS PRIMARY_HOST \
          MPUTEST_CONCURRENCY MPUTEST_OBJECT_SIZE MPUTEST_PART_SIZE \
          MPUTEST_OP_LIMIT_COUNT MPUTEST_TIME_LIMIT \
          MPUTEST_MPU_OBJECTS MPUTEST_MPU_PARTS \
          MPUTEST_CASE_TIMEOUT MPUTEST_WORKER_UP MPUTEST_WORKER_DOWN \
          MPUTEST_HELPER_SNAPSHOTS MPUTEST_HELPER_INTERVAL_SECONDS MPUTEST_HELPER_MAX_SNAPSHOTS \
          MPUTEST_OUTPUT_DIR MPUTEST_STEP_ID_PREFIX; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

[ -f "$GIT_ROOT/.env" ] && source "$GIT_ROOT/.env"
[ -f "$ENGINE_ROOT/.env" ] && source "$ENGINE_ROOT/.env"

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

: "${S3_ENDPOINTS:=http://127.0.0.1:9000}"
: "${S3_ACCESS_KEY:=AWS_ACCESS_KEY_ID_EXAMPLE}"
: "${S3_SECRET_KEY:=AWS_SECRET_ACCESS_KEY_EXAMPLE}"
: "${S3_BUCKET:=testmputest}"
: "${S3_AUTH_VERSION:=4}"

: "${MPUTEST_CONCURRENCY:=4}"
: "${MPUTEST_OBJECT_SIZE:=50MB}"
: "${MPUTEST_PART_SIZE:=5MB}"
: "${MPUTEST_OP_LIMIT_COUNT:=30}"
: "${MPUTEST_TIME_LIMIT:=60s}"
: "${MPUTEST_MPU_OBJECTS:=0}"
: "${MPUTEST_MPU_PARTS:=0}"
: "${MPUTEST_CASE_TIMEOUT:=30m}"
: "${MPUTEST_WORKER_UP:=true}"
: "${MPUTEST_WORKER_DOWN:=true}"
: "${MPUTEST_STEP_ID_PREFIX:=mputest-dist}"
: "${MPUTEST_HELPER_SNAPSHOTS:=false}"
: "${MPUTEST_HELPER_INTERVAL_SECONDS:=15}"
: "${MPUTEST_HELPER_MAX_SNAPSHOTS:=0}"

WORKER_HOSTS="${WORKER_HOSTS:-${HOSTS:-}}"
if [ -z "$WORKER_HOSTS" ]; then
  echo "ERROR: WORKER_HOSTS or HOSTS must be set in .env or environment" >&2
  exit 1
fi

NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"
LOAD_STEP_NODE_ADDRS="$(echo "$WORKER_HOSTS" | sed 's/[[:space:]]//g' | sed 's/@[^,]*/&/g' | sed 's/\([^,@]*@\)\{0,1\}\([^,:]*\)\(:[0-9]*\)\{0,1\}/\2\3/g')"

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi

RUN_ID="$(date +%Y%m%d-%H%M%S)"
RESULTS_ROOT="${MPUTEST_OUTPUT_DIR:-$SCRIPT_DIR/results/mputest-distributed-${RUN_ID}}"
mkdir -p "$RESULTS_ROOT"
SUMMARY_FILE="$RESULTS_ROOT/summary.tsv"
printf "case\tmode\texit_code\toutcome\tduration_sec\tlog_file\n" >"$SUMMARY_FILE"

CONN_ARGS=(
  --storage-driver-type=s3
  "--storage-net-node-addrs=${NODE_ADDRS}"
  "--storage-auth-version=${S3_AUTH_VERSION}"
  "--storage-auth-uid=${S3_ACCESS_KEY}"
  "--storage-auth-secret=${S3_SECRET_KEY}"
  "--storage-driver-limit-concurrency=${MPUTEST_CONCURRENCY}"
  "--storage-driver-limit-multipart-objects=${MPUTEST_MPU_OBJECTS}"
  "--storage-driver-limit-multipart-parts=${MPUTEST_MPU_PARTS}"
  "--load-step-node-addrs=${LOAD_STEP_NODE_ADDRS}"
  --load-batch-size=1
)

COMMON_MPU_ARGS=(
  "${CONN_ARGS[@]}"
  "--item-output-path=/${S3_BUCKET}"
  "--item-data-size=${MPUTEST_OBJECT_SIZE}"
  "--item-data-ranges-threshold=${MPUTEST_PART_SIZE}"
  --load-op-type=create
)

normalize_bool() {
  local raw="${1:-}"
  case "${raw,,}" in
    1|true|yes|on)
      echo "true"
      ;;
    0|false|no|off|"")
      echo "false"
      ;;
    *)
      return 1
      ;;
  esac
}

print_header() {
  local case_name="$1"
  local mode="$2"
  echo ""
  echo "=== Distributed MPU Test: ${case_name} (${mode}) ==="
  echo "Workers:     ${WORKER_HOSTS}"
  echo "S3 endpoint: ${S3_ENDPOINTS}"
  echo "Bucket:      ${S3_BUCKET}"
  echo "Concurrency: ${MPUTEST_CONCURRENCY}"
  echo "Object size: ${MPUTEST_OBJECT_SIZE}"
  echo "Part size:   ${MPUTEST_PART_SIZE}"
  echo "MPU limits:  objects=${MPUTEST_MPU_OBJECTS} parts=${MPUTEST_MPU_PARTS}"
  echo "Helper:      snapshots=${MPUTEST_HELPER_SNAPSHOTS} interval=${MPUTEST_HELPER_INTERVAL_SECONDS}s max=${MPUTEST_HELPER_MAX_SNAPSHOTS}"
  echo "Results dir: ${RESULTS_ROOT}"
  echo "==============================================="
}

capture_node_snapshots() {
  local case_dir="$1"
  mkdir -p "$case_dir"
  "$SCRIPT_DIR/jar-nodes-status.sh" >"$case_dir/nodes-status.txt" 2>&1 || true
  IFS=',' read -r -a _workers <<<"$WORKER_HOSTS"
  for host in "${_workers[@]}"; do
    local host_trim="${host//[$'\n\t\r ']}"
    [ -z "$host_trim" ] && continue
    local host_no_user="${host_trim#*@}"
    local host_safe="${host_no_user//[^A-Za-z0-9._-]/_}"
    curl -m 3 -s "http://${host_no_user}:9999/status" >"$case_dir/status_${host_safe}.json" || true
    curl -m 3 -s "http://${host_no_user}:9999/metrics/json" >"$case_dir/metrics_${host_safe}.json" || true
    curl -m 3 -s "http://${host_no_user}:9999/metrics/fleet/json" >"$case_dir/metrics_fleet_${host_safe}.json" || true
  done
}

helper_snapshot_loop() {
  local helper_dir="$1"
  local timeline_file="$2"
  local interval_sec="$3"
  local max_snapshots="$4"
  local i=0
  while true; do
    i=$((i + 1))
    local snap_dir="$helper_dir/$(printf '%04d' "$i")-$(date +%Y%m%d-%H%M%S)"
    printf "[%s] helper_snapshot_start idx=%s dir=%s\n" "$(date -Is)" "$i" "$snap_dir" >>"$timeline_file"
    capture_node_snapshots "$snap_dir"
    printf "[%s] helper_snapshot_end idx=%s dir=%s\n" "$(date -Is)" "$i" "$snap_dir" >>"$timeline_file"
    if [ "$max_snapshots" -gt 0 ] && [ "$i" -ge "$max_snapshots" ]; then
      printf "[%s] helper_snapshot_stop reason=max_reached count=%s\n" "$(date -Is)" "$i" >>"$timeline_file"
      return 0
    fi
    sleep "$interval_sec"
  done
}

run_case() {
  local case_name="$1"
  local mode="$2"
  shift 2
  local case_dir="$RESULTS_ROOT/$case_name"
  local log_file="$case_dir/run.log"
  local timeline_file="$case_dir/timeline.log"
  local helper_pid=""
  mkdir -p "$case_dir"
  local step_id="${MPUTEST_STEP_ID_PREFIX}-${case_name}-${RUN_ID}"

  local -a limit_args=()
  if [ "$mode" = "count" ]; then
    limit_args+=("--load-op-limit-count=${MPUTEST_OP_LIMIT_COUNT}")
  else
    limit_args+=("--load-step-limit-time=${MPUTEST_TIME_LIMIT}")
  fi

  print_header "$case_name" "$mode"
  local t0 t1 rc dur outcome
  printf "[%s] case_start mode=%s step_id=%s\n" "$(date -Is)" "$mode" "$step_id" >>"$timeline_file"
  capture_node_snapshots "$case_dir/pre"
  if [ "$MPUTEST_HELPER_SNAPSHOTS" = "true" ]; then
    mkdir -p "$case_dir/helper"
    helper_snapshot_loop \
      "$case_dir/helper" \
      "$timeline_file" \
      "$MPUTEST_HELPER_INTERVAL_SECONDS" \
      "$MPUTEST_HELPER_MAX_SNAPSHOTS" &
    helper_pid="$!"
    printf "[%s] helper_loop_start pid=%s interval_sec=%s max_snapshots=%s\n" \
      "$(date -Is)" "$helper_pid" "$MPUTEST_HELPER_INTERVAL_SECONDS" "$MPUTEST_HELPER_MAX_SNAPSHOTS" >>"$timeline_file"
  fi
  t0="$(date +%s)"
  set +e
  timeout --foreground "$MPUTEST_CASE_TIMEOUT" \
    "$RUN_SCRIPT" \
    "${COMMON_MPU_ARGS[@]}" \
    "--load-step-id=${step_id}" \
    "${limit_args[@]}" \
    "$@" 2>&1 | tee "$log_file"
  rc="${PIPESTATUS[0]}"
  set -e
  t1="$(date +%s)"
  dur=$((t1 - t0))

  if [ -n "$helper_pid" ]; then
    if kill -0 "$helper_pid" 2>/dev/null; then
      kill "$helper_pid" 2>/dev/null || true
    fi
    wait "$helper_pid" 2>/dev/null || true
    printf "[%s] helper_loop_end pid=%s\n" "$(date -Is)" "$helper_pid" >>"$timeline_file"
  fi

  if [ "$rc" -eq 0 ]; then
    outcome="success"
  elif [ "$rc" -eq 124 ]; then
    outcome="timeout"
  else
    outcome="error"
  fi

  printf "[%s] case_end rc=%s outcome=%s duration_sec=%s\n" "$(date -Is)" "$rc" "$outcome" "$dur" >>"$timeline_file"
  capture_node_snapshots "$case_dir/post"
  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$case_name" "$mode" "$rc" "$outcome" "$dur" "$log_file" >>"$SUMMARY_FILE"
  echo "[${case_name}] outcome=${outcome} rc=${rc} duration=${dur}s"
}

MODE="${1:-matrix}"
if [ "$#" -gt 0 ]; then
  shift
fi
case "$MODE" in
  count|time|matrix)
    ;;
  --help|-h)
    cat <<'USAGE'
Usage: mputest.distributed.sh [count|time|matrix] [extra run.sh args...]

Helper options:
  --helper-snapshots
  --no-helper-snapshots
  --helper-interval-seconds <N>
  --helper-max-snapshots <N>

Equivalent environment variables:
  MPUTEST_HELPER_SNAPSHOTS=true|false
  MPUTEST_HELPER_INTERVAL_SECONDS=<N>
  MPUTEST_HELPER_MAX_SNAPSHOTS=<N>

Runs distributed MPU create workload through engine run.sh with --load-step-node-addrs.
Reads settings from .env (repo/engine) and environment.
USAGE
    exit 0
    ;;
  *)
    echo "Unknown mode: $MODE (expected count, time, or matrix)" >&2
    exit 2
    ;;
esac

while [ "$#" -gt 0 ]; do
  case "$1" in
    --helper-snapshots)
      MPUTEST_HELPER_SNAPSHOTS=true
      shift
      ;;
    --no-helper-snapshots)
      MPUTEST_HELPER_SNAPSHOTS=false
      shift
      ;;
    --helper-interval-seconds=*)
      MPUTEST_HELPER_INTERVAL_SECONDS="${1#*=}"
      shift
      ;;
    --helper-interval-seconds)
      if [ "$#" -lt 2 ]; then
        echo "ERROR: --helper-interval-seconds requires an integer value" >&2
        exit 2
      fi
      MPUTEST_HELPER_INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --helper-max-snapshots=*)
      MPUTEST_HELPER_MAX_SNAPSHOTS="${1#*=}"
      shift
      ;;
    --helper-max-snapshots)
      if [ "$#" -lt 2 ]; then
        echo "ERROR: --helper-max-snapshots requires a non-negative integer value" >&2
        exit 2
      fi
      MPUTEST_HELPER_MAX_SNAPSHOTS="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

if ! MPUTEST_HELPER_SNAPSHOTS="$(normalize_bool "$MPUTEST_HELPER_SNAPSHOTS")"; then
  echo "ERROR: MPUTEST_HELPER_SNAPSHOTS must be a boolean value" >&2
  exit 2
fi
if ! [[ "$MPUTEST_HELPER_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || [ "$MPUTEST_HELPER_INTERVAL_SECONDS" -lt 1 ]; then
  echo "ERROR: helper snapshot interval must be an integer >= 1 second" >&2
  exit 2
fi
if ! [[ "$MPUTEST_HELPER_MAX_SNAPSHOTS" =~ ^[0-9]+$ ]]; then
  echo "ERROR: helper max snapshots must be a non-negative integer" >&2
  exit 2
fi

if [ "$MPUTEST_WORKER_UP" = "true" ]; then
  "$SCRIPT_DIR/jar-nodes-up.sh"
fi

case "$MODE" in
  count)
    run_case "count_limit" "count" "$@"
    ;;
  time)
    run_case "time_limit" "time" "$@"
    ;;
  matrix)
    run_case "count_limit" "count" "$@"
    run_case "time_limit" "time" "$@"
    ;;
esac

if [ "$MPUTEST_WORKER_DOWN" = "true" ]; then
  "$SCRIPT_DIR/jar-nodes-down.sh"
fi

echo ""
echo "=== Distributed MPU summary ==="
cat "$SUMMARY_FILE"
echo "Artifacts: $RESULTS_ROOT"
