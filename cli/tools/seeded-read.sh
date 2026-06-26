#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

HOSTS=${HOSTS:-"127.0.0.1"}
S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ENDPOINTS=${S3_ENDPOINTS:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
S3_BUCKET=${S3_BUCKET:-""}
THREADS=${THREADS:-8}
OBJECT_SIZE=${OBJECT_SIZE:-"1KB"}
OBJECT_COUNT=${OBJECT_COUNT:-100000}
READ_COUNT=${READ_COUNT:-$OBJECT_COUNT}
MIN_HOSTS=${MIN_HOSTS:-3}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT_DIR/results"}
LABEL_PREFIX=${LABEL_PREFIX:-"seeded-read"}
VERBOSE=${VERBOSE:-true}
FORCE=${FORCE:-true}
KEEP_SCENARIO=${KEEP_SCENARIO:-true}
CLEANUP=${CLEANUP:-false}
ATTACH_EXISTING=${ATTACH_EXISTING:-false}
WAIT_FOR_SHUTDOWN=${WAIT_FOR_SHUTDOWN:-true}
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-90}
WAIT_POLL_SECONDS=${WAIT_POLL_SECONDS:-2}
WAIT_PORTS=${WAIT_PORTS:-"9999,1099"}
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}
SPT_IMAGE=${SPT_IMAGE:-""}
SKIP_IMAGE_PULL=${SPT_SKIP_IMAGE_PULL:-false}
S3_DRIVER=${SPT_S3_DRIVER:-"default"}

while [ $# -gt 0 ]; do
  case "$1" in
    --hosts) HOSTS="${2:-}"; shift 2 ;;
    --s3-endpoint) S3_ENDPOINT="${2:-}"; shift 2 ;;
    --s3-endpoints) S3_ENDPOINTS="${2:-}"; shift 2 ;;
    --s3-access-key) S3_ACCESS_KEY="${2:-}"; shift 2 ;;
    --s3-secret-key) S3_SECRET_KEY="${2:-}"; shift 2 ;;
    --s3-bucket) S3_BUCKET="${2:-}"; shift 2 ;;
    --threads) THREADS="${2:-}"; shift 2 ;;
    --object-size) OBJECT_SIZE="${2:-}"; shift 2 ;;
    --object-count) OBJECT_COUNT="${2:-}"; READ_COUNT="$OBJECT_COUNT"; shift 2 ;;
    --read-count) READ_COUNT="${2:-}"; shift 2 ;;
    --min-hosts) MIN_HOSTS="${2:-}"; shift 2 ;;
    --results-dir) RESULTS_DIR="${2:-}"; shift 2 ;;
    --label-prefix) LABEL_PREFIX="${2:-}"; shift 2 ;;
    --s3-driver) S3_DRIVER="${2:-}"; shift 2 ;;
    --image) SPT_IMAGE="${2:-}"; shift 2 ;;
    --skip-image-pull) SKIP_IMAGE_PULL=true; shift 1 ;;
    --verbose) VERBOSE=true; shift 1 ;;
    --quiet|--no-verbose) VERBOSE=false; shift 1 ;;
    --force) FORCE=true; shift 1 ;;
    --no-force) FORCE=false; shift 1 ;;
    --cleanup) CLEANUP=true; shift 1 ;;
    --no-cleanup) CLEANUP=false; shift 1 ;;
    --keep-scenario) KEEP_SCENARIO=true; shift 1 ;;
    --no-keep-scenario) KEEP_SCENARIO=false; shift 1 ;;
    --attach-existing) ATTACH_EXISTING=true; shift 1 ;;
    --no-attach-existing) ATTACH_EXISTING=false; shift 1 ;;
    --wait-for-shutdown) WAIT_FOR_SHUTDOWN=true; shift 1 ;;
    --no-wait-for-shutdown) WAIT_FOR_SHUTDOWN=false; shift 1 ;;
    --wait-timeout) WAIT_TIMEOUT_SECONDS="${2:-}"; shift 2 ;;
    --wait-poll-seconds) WAIT_POLL_SECONDS="${2:-}"; shift 2 ;;
    --wait-ports) WAIT_PORTS="${2:-}"; shift 2 ;;
    -h|--help)
      cat << EOF
Usage: $(basename "$0") [options]

Seeds objects with a write workload, saves the generated items.csv, then runs a read workload against that exact items.csv.

Connection:
  --hosts CSV              Comma-separated [user@]host list (env: HOSTS)
  --s3-endpoint URL        Single S3 endpoint (env: S3_ENDPOINT)
  --s3-endpoints CSV       Multiple S3 endpoints (env: S3_ENDPOINTS)
  --s3-access-key KEY      S3 access key (env: S3_ACCESS_KEY)
  --s3-secret-key KEY      S3 secret key (env: S3_SECRET_KEY)
  --s3-bucket NAME         S3 bucket (env: S3_BUCKET)

Workload:
  --threads N              Concurrency per node (default: $THREADS)
  --object-size SIZE       Seed/read object size (default: $OBJECT_SIZE)
  --object-count N         Objects to seed and default read count (default: $OBJECT_COUNT)
  --read-count N           Read operation count (default: object-count)
  --min-hosts N            Minimum hosts required (default: $MIN_HOSTS)

Output:
  --results-dir DIR        Auto-results root (default: $RESULTS_DIR)
  --label-prefix LABEL     Label prefix for seed/read result directories (default: $LABEL_PREFIX)

Driver/Docker:
  --s3-driver TYPE         Storage driver: default, aws, rdma (env: SPT_S3_DRIVER, default: $S3_DRIVER)
  --image IMAGE            Override SPT Docker image (env: SPT_IMAGE)
  --skip-image-pull        Use locally cached image, skip pull (env: SPT_SKIP_IMAGE_PULL)

General:
  --[no-]verbose           Enable verbose output (default: $VERBOSE)
  --[no-]force             Stop conflicting containers (default: $FORCE)
  --[no-]cleanup           Delete seeded objects after the read phase (default: $CLEANUP)
  --[no-]keep-scenario     Keep generated scenario files (default: $KEEP_SCENARIO)
  --[no-]attach-existing   Reuse prestarted worker nodes (default: $ATTACH_EXISTING)
  --[no-]wait-for-shutdown Wait for seed containers to release ports before read (default: $WAIT_FOR_SHUTDOWN)
  --wait-timeout SECONDS   Max wait for ports to close (default: $WAIT_TIMEOUT_SECONDS)
  --wait-poll-seconds N    Poll interval while waiting (default: $WAIT_POLL_SECONDS)
  --wait-ports CSV         Ports that must be free before read (default: $WAIT_PORTS)

Examples:
  $(basename "$0") --image ghcr.io/dell/storage-performance-tool:spt_dev --skip-image-pull
  $(basename "$0") --s3-driver aws --threads 16 --cleanup
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

[ -n "$SPT_IMAGE" ] && export SPT_IMAGE
if [ "$SKIP_IMAGE_PULL" = true ]; then
  export SPT_SKIP_IMAGE_PULL=true
fi

RUN_ID="$(date -u +%Y%m%d.%H%M%S)"
SEED_LABEL="${LABEL_PREFIX}-seed-${RUN_ID}"
READ_LABEL="${LABEL_PREFIX}-read-${RUN_ID}"

add_common_args() {
  local -n cmd_ref=$1
  cmd_ref+=(
    --access-key "$S3_ACCESS_KEY"
    --secret-key "$S3_SECRET_KEY"
    --test-hosts "$HOSTS"
    --bucket "$S3_BUCKET"
    --threads "$THREADS"
    --object-size "$OBJECT_SIZE"
    --min-hosts "$MIN_HOSTS"
    --results-dir "$RESULTS_DIR"
  )
  if [ -n "$S3_ENDPOINTS" ]; then
    cmd_ref+=(--endpoints "$S3_ENDPOINTS")
  elif [ -n "$S3_ENDPOINT" ]; then
    cmd_ref+=(--endpoint "$S3_ENDPOINT")
  fi
  if [ "$S3_DRIVER" != "default" ]; then
    cmd_ref+=(--s3-driver "$S3_DRIVER")
  fi
  $FORCE && cmd_ref+=(--force) || true
  $VERBOSE && cmd_ref+=(--verbose) || true
  $KEEP_SCENARIO && cmd_ref+=(--keep-scenario) || true
  $ATTACH_EXISTING && cmd_ref+=(--attach-existing) || true
}

latest_matching_dir() {
  local pattern=$1
  local matches=()
  shopt -s nullglob
  matches=( $pattern )
  shopt -u nullglob
  if [ ${#matches[@]} -eq 0 ]; then
    return 1
  fi
  local newest=${matches[0]}
  local candidate
  for candidate in "${matches[@]}"; do
    if [[ "$candidate" > "$newest" ]]; then
      newest=$candidate
    fi
  done
  printf '%s\n' "$newest"
}

find_items_file() {
  local dir=$1
  local matches=()
  shopt -s nullglob
  matches=( "$dir"/*.items.csv "$dir"/*items.csv )
  shopt -u nullglob
  if [ ${#matches[@]} -eq 0 ]; then
    return 1
  fi
  printf '%s\n' "${matches[0]}"
}

trim_host() {
  local value=$1
  value="${value//$'\n'/}"
  value="${value//$'\t'/}"
  value="${value//$'\r'/}"
  value="${value// /}"
  printf '%s\n' "$value"
}

is_local_host() {
  local target=$1
  local host_no_user=${target#*@}
  [[ "$host_no_user" == "127.0.0.1" || "$host_no_user" == "localhost" || "$host_no_user" == "::1" ]]
}

port_open_on_host() {
  local target=$1
  local port=$2
  if is_local_host "$target"; then
    bash -c ": >/dev/tcp/127.0.0.1/$port" >/dev/null 2>&1
    return $?
  fi
  ssh $SSH_OPTS "$target" "bash -lc ': >/dev/tcp/127.0.0.1/$port'" >/dev/null 2>&1
}

busy_spt_ports() {
  local busy=()
  local host_entries=()
  local port_entries=()
  local raw_host target raw_port port
  IFS=',' read -r -a host_entries <<<"$HOSTS"
  IFS=',' read -r -a port_entries <<<"$WAIT_PORTS"
  for raw_host in "${host_entries[@]}"; do
    target=$(trim_host "$raw_host")
    [ -z "$target" ] && continue
    for raw_port in "${port_entries[@]}"; do
      port=$(trim_host "$raw_port")
      [ -z "$port" ] && continue
      if port_open_on_host "$target" "$port"; then
        busy+=("$target:$port")
      fi
    done
  done
  if [ ${#busy[@]} -gt 0 ]; then
    printf '%s\n' "${busy[*]}"
    return 0
  fi
  return 1
}

wait_for_seed_shutdown() {
  if [ "$WAIT_FOR_SHUTDOWN" != true ]; then
    return 0
  fi
  local deadline=$(( $(date +%s) + WAIT_TIMEOUT_SECONDS ))
  local busy=""
  echo "Waiting for seed containers to release ports ($WAIT_PORTS) on all hosts..."
  while true; do
    if ! busy=$(busy_spt_ports); then
      echo "Seed containers have released required ports."
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "Timed out waiting for seed containers to release ports: $busy" >&2
      echo "Try increasing --wait-timeout or inspect/stop lingering containers before retrying." >&2
      return 1
    fi
    echo "Still waiting; busy ports: $busy"
    sleep "$WAIT_POLL_SECONDS"
  done
}

echo "=== Seeded Read Test ==="
echo "Hosts: $HOSTS (min-hosts: $MIN_HOSTS)"
if [ -n "$S3_ENDPOINTS" ]; then
  echo "Endpoints: $S3_ENDPOINTS"
else
  echo "Endpoint: $S3_ENDPOINT"
fi
echo "Bucket: $S3_BUCKET"
echo "Threads: $THREADS  ObjectSize: $OBJECT_SIZE  SeedObjects: $OBJECT_COUNT  ReadCount: $READ_COUNT"
echo "Results: $RESULTS_DIR"
if [ "$S3_DRIVER" != "default" ]; then
  echo "S3 Driver: $S3_DRIVER"
fi
[ -n "$SPT_IMAGE" ] && echo "Image: $SPT_IMAGE"
[ "$SKIP_IMAGE_PULL" = true ] && echo "Skip image pull: true"
$CLEANUP && echo "Cleanup: read phase will delete seeded objects" || echo "Cleanup: disabled"
$WAIT_FOR_SHUTDOWN && echo "Read start wait: ports $WAIT_PORTS free on all hosts (timeout ${WAIT_TIMEOUT_SECONDS}s)" || echo "Read start wait: disabled"
echo

seed_cmd=("$ROOT_DIR"/spt --debug run write --headless
  --label "$SEED_LABEL"
  --object-count "$OBJECT_COUNT"
  --save-items
)
add_common_args seed_cmd

echo "=== Phase 1/2: seed $OBJECT_COUNT objects and save items.csv ==="
"${seed_cmd[@]}"

SEED_DIR=$(latest_matching_dir "$RESULTS_DIR/$SEED_LABEL-*") || {
  echo "Could not find seed results directory matching $RESULTS_DIR/$SEED_LABEL-*" >&2
  exit 1
}
ITEMS_FILE=$(find_items_file "$SEED_DIR") || {
  echo "Could not find saved items.csv artifact in $SEED_DIR" >&2
  exit 1
}

echo
echo "Saved items file: $ITEMS_FILE"
wait_for_seed_shutdown
echo

read_cmd=("$ROOT_DIR"/spt --debug run read --headless
  --label "$READ_LABEL"
  --object-count "$READ_COUNT"
  --items-file "$ITEMS_FILE"
)
add_common_args read_cmd
$CLEANUP && read_cmd+=(--cleanup) || true

echo "=== Phase 2/2: read back $READ_COUNT objects from saved items.csv ==="
"${read_cmd[@]}"

echo
echo "Seed results: $SEED_DIR"
READ_DIR=$(latest_matching_dir "$RESULTS_DIR/$READ_LABEL-*") || true
if [ -n "${READ_DIR:-}" ]; then
  echo "Read results: $READ_DIR"
fi
echo "=== Seeded Read Test Complete ==="
