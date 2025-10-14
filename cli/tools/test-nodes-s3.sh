#!/usr/bin/env bash
set -euo pipefail

# Turnkey: drive a distributed S3 write->read smoke using remote workers.
# - Requires workers running in --run-node mode (see tools/nodes-up.sh).
# - Runs a local entry container with --network host.
# - Persists created item list to a bind mount so read reuses exact keys.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Simple args parsing (optional env overrides remain supported)
usage() {
  cat <<USAGE
Usage: $(basename "$0") [--keep-logs] [--mode write|read|both]

Environment overrides (see .env.example):
  HOSTS, SPT_IMAGE, RUN_PORT, RMI_PORT
  S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
  THREADS, OBJECT_SIZE, OBJECT_COUNT, WORK_DIR
  KEEP_LOGS, LOG_ROOT, ITEMS_BASENAME or ITEMS_FILE
USAGE
}

MODE=${MODE:-both}
KEEP_LOGS=${KEEP_LOGS:-false}
LOG_ROOT=${LOG_ROOT:-"$ROOT_DIR/tools/logs"}
ENTRY_REMOTE=${ENTRY_REMOTE:-true}
ENTRY_TARGET=${ENTRY_TARGET:-}
REMOTE_WORK_DIR=${REMOTE_WORK_DIR:-"/tmp/spt-work"}
ENTRY_CONTAINER_NAME=${ENTRY_CONTAINER_NAME:-spt-primary}

while [ $# -gt 0 ]; do
  case "$1" in
    --keep-logs) KEEP_LOGS=true; shift ;;
    --mode) MODE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

# Env/config (can be overridden via environment or .env)
PRIMARY_HOST=${PRIMARY_HOST:-}
WORKER_HOSTS=${WORKER_HOSTS:-}
if [ -z "$PRIMARY_HOST" ] && [ -n "${HOSTS:-}" ]; then
  # Fallback: first of HOSTS is primary, rest workers
  IFS=',' read -r PRIMARY_HOST _rest <<<"$HOSTS"
fi
if [ -z "$WORKER_HOSTS" ]; then
  if [ -n "${HOSTS:-}" ]; then
    WORKER_HOSTS="$HOSTS"
  else
    WORKER_HOSTS="root@spt-test-2.cec.delllabs.net,root@spt-test-3.cec.delllabs.net,root@spt-test-4.cec.delllabs.net"
  fi
fi
SPT_IMAGE=${SPT_IMAGE:-"dcr.octo.dell.com/spt/spt"}
RUN_PORT=${RUN_PORT:-9999}
RMI_PORT=${RMI_PORT:-1099}

S3_ENDPOINT=${S3_ENDPOINT:-"http://10.246.190.191:9000"}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
S3_BUCKET=${S3_BUCKET:-"testwrites"}

THREADS=${THREADS:-8}
OBJECT_SIZE=${OBJECT_SIZE:-"1MB"}
OBJECT_COUNT=${OBJECT_COUNT:-2500}

WORK_DIR=${WORK_DIR:-"$ROOT_DIR/tools/work"}

# Create a unique default items file name for each run
_ts="$(date -u +%Y%m%d-%H%M%S)"
_pid="$$"
_default_items_basename="items.${_ts}.${_pid}.csv"

# Allow override via ITEMS_BASENAME or ITEMS_FILE
ITEMS_BASENAME=${ITEMS_BASENAME:-"$_default_items_basename"}

# Derive container/host paths for items file
if [ -n "${ITEMS_FILE:-}" ]; then
  if [[ "$ITEMS_FILE" = /* ]]; then
    CONTAINER_ITEMS_FILE="$ITEMS_FILE"
    HOST_ITEMS_FILE="$WORK_DIR/${ITEMS_FILE#/work/}"
  else
    ITEMS_BASENAME="$ITEMS_FILE"
    CONTAINER_ITEMS_FILE="/work/$ITEMS_BASENAME"
    HOST_ITEMS_FILE="$WORK_DIR/$ITEMS_BASENAME"
  fi
else
  CONTAINER_ITEMS_FILE="/work/$ITEMS_BASENAME"
  HOST_ITEMS_FILE="$WORK_DIR/$ITEMS_BASENAME"
fi

MODE=${MODE:-both}   # write|read|both

if [ -z "$S3_ACCESS_KEY" ] || [ -z "$S3_SECRET_KEY" ]; then
  echo "ERROR: S3_ACCESS_KEY and S3_SECRET_KEY must be set (see .env.example)" >&2
  exit 1
fi

# Ensure work dir exists and is writable by the container user
mkdir -p "$WORK_DIR"
chmod 0777 "$WORK_DIR" || true

# Parse S3 endpoint into host/port/ssl
proto="${S3_ENDPOINT%%://*}"
rest="${S3_ENDPOINT#*://}"
hostport="${rest%%/*}"
host="${hostport%%:*}"
port="${hostport##*:}"
if [ "$hostport" = "$port" ]; then
  if [ "$proto" = "https" ]; then port=443; else port=80; fi
fi
ssl_flag=""
if [ "$proto" = "https" ]; then ssl_flag="--storage-net-ssl-enabled=true"; fi

# Safety: ensure PRIMARY_HOST is not in WORKER_HOSTS
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

# Build RMI node list from WORKER_HOSTS (strip user@)
IFS=',' read -r -a HOST_ARR <<<"$WORKER_HOSTS"
NODE_ADDRS=()
for target in "${HOST_ARR[@]}"; do
  t="${target//[$'\n\t\r ']}"; [ -z "$t" ] && continue
  if [[ "$t" == *"@"* ]]; then
    NODE="${t#*@}"
  else
    NODE="$t"
  fi
  NODE_ADDRS+=("${NODE}:${RMI_PORT}")
done
NODE_ADDRS_CSV=$(IFS=','; echo "${NODE_ADDRS[*]}")

echo "Entry: using workers at: $NODE_ADDRS_CSV"
echo "S3: $S3_ENDPOINT bucket=$S3_BUCKET threads=$THREADS size=$OBJECT_SIZE count=$OBJECT_COUNT"
echo "Items file: $HOST_ITEMS_FILE (container: $CONTAINER_ITEMS_FILE)"

# Prepare run log directory if logging is enabled
RUN_LOG_DIR="${LOG_ROOT}/run.${_ts}.${_pid}"
if [ "$KEEP_LOGS" = true ]; then
  mkdir -p "$RUN_LOG_DIR"
  echo "Logs: $RUN_LOG_DIR"
fi

mask_args_for_log() {
  local out=()
  for a in "$@"; do
    case "$a" in
      --storage-auth-secret=*) out+=("--storage-auth-secret=****");;
      *) out+=("$a");;
    esac
  done
  printf '%s ' "${out[@]}"
}

run_entry_local() {
  local mode="$1"; shift || true
  echo "> docker run [$mode] $(mask_args_for_log "$@")"
  if [ "$KEEP_LOGS" = true ]; then
    # shellcheck disable=SC2086
    docker run --rm --network host -v "$WORK_DIR":/work "$SPT_IMAGE" "$@" \
      2>&1 | tee -a "$RUN_LOG_DIR/entry-${mode}.log"
  else
    # shellcheck disable=SC2086
    docker run --rm --network host -v "$WORK_DIR":/work "$SPT_IMAGE" "$@"
  fi
}

run_entry_remote() {
  local mode="$1"; shift || true
  # default entry target is the first HOSTS entry
  if [ -z "$ENTRY_TARGET" ]; then
    ENTRY_TARGET="$PRIMARY_HOST"
  fi
  echo "> [remote $ENTRY_TARGET] docker run [$mode] $(mask_args_for_log "$@")"
  # Ensure remote work dir exists and is writable
  ssh ${SSH_OPTS:-} "$ENTRY_TARGET" "mkdir -p '$REMOTE_WORK_DIR' && chmod 0777 '$REMOTE_WORK_DIR'" >/dev/null 2>&1 || true

  # Run remotely with fixed container name so nodes-status can see it
  if [ "$KEEP_LOGS" = true ]; then
    ssh ${SSH_OPTS:-} "$ENTRY_TARGET" ENTRY_CONTAINER_NAME="$ENTRY_CONTAINER_NAME" REMOTE_WORK_DIR="$REMOTE_WORK_DIR" SPT_IMAGE="$SPT_IMAGE" bash -s -- "$@" <<'EOS' \
      | tee -a "$RUN_LOG_DIR/entry-${mode}.log"
set -euo pipefail
docker rm -f "$ENTRY_CONTAINER_NAME" >/dev/null 2>&1 || true
docker run --rm --name "$ENTRY_CONTAINER_NAME" --network host -v "$REMOTE_WORK_DIR":/work "$SPT_IMAGE" "$@"
EOS
  else
    ssh ${SSH_OPTS:-} "$ENTRY_TARGET" ENTRY_CONTAINER_NAME="$ENTRY_CONTAINER_NAME" REMOTE_WORK_DIR="$REMOTE_WORK_DIR" SPT_IMAGE="$SPT_IMAGE" bash -s -- "$@" <<'EOS'
set -euo pipefail
docker rm -f "$ENTRY_CONTAINER_NAME" >/dev/null 2>&1 || true
docker run --rm --name "$ENTRY_CONTAINER_NAME" --network host -v "$REMOTE_WORK_DIR":/work "$SPT_IMAGE" "$@"
EOS
  fi
}

WRITE_ARGS=(
  "--load-step-node-addrs=$NODE_ADDRS_CSV"
  "--storage-driver-type=s3"
  "--storage-net-node-addrs=$host"
  "--storage-net-node-port=$port"
  $ssl_flag
  "--storage-auth-uid=$S3_ACCESS_KEY"
  "--storage-auth-secret=$S3_SECRET_KEY"
  "--item-output-path=/$S3_BUCKET"
  "--item-output-file=$CONTAINER_ITEMS_FILE"
  "--load-op-type=create"
  "--storage-driver-limit-concurrency=$THREADS"
  "--item-data-size=$OBJECT_SIZE"
  "--load-op-limit-count=$OBJECT_COUNT"
)

READ_ARGS=(
  "--load-step-node-addrs=$NODE_ADDRS_CSV"
  "--storage-driver-type=s3"
  "--storage-net-node-addrs=$host"
  "--storage-net-node-port=$port"
  $ssl_flag
  "--storage-auth-uid=$S3_ACCESS_KEY"
  "--storage-auth-secret=$S3_SECRET_KEY"
  "--item-input-file=$CONTAINER_ITEMS_FILE"
  "--item-input-path=/$S3_BUCKET"
  "--load-op-type=read"
  "--storage-driver-limit-concurrency=$THREADS"
  "--item-data-size=$OBJECT_SIZE"
  "--load-op-limit-count=$OBJECT_COUNT"
)

case "$MODE" in
  write)
    if [ "$ENTRY_REMOTE" = true ]; then
      run_entry_remote write "${WRITE_ARGS[@]}"
      # Fetch items file from remote entry host
      if [ -n "$ENTRY_TARGET" ]; then
        scp -q ${SSH_OPTS//-o /-o} "$ENTRY_TARGET:$REMOTE_WORK_DIR/$(basename "$HOST_ITEMS_FILE")" "$HOST_ITEMS_FILE" || true
      fi
    else
      run_entry_local write "${WRITE_ARGS[@]}"
    fi
    ;;
  read)
    if [ ! -s "$HOST_ITEMS_FILE" ]; then
      # Try to use the most recent items.*.csv from WORK_DIR
      latest=$(ls -1t "$WORK_DIR"/items.*.csv 2>/dev/null | head -n1 || true)
      if [ -n "$latest" ]; then
        HOST_ITEMS_FILE="$latest"
        CONTAINER_ITEMS_FILE="/work/$(basename "$latest")"
        # Patch the args for the detected file
        for i in "${!READ_ARGS[@]}"; do
          case "${READ_ARGS[$i]}" in
            --item-input-file=*) READ_ARGS[$i]="--item-input-file=$CONTAINER_ITEMS_FILE";;
          esac
        done
        echo "Using latest items file: $HOST_ITEMS_FILE"
      else
        echo "WARN: no items file found in $WORK_DIR; read will rely on bucket listing (may be slow)." >&2
      fi
    fi
    if [ "$ENTRY_REMOTE" = true ]; then
      run_entry_remote read "${READ_ARGS[@]}"
    else
      run_entry_local read "${READ_ARGS[@]}"
    fi
    ;;
  both)
    if [ "$ENTRY_REMOTE" = true ]; then
      run_entry_remote write "${WRITE_ARGS[@]}"
      if [ -n "$ENTRY_TARGET" ]; then
        scp -q ${SSH_OPTS//-o /-o} "$ENTRY_TARGET:$REMOTE_WORK_DIR/$(basename "$HOST_ITEMS_FILE")" "$HOST_ITEMS_FILE" || true
      fi
      sleep 2
      run_entry_remote read "${READ_ARGS[@]}"
    else
      run_entry_local write "${WRITE_ARGS[@]}"
      sleep 2
      run_entry_local read "${READ_ARGS[@]}"
    fi
    ;;
  *)
    echo "ERROR: MODE must be one of: write|read|both" >&2
    exit 1
    ;;
esac

echo "Done."
