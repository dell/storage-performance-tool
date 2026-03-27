#!/usr/bin/env bash
#
# MPU test script — multipart upload write-then-read workflow:
#   1. Seed phase writes objects via MPU (multipart upload)
#   2. Read phase downloads those objects via parallel Range GET requests
#
# Both phases use the same --item-data-ranges-threshold (part size), matching
# mpuload's behavior where -chunk_mbs governs both write parts and read ranges.
#
# This exercises the MPUGAP-1 feature: when --item-data-ranges-threshold is
# set, the engine splits each READ into multiple partial GET requests with
# Range headers, analogous to how multipart upload splits writes.
#
# Prerequisites:
#   - SPT distribution built (./gradlew :bundle:build)
#   - An S3-compatible endpoint reachable at S3_ENDPOINTS
#
# Usage:
#   ./tools/mputest.local.sh                 # seed + read (default)
#   ./tools/mputest.local.sh seed             # seed only
#   ./tools/mputest.local.sh read             # read only (objects must exist)
#   ./tools/mputest.local.sh cleanup          # delete seeded objects
#   ./tools/mputest.local.sh all              # seed + read + cleanup
#
#   # Override defaults via environment:
#   MPUTEST_OBJECT_SIZE=100MB MPUTEST_PART_SIZE=10MB \
#     ./tools/mputest.local.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"
RUN_SCRIPT="$ENGINE_ROOT/bundle/build/dist/run.sh"

# ---------------------------------------------------------------------------
# Environment: snapshot caller overrides, source .env, then restore overrides
# ---------------------------------------------------------------------------
declare -A _caller_env
for _v in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_AUTH_VERSION \
          MPUTEST_CONCURRENCY MPUTEST_OBJECT_SIZE MPUTEST_PART_SIZE \
          MPUTEST_SEED_COUNT MPUTEST_READ_COUNT MPUTEST_TIME_LIMIT; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

if [[ -f "$GIT_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$GIT_ROOT/.env"
elif [[ -f "$ENGINE_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ENGINE_ROOT/.env"
fi

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
: "${S3_ENDPOINTS:=http://127.0.0.1:9000}"
: "${S3_ACCESS_KEY:=AWS_ACCESS_KEY_ID_EXAMPLE}"
: "${S3_SECRET_KEY:=AWS_SECRET_ACCESS_KEY_EXAMPLE}"
: "${S3_BUCKET:=testmputest}"
: "${S3_AUTH_VERSION:=4}"

: "${MPUTEST_CONCURRENCY:=4}"
: "${MPUTEST_OBJECT_SIZE:=50MB}"
: "${MPUTEST_PART_SIZE:=5MB}"
: "${MPUTEST_SEED_COUNT:=10}"
: "${MPUTEST_READ_COUNT:=}"           # empty → use time limit instead
: "${MPUTEST_TIME_LIMIT:=30s}"

# ---------------------------------------------------------------------------
# Derived paths
# ---------------------------------------------------------------------------
NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

ITEMS_FILE="/tmp/spt/mputest-items.csv"

# ---------------------------------------------------------------------------
# Parse mode
# ---------------------------------------------------------------------------
MODE="${1:-both}"
shift 2>/dev/null || true

case "$MODE" in
  seed|read|both|cleanup|all) ;;
  -h|--help)
    cat <<'USAGE'
Usage: mputest.local.sh [seed|read|both|cleanup|all] [extra SPT args...]

Modes:
  seed     - Create objects (write phase only)
  read     - Read objects with parallel range-read (objects must exist)
  both     - Seed then read (default)
  cleanup  - Delete seeded objects
  all      - Seed, read, then cleanup

Environment variables:
  S3_ENDPOINTS              Comma-separated S3 endpoints  (default: http://127.0.0.1:9000)
  S3_ACCESS_KEY             Access key                    (default: example)
  S3_SECRET_KEY             Secret key                    (default: example)
  S3_BUCKET                 Bucket name                   (default: testmputest)
  MPUTEST_CONCURRENCY       Parallel operations           (default: 4)
  MPUTEST_OBJECT_SIZE       Object size                   (default: 50MB)
  MPUTEST_PART_SIZE         MPU/range-read part size      (default: 5MB, min 5MB for S3)
  MPUTEST_SEED_COUNT        Number of objects to seed     (default: 10)
  MPUTEST_READ_COUNT        Read op count (overrides time limit if set)
  MPUTEST_TIME_LIMIT        Read duration                 (default: 30s)
USAGE
    exit 0
    ;;
  *)
    echo "Unknown mode: $MODE (expected: seed, read, both, cleanup, or all)" >&2
    exit 1
    ;;
esac

# ---------------------------------------------------------------------------
# Ensure the distribution is built
# ---------------------------------------------------------------------------
if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi

# ---------------------------------------------------------------------------
# Common args shared by every phase
# ---------------------------------------------------------------------------
CONN_ARGS=(
  --storage-driver-type=s3
  "--storage-net-node-addrs=${NODE_ADDRS}"
  "--storage-auth-version=${S3_AUTH_VERSION}"
  "--storage-auth-uid=${S3_ACCESS_KEY}"
  "--storage-auth-secret=${S3_SECRET_KEY}"
  "--storage-driver-limit-concurrency=${MPUTEST_CONCURRENCY}"
)

# Composite-data args: drive MPU writes (seed) and parallel range-reads.
# Not used for cleanup — DELETE operates on the whole object.
COMMON_ARGS=(
  "${CONN_ARGS[@]}"
  "--item-data-size=${MPUTEST_OBJECT_SIZE}"
  "--item-data-ranges-threshold=${MPUTEST_PART_SIZE}"
  --load-batch-size=1
)

# ---------------------------------------------------------------------------
print_header() {
  local label="$1"
  echo "" >&2
  echo "=== MPU Test: ${label} ===" >&2
  echo "Endpoints:   ${S3_ENDPOINTS}" >&2
  echo "Bucket:      ${S3_BUCKET}" >&2
  echo "Concurrency: ${MPUTEST_CONCURRENCY}" >&2
  echo "Object Size: ${MPUTEST_OBJECT_SIZE}" >&2
  echo "Part Size:   ${MPUTEST_PART_SIZE}" >&2
  echo "Seed Count:  ${MPUTEST_SEED_COUNT}" >&2
  if [[ -n "${MPUTEST_READ_COUNT}" ]]; then
    echo "Read Count:  ${MPUTEST_READ_COUNT}" >&2
  else
    echo "Read Time:   ${MPUTEST_TIME_LIMIT}" >&2
  fi
  echo "Items File:  ${ITEMS_FILE}" >&2
  echo "==========================================" >&2
}

# ---------------------------------------------------------------------------
# Phase: Seed (write objects via MPU)
# ---------------------------------------------------------------------------
run_seed() {
  print_header "Seed (MPU Write)"
  mkdir -p "$(dirname "$ITEMS_FILE")"
  # Truncate any existing items file so the read phase only sees freshly-seeded objects
  rm -f "$ITEMS_FILE"
  "$RUN_SCRIPT" \
    "${COMMON_ARGS[@]}" \
    "--item-output-path=/${S3_BUCKET}" \
    "--item-output-file=${ITEMS_FILE}" \
    "--load-op-limit-count=${MPUTEST_SEED_COUNT}" \
    --load-step-id=mputest-seed \
    "$@"
  local count
  count=$(wc -l < "$ITEMS_FILE" 2>/dev/null || echo 0)
  echo "Seed complete: ${count} items written to ${ITEMS_FILE}" >&2
}

# ---------------------------------------------------------------------------
# Phase: Read (parallel range-read)
# ---------------------------------------------------------------------------
run_read() {
  if [[ ! -f "$ITEMS_FILE" ]]; then
    echo "Error: items file not found at ${ITEMS_FILE}" >&2
    echo "Run the seed phase first, or set ITEMS_FILE to an existing CSV." >&2
    exit 1
  fi
  local count
  count=$(wc -l < "$ITEMS_FILE")
  echo "Using items file: ${ITEMS_FILE} (${count} items)" >&2
  print_header "Read (Range GET)"

  local read_limit_args=()
  if [[ -n "${MPUTEST_READ_COUNT}" ]]; then
    read_limit_args+=("--load-op-limit-count=${MPUTEST_READ_COUNT}")
  else
    read_limit_args+=("--load-step-limit-time=${MPUTEST_TIME_LIMIT}")
    # Recycle mode lets the engine loop over the item set for the full duration
    read_limit_args+=("--load-op-recycle-mode=true")
  fi

  "$RUN_SCRIPT" \
    "${COMMON_ARGS[@]}" \
    --load-op-type=read \
    "--item-input-file=${ITEMS_FILE}" \
    --load-step-id=mputest-read \
    "${read_limit_args[@]}" \
    "$@"
}

# ---------------------------------------------------------------------------
# Phase: Cleanup (delete objects)
# ---------------------------------------------------------------------------
run_cleanup() {
  if [[ ! -f "$ITEMS_FILE" ]]; then
    echo "Error: items file not found at ${ITEMS_FILE}" >&2
    echo "Nothing to clean up." >&2
    exit 1
  fi
  local count
  count=$(wc -l < "$ITEMS_FILE")
  echo "Deleting ${count} items from ${ITEMS_FILE}" >&2
  print_header "Cleanup (Delete)"
  "$RUN_SCRIPT" \
    "${CONN_ARGS[@]}" \
    --load-op-type=delete \
    "--item-input-file=${ITEMS_FILE}" \
    --load-step-id=mputest-cleanup \
    "$@"
  echo "Cleanup complete." >&2
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "$MODE" in
  seed)
    run_seed "$@"
    ;;
  read)
    run_read "$@"
    ;;
  both)
    run_seed "$@"
    echo "" >&2
    echo "========== Seed complete, starting read phase ==========" >&2
    run_read "$@"
    ;;
  cleanup)
    run_cleanup "$@"
    ;;
  all)
    run_seed "$@"
    echo "" >&2
    echo "========== Seed complete, starting read phase ==========" >&2
    run_read "$@"
    echo "" >&2
    echo "========== Read complete, starting cleanup phase ==========" >&2
    run_cleanup "$@"
    ;;
esac
