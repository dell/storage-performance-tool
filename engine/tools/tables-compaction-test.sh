#!/usr/bin/env bash
set -euo pipefail

# S3 Tables compaction benchmark (engine-direct).
#
# Runs three sequential phases against a live S3/Tables endpoint:
#   1. provision          — idempotent CreateTableBucket + Namespace + Table
#   2. tableWrite (seed)  — write small Parquet files to fill the table
#   3. tableCompactionPoll — trigger compaction and poll until convergence
#
# Usage:
#   ./tools/tables-compaction-test.sh                     # defaults from .env
#   ./tools/tables-compaction-test.sh --smoketest         # quick end-to-end (5 ops, LocalStack-friendly)
#   ./tools/tables-compaction-test.sh --skip-provision    # skip phase 1
#   ./tools/tables-compaction-test.sh --seed-only         # run phases 1+2 only
#   LOG_LEVEL=debug ./tools/tables-compaction-test.sh     # verbose engine output
#
# Environment (or .env):
#   S3_ENDPOINT                 S3/Tables endpoint URL  (required)
#   S3_ACCESS_KEY               Access key              (required)
#   S3_SECRET_KEY               Secret key              (required)
#   TABLE_BUCKET                Table bucket name       (default: spt-tables)
#   NAMESPACE                   Namespace               (default: default)
#   TABLE_NAME                  Table name              (default: spt-bench)
#   INGEST_FILE_SIZE_BYTES      Small file size for seed phase (default: 102400 = 100 KiB)
#   TARGET_FILE_SIZE_BYTES      Compaction target file size   (default: 67108864 = 64 MiB)
#   TOTAL_INGEST_BYTES          Total bytes to seed           (default: 104857600 = 100 MiB)
#   SEED_CONCURRENCY            Writers during seed phase     (default: 4)
#   COMMIT_FREQ_MS              Iceberg commit interval ms    (default: 500)
#   COMPACTION_POLL_INTERVAL_MS Poll interval during compaction (default: 5000)
#   COMPACTION_POLL_TIMEOUT_MS  Max wait for convergence ms   (default: 14400000 = 4 h)
#   COMPACTION_TOLERANCE_FACTOR File-count tolerance multiplier (default: 1.1)
#   COMPACTION_STABILIZATION_POLLS Consecutive stable polls required (default: 2)
#   LOG_LEVEL                   Engine log level              (default: info)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Save any caller-provided overrides before .env may clobber them
declare -A _cli_overrides=()
for _var in S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY \
            TABLE_BUCKET NAMESPACE TABLE_NAME \
            INGEST_FILE_SIZE_BYTES TARGET_FILE_SIZE_BYTES TOTAL_INGEST_BYTES \
            SEED_CONCURRENCY COMMIT_FREQ_MS \
            COMPACTION_POLL_INTERVAL_MS COMPACTION_POLL_TIMEOUT_MS \
            COMPACTION_TOLERANCE_FACTOR COMPACTION_STABILIZATION_POLLS \
            LOG_LEVEL; do
  if [[ -n "${!_var+set}" ]]; then
    _cli_overrides[$_var]="${!_var}"
  fi
done

if [[ -f "$REPO_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
fi

for _var in "${!_cli_overrides[@]}"; do
  export "$_var=${_cli_overrides[$_var]}"
done
unset _cli_overrides _var

# Defaults
: "${S3_ENDPOINT:=}"
: "${S3_ACCESS_KEY:=}"
: "${S3_SECRET_KEY:=}"
: "${TABLE_BUCKET:=spt-tables}"
: "${NAMESPACE:=default}"
: "${TABLE_NAME:=spt-bench}"
: "${INGEST_FILE_SIZE_BYTES:=102400}"
: "${TARGET_FILE_SIZE_BYTES:=67108864}"
: "${TOTAL_INGEST_BYTES:=104857600}"
: "${SEED_CONCURRENCY:=4}"
: "${COMMIT_FREQ_MS:=500}"
: "${COMPACTION_POLL_INTERVAL_MS:=5000}"
: "${COMPACTION_POLL_TIMEOUT_MS:=14400000}"
: "${COMPACTION_TOLERANCE_FACTOR:=1.1}"
: "${COMPACTION_STABILIZATION_POLLS:=2}"
: "${LOG_LEVEL:=info}"
: "${NET_TIMEOUT_MS:=30000}"

SKIP_PROVISION=false
SEED_ONLY=false
SMOKETEST=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-provision)        SKIP_PROVISION=true;            shift ;;
    --seed-only)             SEED_ONLY=true;                 shift ;;
    --smoketest)             SMOKETEST=true;                 shift ;;
    --s3-endpoint)           S3_ENDPOINT="${2:-}";           shift 2 ;;
    --s3-access-key)         S3_ACCESS_KEY="${2:-}";         shift 2 ;;
    --s3-secret-key)         S3_SECRET_KEY="${2:-}";         shift 2 ;;
    --table-bucket)          TABLE_BUCKET="${2:-}";          shift 2 ;;
    --namespace)             NAMESPACE="${2:-}";             shift 2 ;;
    --table-name)            TABLE_NAME="${2:-}";            shift 2 ;;
    --ingest-file-size)      INGEST_FILE_SIZE_BYTES="${2:-}"; shift 2 ;;
    --target-file-size)      TARGET_FILE_SIZE_BYTES="${2:-}"; shift 2 ;;
    --total-ingest)          TOTAL_INGEST_BYTES="${2:-}";    shift 2 ;;
    --seed-concurrency)      SEED_CONCURRENCY="${2:-}";      shift 2 ;;
    --commit-freq-ms)        COMMIT_FREQ_MS="${2:-}";        shift 2 ;;
    --poll-interval-ms)      COMPACTION_POLL_INTERVAL_MS="${2:-}"; shift 2 ;;
    --poll-timeout-ms)       COMPACTION_POLL_TIMEOUT_MS="${2:-}"; shift 2 ;;
    --tolerance-factor)      COMPACTION_TOLERANCE_FACTOR="${2:-}"; shift 2 ;;
    --stabilization-polls)   COMPACTION_STABILIZATION_POLLS="${2:-}"; shift 2 ;;
    --log-level)             LOG_LEVEL="${2:-}";             shift 2 ;;
    -h|--help)
      cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --skip-provision            Skip phase 1 (CreateTableBucket/Namespace/Table)
  --seed-only                 Run phases 1+2 only (no compaction poll)
  --smoketest                 Quick end-to-end check: 5 seed ops, concurrency=1, 60s poll timeout
  --s3-endpoint URL           S3/Tables endpoint      (env: S3_ENDPOINT)
  --s3-access-key KEY         Access key              (env: S3_ACCESS_KEY)
  --s3-secret-key KEY         Secret key              (env: S3_SECRET_KEY)
  --table-bucket NAME         Table bucket            (env: TABLE_BUCKET,   default: spt-tables)
  --namespace NS              Namespace               (env: NAMESPACE,      default: default)
  --table-name NAME           Table name              (env: TABLE_NAME,     default: spt-bench)
  --ingest-file-size BYTES    Seed Parquet file size  (env: INGEST_FILE_SIZE_BYTES, default: 102400)
  --target-file-size BYTES    Compaction target size  (env: TARGET_FILE_SIZE_BYTES, default: 67108864)
  --total-ingest BYTES        Total seed bytes        (env: TOTAL_INGEST_BYTES,     default: 104857600)
  --seed-concurrency N        Parallel writers        (env: SEED_CONCURRENCY,       default: 4)
  --commit-freq-ms MS         Iceberg commit interval (env: COMMIT_FREQ_MS,         default: 500)
  --poll-interval-ms MS       Compaction poll interval (env: COMPACTION_POLL_INTERVAL_MS, default: 5000)
  --poll-timeout-ms MS        Compaction poll timeout  (env: COMPACTION_POLL_TIMEOUT_MS,  default: 14400000)
  --tolerance-factor F        File-count tolerance    (env: COMPACTION_TOLERANCE_FACTOR, default: 1.1)
  --stabilization-polls N     Stable polls required   (env: COMPACTION_STABILIZATION_POLLS, default: 2)
  --log-level LEVEL           Engine log level        (env: LOG_LEVEL, default: info)
EOF
      exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ "$SMOKETEST" == "true" ]]; then
  TOTAL_INGEST_BYTES=$(( 5 * INGEST_FILE_SIZE_BYTES ))
  SEED_CONCURRENCY=1
  COMPACTION_POLL_TIMEOUT_MS=60000
  COMPACTION_POLL_INTERVAL_MS=3000
fi

if [[ -z "$S3_ENDPOINT" ]]; then
  echo "ERROR: S3_ENDPOINT is required (set in .env or pass --s3-endpoint)" >&2
  exit 1
fi
if [[ -z "$S3_ACCESS_KEY" || -z "$S3_SECRET_KEY" ]]; then
  echo "ERROR: S3_ACCESS_KEY and S3_SECRET_KEY are required" >&2
  exit 1
fi

RUN_SCRIPT="$REPO_ROOT/bundle/build/dist/run.sh"
if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Distribution not found at $RUN_SCRIPT; building..." >&2
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi

# Strip scheme for node address (SPT expects host:port)
NODE_ADDR="${S3_ENDPOINT#http://}"
NODE_ADDR="${NODE_ADDR#https://}"
# Determine if TLS is needed
SSL_ENABLED=false
[[ "$S3_ENDPOINT" == https://* ]] && SSL_ENABLED=true

log() { printf "[%s] %s\n" "$(date -u +%H:%M:%S)" "$*" >&2; }

# Common args shared across all phases
COMMON_ARGS=(
  "--storage-driver-type=s3-tables"
  "--storage-net-node-addrs=${NODE_ADDR}"
  "--storage-net-ssl-enabled=${SSL_ENABLED}"
  "--storage-auth-uid=${S3_ACCESS_KEY}"
  "--storage-auth-secret=${S3_SECRET_KEY}"
  "--storage-auth-version=4"
  "--storage-s3tables-controlPlaneEndpoint=${S3_ENDPOINT}"
  "--storage-s3tables-bucket=${TABLE_BUCKET}"
  "--storage-s3tables-namespace=${NAMESPACE}"
  "--storage-s3tables-tableName=${TABLE_NAME}"
  "--storage-s3tables-targetFileSizeBytes=${TARGET_FILE_SIZE_BYTES}"
  "--storage-s3tables-ingestFileSizeBytes=${INGEST_FILE_SIZE_BYTES}"
  "--storage-s3tables-totalIngestBytes=${TOTAL_INGEST_BYTES}"
  "--storage-s3tables-commitFreqMs=${COMMIT_FREQ_MS}"
  "--storage-s3tables-compactionPollIntervalMs=${COMPACTION_POLL_INTERVAL_MS}"
  "--storage-s3tables-compactionPollTimeoutMs=${COMPACTION_POLL_TIMEOUT_MS}"
  "--storage-s3tables-compactionToleranceFactor=${COMPACTION_TOLERANCE_FACTOR}"
  "--storage-s3tables-compactionStabilizationPolls=${COMPACTION_STABILIZATION_POLLS}"
  "--storage-net-timeoutMilliSec=${NET_TIMEOUT_MS}"
  "--log-level=${LOG_LEVEL}"
  "--load-batch-size=1"
)

echo "=== S3 Tables compaction benchmark ==="
echo "Endpoint:          ${S3_ENDPOINT}"
echo "Table:             ${TABLE_BUCKET}/${NAMESPACE}/${TABLE_NAME}"
echo "Ingest file size:  ${INGEST_FILE_SIZE_BYTES} bytes"
echo "Target file size:  ${TARGET_FILE_SIZE_BYTES} bytes"
echo "Total ingest:      ${TOTAL_INGEST_BYTES} bytes"
echo "Seed concurrency:  ${SEED_CONCURRENCY}"
echo "Tolerance factor:  ${COMPACTION_TOLERANCE_FACTOR}"
echo "Poll interval:     ${COMPACTION_POLL_INTERVAL_MS} ms"
echo "Poll timeout:      ${COMPACTION_POLL_TIMEOUT_MS} ms"
echo

# ---------------------------------------------------------------------------
# Phase 1: Provision
# ---------------------------------------------------------------------------
if [[ "$SKIP_PROVISION" == "false" ]]; then
  log "Phase 1: provision (CreateTableBucket + Namespace + Table)"
  "$RUN_SCRIPT" "${COMMON_ARGS[@]}" \
    "--storage-s3tables-opMode=provision" \
    "--storage-driver-limit-concurrency=1" \
    "--load-op-type=noop" \
    "--load-op-limit-count=1" \
    "--load-step-id=tables-provision"
  log "Phase 1 complete."
else
  log "Phase 1: skipped (--skip-provision)"
fi

# ---------------------------------------------------------------------------
# Phase 2: Seed (tableWrite)
# ---------------------------------------------------------------------------
SEED_OP_COUNT=$(( (TOTAL_INGEST_BYTES + INGEST_FILE_SIZE_BYTES - 1) / INGEST_FILE_SIZE_BYTES ))
log "Phase 2: seed (tableWrite, concurrency=${SEED_CONCURRENCY}, ops=${SEED_OP_COUNT}, total=${TOTAL_INGEST_BYTES} bytes)"
"$RUN_SCRIPT" "${COMMON_ARGS[@]}" \
  "--storage-s3tables-opMode=tableWrite" \
  "--storage-driver-limit-concurrency=${SEED_CONCURRENCY}" \
  "--load-op-type=noop" \
  "--load-op-limit-count=${SEED_OP_COUNT}" \
  "--load-step-id=tables-seed"
log "Phase 2 complete."

if [[ "$SEED_ONLY" == "true" ]]; then
  log "Seed-only mode — skipping compaction poll."
  echo "=== Seed complete ==="
  exit 0
fi

# ---------------------------------------------------------------------------
# Phase 3: Compaction poll (tableCompactionPoll)
# ---------------------------------------------------------------------------
log "Phase 3: compaction poll (trigger + poll for convergence)"
"$RUN_SCRIPT" "${COMMON_ARGS[@]}" \
  "--storage-s3tables-opMode=tableCompactionPoll" \
  "--storage-driver-limit-concurrency=1" \
  "--load-op-type=noop" \
  "--load-op-limit-count=1" \
  "--load-step-id=tables-compaction"
log "Phase 3 complete."

echo
echo "=== Compaction benchmark complete ==="
echo
