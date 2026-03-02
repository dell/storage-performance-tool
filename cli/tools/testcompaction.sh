#!/usr/bin/env bash
set -euo pipefail

# spt S3 Tables compaction test (headless, CLI path).
#
# Runs the three-phase compaction test vector via the spt CLI (Docker):
#   1. provision  — idempotent CreateTableBucket + Namespace + Table
#   2. seed       — tableWrite until --total-ingest bytes
#   3. compaction — tableCompactionPoll until file count converges
#
# Usage:
#   ./tools/testcompaction.sh                     # defaults from .env
#   ./tools/testcompaction.sh --smoketest         # 5-op quick end-to-end check
#   ./tools/testcompaction.sh --no-provision      # reuse existing table resources
#   ./tools/testcompaction.sh --generate-only     # print scenario without running
#   LOG_LEVEL=debug ./tools/testcompaction.sh     # verbose engine output
#
# Environment (or .env):
#   S3_ENDPOINT            S3/Tables endpoint URL  (required)
#   S3_ACCESS_KEY          Access key              (required)
#   S3_SECRET_KEY          Secret key              (required)
#   TABLE_BUCKET           Table bucket name       (default: spt-tables)
#   NAMESPACE              Namespace               (default: default)
#   TABLE_NAME             Table name              (default: spt-bench)
#   CONCURRENT_WRITERS     Seed-phase writers      (default: 4)
#   INGEST_FILE_SIZE       Small file size         (default: 100KB)
#   TARGET_FILE_SIZE       Compaction target size  (default: 64MB)
#   TOTAL_INGEST           Total bytes to seed     (default: 1GB)
#   COMPACTION_TIMEOUT     Max wait for convergence (default: 4h)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
TABLE_BUCKET=${TABLE_BUCKET:-"spt-tables"}
NAMESPACE=${NAMESPACE:-"default"}
TABLE_NAME=${TABLE_NAME:-"spt-bench"}
CONCURRENT_WRITERS=${CONCURRENT_WRITERS:-4}
INGEST_FILE_SIZE=${INGEST_FILE_SIZE:-"100KB"}
TARGET_FILE_SIZE=${TARGET_FILE_SIZE:-"64MB"}
TOTAL_INGEST=${TOTAL_INGEST:-"1GB"}
COMPACTION_TIMEOUT=${COMPACTION_TIMEOUT:-"4h"}
AUTO_TERMINATE=${AUTO_TERMINATE:-14400}
GENERATE_ONLY=${GENERATE_ONLY:-false}
NO_PROVISION=${NO_PROVISION:-false}
VERBOSE=${VERBOSE:-true}
FORCE=${FORCE:-true}

SMOKETEST=false

while [ $# -gt 0 ]; do
  case "$1" in
    --smoketest)           SMOKETEST=true;                 shift ;;
    --no-provision)        NO_PROVISION=true;              shift ;;
    --s3-endpoint)         S3_ENDPOINT="${2:-}";           shift 2 ;;
    --s3-access-key)       S3_ACCESS_KEY="${2:-}";         shift 2 ;;
    --s3-secret-key)       S3_SECRET_KEY="${2:-}";         shift 2 ;;
    --table-bucket)        TABLE_BUCKET="${2:-}";          shift 2 ;;
    --namespace)           NAMESPACE="${2:-}";             shift 2 ;;
    --table-name)          TABLE_NAME="${2:-}";            shift 2 ;;
    --concurrent-writers)  CONCURRENT_WRITERS="${2:-}";    shift 2 ;;
    --ingest-file-size)    INGEST_FILE_SIZE="${2:-}";      shift 2 ;;
    --target-file-size)    TARGET_FILE_SIZE="${2:-}";      shift 2 ;;
    --total-ingest)        TOTAL_INGEST="${2:-}";          shift 2 ;;
    --compaction-timeout)  COMPACTION_TIMEOUT="${2:-}";    shift 2 ;;
    --auto-terminate-seconds) AUTO_TERMINATE="${2:-}";     shift 2 ;;
    --generate-only)       GENERATE_ONLY=true;             shift ;;
    --verbose)             VERBOSE=true;                   shift ;;
    --no-verbose)          VERBOSE=false;                  shift ;;
    --force)               FORCE=true;                     shift ;;
    --no-force)            FORCE=false;                    shift ;;
    -h|--help)
      cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --smoketest                Quick end-to-end check: 5 seed ops, 60s compaction timeout
  --no-provision             Skip table bucket/namespace/table creation (reuse existing)
  --s3-endpoint URL          S3/Tables endpoint      (env: S3_ENDPOINT)
  --s3-access-key KEY        Access key              (env: S3_ACCESS_KEY)
  --s3-secret-key KEY        Secret key              (env: S3_SECRET_KEY)
  --table-bucket NAME        Table bucket name       (env: TABLE_BUCKET,    default: spt-tables)
  --namespace NS             Namespace               (env: NAMESPACE,       default: default)
  --table-name NAME          Table name              (env: TABLE_NAME,      default: spt-bench)
  --concurrent-writers N     Seed-phase writers      (env: CONCURRENT_WRITERS, default: 4)
  --ingest-file-size SIZE    Small file size         (env: INGEST_FILE_SIZE,   default: 100KB)
  --target-file-size SIZE    Compaction target size  (env: TARGET_FILE_SIZE,   default: 64MB)
  --total-ingest SIZE        Total bytes to seed     (env: TOTAL_INGEST,       default: 1GB)
  --compaction-timeout DUR   Max wait for convergence (env: COMPACTION_TIMEOUT, default: 4h)
  --auto-terminate-seconds N Headless auto-terminate (default: 14400)
  --generate-only            Print scenario without running
  --[no-]verbose             Verbose output (default: true)
  --[no-]force               Auto-resolve port conflicts (default: true)
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ "$SMOKETEST" == "true" ]]; then
  # 5 seed ops: total = 5 × ingest-file-size (100KB default = 500KB)
  TOTAL_INGEST="500KB"
  CONCURRENT_WRITERS=1
  COMPACTION_TIMEOUT="60s"
  AUTO_TERMINATE=600
fi

echo "=== S3 Tables compaction test ==="
echo "Endpoint:          ${S3_ENDPOINT}"
echo "Table:             ${TABLE_BUCKET}/${NAMESPACE}/${TABLE_NAME}"
echo "Concurrent writers:${CONCURRENT_WRITERS}"
echo "Ingest file size:  ${INGEST_FILE_SIZE}"
echo "Target file size:  ${TARGET_FILE_SIZE}"
echo "Total ingest:      ${TOTAL_INGEST}"
echo "Compaction timeout:${COMPACTION_TIMEOUT}"
[[ "$SMOKETEST" == "true" ]] && echo "(smoketest mode)"
echo

cmd=("$ROOT_DIR"/spt --debug run tables --headless \
  --endpoint "$S3_ENDPOINT" \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --table-bucket "$TABLE_BUCKET" \
  --namespace "$NAMESPACE" \
  --table-name "$TABLE_NAME" \
  --test-vector compaction \
  --concurrent-writers "$CONCURRENT_WRITERS" \
  --ingest-file-size "$INGEST_FILE_SIZE" \
  --target-file-size "$TARGET_FILE_SIZE" \
  --total-ingest "$TOTAL_INGEST" \
  --compaction-timeout "$COMPACTION_TIMEOUT" \
  --auto-terminate-seconds "$AUTO_TERMINATE")

$GENERATE_ONLY  && cmd+=(--generate-only)  || true
$VERBOSE        && cmd+=(--verbose)        || true
$FORCE          && cmd+=(--force)          || true
$NO_PROVISION   && cmd+=(--no-provision)   || true
cmd+=(--skip-image-pull)

"${cmd[@]}"

echo
echo "=== Compaction test complete ==="
echo
