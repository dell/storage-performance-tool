#!/usr/bin/env bash
set -euo pipefail

# spt S3 Tables test (headless). Loads defaults from repo-root .env.
#
# Usage:
#   ./tools/testtables.sh                               # defaults: tps, 10 writers, 30s
#   ./tools/testtables.sh --test-vector compaction      # compaction test vector
#   ./tools/testtables.sh --test-vector catalog         # catalog test vector
#   ./tools/testtables.sh --concurrent-writers 4 --duration 60s
#   ./tools/testtables.sh --generate-only               # print scenario without running

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
TABLE_BUCKET=${TABLE_BUCKET:-"spt-tables"}
NAMESPACE=${NAMESPACE:-"default"}
TEST_VECTOR=${TEST_VECTOR:-"tps"}
CONCURRENT_WRITERS=${CONCURRENT_WRITERS:-10}
DURATION=${DURATION:-"30s"}
TARGET_FILE_SIZE=${TARGET_FILE_SIZE:-"1MB"}
INGEST_FILE_SIZE=${INGEST_FILE_SIZE:-"100KB"}
TOTAL_INGEST=${TOTAL_INGEST:-"100MB"}
GENERATE_ONLY=${GENERATE_ONLY:-false}
AUTO_TERMINATE=${AUTO_TERMINATE:-120}
VERBOSE=${VERBOSE:-true}
FORCE=${FORCE:-true}

while [ $# -gt 0 ]; do
  case "$1" in
    --s3-endpoint)       S3_ENDPOINT="${2:-}";        shift 2 ;;
    --s3-access-key)     S3_ACCESS_KEY="${2:-}";      shift 2 ;;
    --s3-secret-key)     S3_SECRET_KEY="${2:-}";      shift 2 ;;
    --table-bucket)      TABLE_BUCKET="${2:-}";       shift 2 ;;
    --namespace)         NAMESPACE="${2:-}";          shift 2 ;;
    --test-vector)       TEST_VECTOR="${2:-}";        shift 2 ;;
    --concurrent-writers) CONCURRENT_WRITERS="${2:-}"; shift 2 ;;
    --duration)          DURATION="${2:-}";           shift 2 ;;
    --target-file-size)  TARGET_FILE_SIZE="${2:-}";   shift 2 ;;
    --ingest-file-size)  INGEST_FILE_SIZE="${2:-}";   shift 2 ;;
    --total-ingest)      TOTAL_INGEST="${2:-}";       shift 2 ;;
    --auto-terminate-seconds) AUTO_TERMINATE="${2:-}"; shift 2 ;;
    --generate-only)     GENERATE_ONLY=true;          shift 1 ;;
    --verbose)           VERBOSE=true;                shift 1 ;;
    --no-verbose)        VERBOSE=false;               shift 1 ;;
    --force)             FORCE=true;                  shift 1 ;;
    --no-force)          FORCE=false;                 shift 1 ;;
    -h|--help)
      cat << EOF
Usage: $(basename "$0") [options]
  --s3-endpoint URL          S3/Tables endpoint (env: S3_ENDPOINT)
  --s3-access-key KEY        Access key (env: S3_ACCESS_KEY)
  --s3-secret-key KEY        Secret key (env: S3_SECRET_KEY)
  --table-bucket NAME        Table bucket name (default: spt-tables)
  --namespace NS             Namespace (default: default)
  --test-vector VEC          tps | compaction | catalog (default: tps)
  --concurrent-writers N     Concurrent Iceberg writers (default: 10)
  --duration DUR             Test duration e.g. 30s, 5m (default: 30s)
  --target-file-size SIZE    Target Parquet file size (default: 1MB)
  --ingest-file-size SIZE    Small file size for compaction seed (default: 100KB)
  --total-ingest SIZE        Total ingest for compaction seed (default: 100MB)
  --auto-terminate-seconds N Headless auto-terminate timeout (default: 120)
  --generate-only            Print scenario without running
  --[no-]verbose             Verbose output (default: true)
  --[no-]force               Auto-resolve port conflicts (default: true)
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

echo "=== S3 Tables test (${TEST_VECTOR}) ==="
echo "Endpoint:          ${S3_ENDPOINT}"
echo "Table bucket:      ${TABLE_BUCKET}"
echo "Namespace:         ${NAMESPACE}"
echo "Concurrent writers:${CONCURRENT_WRITERS}"
echo "Duration:          ${DURATION}"
echo

cmd=("$ROOT_DIR"/spt --debug run tables --headless \
  --endpoint "$S3_ENDPOINT" \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --table-bucket "$TABLE_BUCKET" \
  --namespace "$NAMESPACE" \
  --test-vector "$TEST_VECTOR" \
  --concurrent-writers "$CONCURRENT_WRITERS" \
  --duration "$DURATION" \
  --target-file-size "$TARGET_FILE_SIZE" \
  --ingest-file-size "$INGEST_FILE_SIZE" \
  --total-ingest "$TOTAL_INGEST" \
  --auto-terminate-seconds "$AUTO_TERMINATE")

$GENERATE_ONLY && cmd+=(--generate-only) || true
$VERBOSE       && cmd+=(--verbose)       || true
$FORCE         && cmd+=(--force)         || true

"${cmd[@]}"

echo
echo "=== Test Complete ==="
echo
