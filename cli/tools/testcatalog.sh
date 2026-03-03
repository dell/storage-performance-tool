#!/usr/bin/env bash
set -euo pipefail

# spt S3 Tables catalog test (headless, CLI path).
#
# Runs the three-phase catalog test vector via the spt CLI (Docker):
#   1. provision   — idempotent CreateTableBucket + Namespace + Table
#   2. catalogSeed — bulk create namespaceCount × tablesPerNs tables
#   3. tableCatalog — concurrent GetTable/ListTables (50/50 random), duration-based
#
# Usage:
#   ./tools/testcatalog.sh                     # defaults from .env
#   ./tools/testcatalog.sh --smoketest         # 10ns × 10tbl seed, 30s read phase
#   ./tools/testcatalog.sh --no-provision      # reuse existing table resources
#   ./tools/testcatalog.sh --generate-only     # print scenario without running
#   LOG_LEVEL=debug ./tools/testcatalog.sh     # verbose engine output
#
# Environment (or .env):
#   S3_ENDPOINT            S3/Tables endpoint URL  (required)
#   S3_ACCESS_KEY          Access key              (required)
#   S3_SECRET_KEY          Secret key              (required)
#   TABLE_BUCKET           Table bucket name       (default: spt-tables)
#   NAMESPACE              Base namespace          (default: default)
#   NAMESPACE_COUNT        Number of namespaces    (default: 100)
#   TABLES_PER_NS          Tables per namespace    (default: 100)
#   READ_CONCURRENCY       Concurrent catalog readers (default: 100)
#   DURATION               Catalog read duration   (default: 5m)
#   CONCURRENT_WRITERS     Seed-phase writers      (default: 8)
#   AUTO_TERMINATE         Headless auto-terminate (default: 3600)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
TABLE_BUCKET=${TABLE_BUCKET:-"spt-tables"}
NAMESPACE=${NAMESPACE:-"default"}
NAMESPACE_COUNT=${NAMESPACE_COUNT:-100}
TABLES_PER_NS=${TABLES_PER_NS:-100}
READ_CONCURRENCY=${READ_CONCURRENCY:-10}
DURATION=${DURATION:-"5m"}
CONCURRENT_WRITERS=${CONCURRENT_WRITERS:-8}
AUTO_TERMINATE=${AUTO_TERMINATE:-3600}
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
    --namespace-count)     NAMESPACE_COUNT="${2:-}";       shift 2 ;;
    --tables-per-ns)       TABLES_PER_NS="${2:-}";         shift 2 ;;
    --read-concurrency)    READ_CONCURRENCY="${2:-}";      shift 2 ;;
    --duration)            DURATION="${2:-}";              shift 2 ;;
    --concurrent-writers)  CONCURRENT_WRITERS="${2:-}";    shift 2 ;;
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
  --smoketest                Quick end-to-end check: 10ns × 10tbl seed, 30s read phase
  --no-provision             Skip table bucket/namespace/table creation (reuse existing)
  --s3-endpoint URL          S3/Tables endpoint      (env: S3_ENDPOINT)
  --s3-access-key KEY        Access key              (env: S3_ACCESS_KEY)
  --s3-secret-key KEY        Secret key              (env: S3_SECRET_KEY)
  --table-bucket NAME        Table bucket name       (env: TABLE_BUCKET,      default: spt-tables)
  --namespace NS             Base namespace          (env: NAMESPACE,         default: default)
  --namespace-count N        Number of namespaces    (env: NAMESPACE_COUNT,   default: 100)
  --tables-per-ns N          Tables per namespace    (env: TABLES_PER_NS,     default: 100)
  --read-concurrency N       Concurrent readers      (env: READ_CONCURRENCY,  default: 10)
  --duration DUR             Catalog read duration   (env: DURATION,          default: 5m)
  --concurrent-writers N     Seed-phase writers      (env: CONCURRENT_WRITERS, default: 8)
  --auto-terminate-seconds N Headless auto-terminate (default: 3600)
  --generate-only            Print scenario without running
  --[no-]verbose             Verbose output (default: true)
  --[no-]force               Auto-resolve port conflicts (default: true)
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ "$SMOKETEST" == "true" ]]; then
  NAMESPACE_COUNT=10
  TABLES_PER_NS=10
  READ_CONCURRENCY=4
  DURATION="30s"
  CONCURRENT_WRITERS=4
  AUTO_TERMINATE=600
fi

echo "=== S3 Tables catalog test ==="
echo "Endpoint:          ${S3_ENDPOINT}"
echo "Table bucket:      ${TABLE_BUCKET}"
echo "Namespaces:        ${NAMESPACE_COUNT}"
echo "Tables per ns:     ${TABLES_PER_NS}"
echo "Total tables:      $((NAMESPACE_COUNT * TABLES_PER_NS))"
echo "Seed writers:      ${CONCURRENT_WRITERS}"
echo "Read concurrency:  ${READ_CONCURRENCY}"
echo "Read duration:     ${DURATION}"
[[ "$SMOKETEST" == "true" ]] && echo "(smoketest mode)"
echo

cmd=("$ROOT_DIR"/spt --debug run tables --headless \
  --endpoint "$S3_ENDPOINT" \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --table-bucket "$TABLE_BUCKET" \
  --namespace "$NAMESPACE" \
  --test-vector catalog \
  --namespace-count "$NAMESPACE_COUNT" \
  --tables-per-ns "$TABLES_PER_NS" \
  --read-concurrency "$READ_CONCURRENCY" \
  --concurrent-writers "$CONCURRENT_WRITERS" \
  --duration "$DURATION" \
  --auto-terminate-seconds "$AUTO_TERMINATE")

$GENERATE_ONLY  && cmd+=(--generate-only)  || true
$VERBOSE        && cmd+=(--verbose)        || true
$FORCE          && cmd+=(--force)          || true
$NO_PROVISION   && cmd+=(--no-provision)   || true
cmd+=(--skip-image-pull)

"${cmd[@]}"

echo
echo "=== Catalog test complete ==="
echo
