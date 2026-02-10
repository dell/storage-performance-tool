#!/usr/bin/env bash
set -euo pipefail

# spt single-host S3 write test (TUI). Loads defaults from repo-root .env.
#
# Usage:
#   ./tools/testlocal.sh                                 # defaults from .env
#   ./tools/testlocal.sh --use-rdma                      # enable RDMA passthrough
#   ./tools/testlocal.sh --threads 8 --object-size 4MB   # override defaults
#   S3_BUCKET=mybucket ./tools/testlocal.sh              # env override

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Inputs (env or CLI)
S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ENDPOINTS=${S3_ENDPOINTS:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
S3_BUCKET=${S3_BUCKET:-""}
THREADS=${THREADS:-4}
OBJECT_SIZE=${OBJECT_SIZE:-"1MB"}
OBJECT_COUNT=${OBJECT_COUNT:-1000}
DURATION=${DURATION:-""}
VERBOSE=${VERBOSE:-true}
FORCE=${FORCE:-true}
CLEANUP=${CLEANUP:-true}
KEEP_SCENARIO=${KEEP_SCENARIO:-true}
MOCK=${MOCK:-false}
USE_RDMA=${USE_RDMA:-false}

while [ $# -gt 0 ]; do
  case "$1" in
    --mock) MOCK=true; shift 1 ;;
    --use-rdma) USE_RDMA=true; shift 1 ;;
    --s3-endpoint) S3_ENDPOINT="${2:-}"; shift 2 ;;
    --s3-endpoints) S3_ENDPOINTS="${2:-}"; shift 2 ;;
    --s3-access-key) S3_ACCESS_KEY="${2:-}"; shift 2 ;;
    --s3-secret-key) S3_SECRET_KEY="${2:-}"; shift 2 ;;
    --s3-bucket) S3_BUCKET="${2:-}"; shift 2 ;;
    --threads) THREADS="${2:-}"; shift 2 ;;
    --object-size) OBJECT_SIZE="${2:-}"; shift 2 ;;
    --object-count) OBJECT_COUNT="${2:-}"; shift 2 ;;
    --duration) DURATION="${2:-}"; shift 2 ;;
    --verbose) VERBOSE=true; shift 1 ;;
    --quiet|--no-verbose) VERBOSE=false; shift 1 ;;
    --force) FORCE=true; shift 1 ;;
    --no-force) FORCE=false; shift 1 ;;
    --cleanup) CLEANUP=true; shift 1 ;;
    --no-cleanup) CLEANUP=false; shift 1 ;;
    --keep-scenario) KEEP_SCENARIO=true; shift 1 ;;
    --no-keep-scenario) KEEP_SCENARIO=false; shift 1 ;;
    -h|--help)
      cat << EOF
Usage: $(basename "$0") [options]
  --mock                   Run a mock workload (no S3 endpoint required)
  --use-rdma               Enable RDMA device passthrough in containers
  --s3-endpoint URL        Single S3 endpoint (env: S3_ENDPOINT)
  --s3-endpoints CSV       Multiple S3 endpoints (env: S3_ENDPOINTS)
  --s3-access-key KEY      S3 access key (env: S3_ACCESS_KEY)
  --s3-secret-key KEY      S3 secret key (env: S3_SECRET_KEY)
  --s3-bucket NAME         S3 bucket (env: S3_BUCKET)
  --threads N              Concurrency (default: $THREADS)
  --object-size SIZE       e.g. 1MB (default: $OBJECT_SIZE)
  --object-count N         Operations (default: $OBJECT_COUNT)
  --duration DUR           e.g. 5m; overrides object-count
  --[no-]verbose           Enable/disable verbose flag (default: $VERBOSE)
  --[no-]force             Stop conflicting containers (default: $FORCE)
  --[no-]cleanup           Delete created objects (default: $CLEANUP)
  --[no-]keep-scenario     Keep generated scenario (default: $KEEP_SCENARIO)

Environment:
  All --s3-* flags can be set via S3_* env vars or .env file.
  USE_RDMA=true is equivalent to --use-rdma.
  SPT_IMAGE overrides the Docker image (e.g. spt-rdma:dev).
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# RDMA: export SPT_RDMA so the CLI enables device passthrough in containers
if $USE_RDMA; then
  export SPT_RDMA=true
fi

if $MOCK; then
  echo "=== Run Mock test (TUI) ==="
else
  echo "=== Run S3 Write test (TUI) ==="
  if [ -n "$S3_ENDPOINTS" ]; then
    echo "Endpoints: $S3_ENDPOINTS"
  else
    echo "Endpoint: $S3_ENDPOINT"
  fi
  echo "Bucket: $S3_BUCKET"
fi
if [ -n "$DURATION" ]; then
  RUN_SPEC="Duration: $DURATION"
else
  RUN_SPEC="ObjectCount: $OBJECT_COUNT"
fi
echo "Threads: $THREADS  ObjectSize: $OBJECT_SIZE  $RUN_SPEC"
if $USE_RDMA; then
  echo "RDMA:    enabled (SPT_RDMA=true)"
  [[ -n "${SPT_IMAGE:-}" ]] && echo "Image:   $SPT_IMAGE"
fi
echo

if $MOCK; then
  cmd=("$ROOT_DIR"/spt --debug run mock \
    --threads "$THREADS" \
    --object-size "$OBJECT_SIZE")
else
  cmd=("$ROOT_DIR"/spt --debug run write \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket "$S3_BUCKET" \
    --threads "$THREADS" \
    --object-size "$OBJECT_SIZE")
  if [ -n "$S3_ENDPOINTS" ]; then
    cmd+=(--endpoints "$S3_ENDPOINTS")
  else
    cmd+=(--endpoint "$S3_ENDPOINT")
  fi
fi

if [ -n "$DURATION" ]; then
  cmd+=(--duration "$DURATION")
else
  cmd+=(--object-count "$OBJECT_COUNT")
fi

$FORCE && cmd+=(--force) || true
$VERBOSE && cmd+=(--verbose) || true
$CLEANUP && cmd+=(--cleanup) || true
$KEEP_SCENARIO && cmd+=(--keep-scenario) || true

"${cmd[@]}"

echo
echo "=== Test Complete ==="
echo
