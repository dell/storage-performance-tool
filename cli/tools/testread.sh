#!/usr/bin/env bash
set -euo pipefail

# spt multi-host read test (headless). Loads defaults from repo-root .env.
#
# Seeds 1000 objects then reads them for 60 seconds (by default).
# RDMA is off by default; pass --use-rdma to enable.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Inputs (env or CLI)
HOSTS=${HOSTS:-"127.0.0.1"}
S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ENDPOINTS=${S3_ENDPOINTS:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
S3_BUCKET=${S3_BUCKET:-""}
THREADS=${THREADS:-8}
OBJECT_SIZE=${OBJECT_SIZE:-"1MB"}
DURATION=${DURATION:-"60s"}
OBJECT_COUNT=${OBJECT_COUNT:-""}
SEED_OBJECTS=${SEED_OBJECTS:-2500}
MIN_HOSTS=${MIN_HOSTS:-3}
VERBOSE=${VERBOSE:-true}
FORCE=${FORCE:-true}
CLEANUP=${CLEANUP:-true}
KEEP_SCENARIO=${KEEP_SCENARIO:-true}
ATTACH_EXISTING=${ATTACH_EXISTING:-false}

# RDMA (off by default; pass --use-rdma to enable)
USE_RDMA=${USE_RDMA:-false}
RDMA_FALLBACK=${RDMA_FALLBACK_ENABLED:-false}
RDMA_THRESHOLD=${RDMA_THRESHOLD_BYTES:-1048576}
RDMA_DEVICE=${RDMA_DEVICE:-"auto"}
RDMA_LOCAL_IP=${RDMA_LOCAL_IP:-""}
RDMA_LOG_LEVEL=${RDMA_LOG_LEVEL:-"WARN"}
RDMA_TIMEOUT=${RDMA_TIMEOUT_MS:-30000}

# Simple arg parsing (overrides env)
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
    --object-count) OBJECT_COUNT="${2:-}"; shift 2 ;;
    --duration) DURATION="${2:-}"; shift 2 ;;
    --seed-objects) SEED_OBJECTS="${2:-}"; shift 2 ;;
    --min-hosts) MIN_HOSTS="${2:-}"; shift 2 ;;
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
    --use-rdma) USE_RDMA=true; shift 1 ;;
    --no-rdma) USE_RDMA=false; shift 1 ;;
    --rdma-fallback) RDMA_FALLBACK=true; shift 1 ;;
    --no-rdma-fallback) RDMA_FALLBACK=false; shift 1 ;;
    --rdma-threshold) RDMA_THRESHOLD="${2:-}"; shift 2 ;;
    --rdma-device) RDMA_DEVICE="${2:-}"; shift 2 ;;
    --rdma-local-ip) RDMA_LOCAL_IP="${2:-}"; shift 2 ;;
    --rdma-log-level) RDMA_LOG_LEVEL="${2:-}"; shift 2 ;;
    --rdma-timeout|--rdma-timeout-ms) RDMA_TIMEOUT="${2:-}"; shift 2 ;;
    -h|--help)
      cat << EOF
Usage: $(basename "$0") [options]

Multi-host read test. Seeds objects then reads for a duration or count.
RDMA is off by default; pass --use-rdma to enable.

Connection:
  --hosts CSV              Comma-separated [user@]host list (env: HOSTS)
  --s3-endpoint URL        Single S3 endpoint (env: S3_ENDPOINT)
  --s3-endpoints CSV       Multiple S3 endpoints (env: S3_ENDPOINTS)
  --s3-access-key KEY      S3 access key (env: S3_ACCESS_KEY)
  --s3-secret-key KEY      S3 secret key (env: S3_SECRET_KEY)
  --s3-bucket NAME         S3 bucket (env: S3_BUCKET)

Workload:
  --threads N              Concurrency per node (default: $THREADS)
  --object-size SIZE       e.g. 1MB, 4MB (default: $OBJECT_SIZE)
  --seed-objects N          Objects to pre-create for reading (default: $SEED_OBJECTS)
  --duration DUR           Read phase duration, e.g. 60s, 5m (default: $DURATION)
  --object-count N         Read ops; only used if --duration is empty
  --min-hosts N            Minimum hosts required (default: $MIN_HOSTS)

RDMA:
  --use-rdma               Enable RDMA-accelerated S3 driver (default: off)
  --no-rdma                Disable RDMA (default)
  --[no-]rdma-fallback     Fall back to HTTP on RDMA failure (default: $RDMA_FALLBACK)
  --rdma-threshold SIZE    Min object size for RDMA, e.g. 0, 256KB, 1MB (default: $RDMA_THRESHOLD)
  --rdma-device NAME       Device name or 'auto' (default: $RDMA_DEVICE)
  --rdma-local-ip IP       Local RDMA interface IP (default: auto-detect)
  --rdma-log-level LEVEL   WARN, INFO, DEBUG, TRACE (default: $RDMA_LOG_LEVEL)
  --rdma-timeout-ms MS     RDMA op timeout in ms (default: $RDMA_TIMEOUT)

General:
  --[no-]verbose           Enable verbose output (default: $VERBOSE)
  --[no-]force             Stop conflicting containers (default: $FORCE)
  --[no-]cleanup           Delete seed objects after test (default: $CLEANUP)
  --[no-]keep-scenario     Keep generated scenario file (default: $KEEP_SCENARIO)
  --[no-]attach-existing   Reuse prestarted worker nodes (default: $ATTACH_EXISTING)

Examples:
  $(basename "$0")                                     # 2500 seed, 60s read, HTTP
  $(basename "$0") --use-rdma --rdma-threshold 0       # RDMA for all sizes
  $(basename "$0") --seed-objects 5000 --duration 5m   # larger seed, longer read
  $(basename "$0") --no-cleanup                        # keep seed data for reuse
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Display test parameters
echo "=== Read Test ==="
echo "Hosts: $HOSTS (min-hosts: $MIN_HOSTS)"
if [ -n "$S3_ENDPOINTS" ]; then
  echo "Endpoints: $S3_ENDPOINTS"
else
  echo "Endpoint: $S3_ENDPOINT"
fi
echo "Bucket: $S3_BUCKET"
if [ -n "$DURATION" ]; then
  RUN_SPEC="Duration: $DURATION"
else
  RUN_SPEC="ObjectCount: ${OBJECT_COUNT:-1000}"
fi
echo "Threads: $THREADS  ObjectSize: $OBJECT_SIZE  SeedObjects: $SEED_OBJECTS  $RUN_SPEC"
if $USE_RDMA; then
  echo "RDMA: enabled  fallback=$RDMA_FALLBACK  threshold=$RDMA_THRESHOLD  device=$RDMA_DEVICE"
else
  echo "RDMA: disabled (pass --use-rdma to enable)"
fi
echo

# Build command
cmd=("$ROOT_DIR"/spt run read \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --test-hosts "$HOSTS" \
  --bucket "$S3_BUCKET" \
  --threads "$THREADS" \
  --object-size "$OBJECT_SIZE" \
  --seed-objects "$SEED_OBJECTS" \
  --min-hosts "$MIN_HOSTS")

if [ -n "$S3_ENDPOINTS" ]; then
  cmd+=(--endpoints "$S3_ENDPOINTS")
elif [ -n "$S3_ENDPOINT" ]; then
  cmd+=(--endpoint "$S3_ENDPOINT")
fi

if [ -n "$DURATION" ]; then
  cmd+=(--duration "$DURATION")
elif [ -n "$OBJECT_COUNT" ]; then
  cmd+=(--object-count "$OBJECT_COUNT")
fi

# RDMA flags (only when enabled)
if $USE_RDMA; then
  cmd+=(--use-rdma \
    --rdma-threshold "$RDMA_THRESHOLD" \
    --rdma-device "$RDMA_DEVICE" \
    --rdma-log-level "$RDMA_LOG_LEVEL" \
    --rdma-timeout-ms "$RDMA_TIMEOUT")
  if [ -n "$RDMA_LOCAL_IP" ]; then
    cmd+=(--rdma-local-ip "$RDMA_LOCAL_IP")
  fi
  $RDMA_FALLBACK && cmd+=(--rdma-fallback) || cmd+=(--rdma-fallback=false)
fi

$FORCE && cmd+=(--force) || true
$VERBOSE && cmd+=(--verbose) || true
$CLEANUP && cmd+=(--cleanup) || true
$KEEP_SCENARIO && cmd+=(--keep-scenario) || true
$ATTACH_EXISTING && cmd+=(--attach-existing) || true

"${cmd[@]}"

echo
echo "=== Read Test Complete ==="
echo
