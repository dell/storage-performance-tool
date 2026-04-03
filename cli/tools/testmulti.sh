#!/usr/bin/env bash
set -euo pipefail

# spt multi-host S3 write test (TUI). Loads defaults from repo-root .env.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Inputs (env or CLI)
#   HOSTS          - CSV of user@host
#   S3_ENDPOINT    - S3 endpoint URL (single)
#   S3_ENDPOINTS   - CSV of S3 endpoint URLs (multi)
#   S3_ACCESS_KEY  - S3 access key
#   S3_SECRET_KEY  - S3 secret key
#   S3_BUCKET      - Target bucket name
#   THREADS        - Concurrency per node
#   OBJECT_SIZE    - e.g. 1MB
#   OBJECT_COUNT   - op count (ignored if DURATION set)
#   DURATION       - e.g. 5m (overrides OBJECT_COUNT when set)
#   VERBOSE        - true|false
#   FORCE          - true|false (stop conflicting containers)
#   CLEANUP        - true|false (delete created objects)
#   KEEP_SCENARIO  - true|false
#   MIN_HOSTS      - minimum hosts required (0 = all)

HOSTS=${HOSTS:-${HOSTS:-"127.0.0.1"}}
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
AUTO_TERMINATE_SECONDS=${AUTO_TERMINATE_SECONDS:-0}
ATTACH_EXISTING=${ATTACH_EXISTING:-false}
MIN_HOSTS=${MIN_HOSTS:-0}

# Storage driver selection (default, netty, aws, rdma)
S3_DRIVER=${SPT_S3_DRIVER:-"default"}

# Simple arg parsing (overrides env)
while [ $# -gt 0 ]; do
  case "$1" in
    --mock) MOCK=true; shift 1 ;;
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
    --verbose) VERBOSE=true; shift 1 ;;
    --quiet|--no-verbose) VERBOSE=false; shift 1 ;;
    --force) FORCE=true; shift 1 ;;
    --no-force) FORCE=false; shift 1 ;;
    --cleanup) CLEANUP=true; shift 1 ;;
    --no-cleanup) CLEANUP=false; shift 1 ;;
    --keep-scenario) KEEP_SCENARIO=true; shift 1 ;;
    --no-keep-scenario) KEEP_SCENARIO=false; shift 1 ;;
    --auto-terminate-seconds) AUTO_TERMINATE_SECONDS="${2:-0}"; shift 2 ;;
    --min-hosts) MIN_HOSTS="${2:-0}"; shift 2 ;;
    --attach-existing) ATTACH_EXISTING=true; shift 1 ;;
    --no-attach-existing) ATTACH_EXISTING=false; shift 1 ;;
    --s3-driver) S3_DRIVER="${2:-}"; shift 2 ;;
    -h|--help)
      cat << EOF
Usage: $(basename "$0") [options]
  --hosts CSV              Comma-separated [user@]host list
  --mock                   Run a mock workload (no S3 endpoint required)
  --s3-endpoint URL        Single S3 endpoint (env: S3_ENDPOINT)
  --s3-endpoints CSV       Multiple S3 endpoints (env: S3_ENDPOINTS)
  --s3-access-key KEY      S3 access key (env: S3_ACCESS_KEY)
  --s3-secret-key KEY      S3 secret key (env: S3_SECRET_KEY)
  --s3-bucket NAME         S3 bucket (env: S3_BUCKET)
  --threads N              Concurrency per node (default: $THREADS)
  --object-size SIZE       e.g. 1MB (default: $OBJECT_SIZE)
  --object-count N         operations (default: $OBJECT_COUNT)
  --duration DUR           e.g. 5m; overrides object-count
  --[no-]verbose           enable/disable verbose flag (default: $VERBOSE)
  --auto-terminate-seconds N  Auto-terminate after N seconds (0=unlimited)
  --min-hosts N            Minimum hosts required to proceed (env: MIN_HOSTS, default: all)
  --[no-]force             stop conflicting containers (default: $FORCE)
  --[no-]cleanup           delete created objects (default: $CLEANUP)
  --[no-]keep-scenario     keep generated scenario (default: $KEEP_SCENARIO)
  --[no-]attach-existing   reuse prestarted worker nodes (default: $ATTACH_EXISTING)
  --s3-driver TYPE         Storage driver: default, aws, rdma (env: SPT_S3_DRIVER, default: $S3_DRIVER)

Tips:
  - Target multiple endpoints with --s3-endpoints or env S3_ENDPOINTS.
  - For distributed runs, consider adding: --slice-endpoints
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if $MOCK; then
  echo "=== Run Mock test (TUI) ==="
  echo "Hosts: $HOSTS"
else
  echo "=== Run S3 Write test (TUI) ==="
  echo "Hosts: $HOSTS"
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
if [ "$S3_DRIVER" != "default" ]; then
  echo "S3 Driver: $S3_DRIVER"
fi
if [[ "$AUTO_TERMINATE_SECONDS" =~ ^[0-9]+$ ]] && [ "$AUTO_TERMINATE_SECONDS" -gt 0 ]; then
  echo "Auto-terminate: ${AUTO_TERMINATE_SECONDS}s"
fi
echo

if $MOCK; then
  cmd=("$ROOT_DIR"/spt --debug run mock \
    --test-hosts "$HOSTS" \
    --threads "$THREADS" \
    --object-size "$OBJECT_SIZE")
else
  cmd=("$ROOT_DIR"/spt --debug run write \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --test-hosts "$HOSTS" \
    --bucket "$S3_BUCKET" \
    --threads "$THREADS" \
    --object-size "$OBJECT_SIZE")
  if [ -n "$S3_ENDPOINTS" ]; then
    cmd+=(--endpoints "$S3_ENDPOINTS")
  else
    cmd+=(--endpoint "$S3_ENDPOINT")
  fi
fi

# Storage driver (only pass when non-default to avoid requiring the flag on older builds)
if [ "$S3_DRIVER" != "default" ]; then
  cmd+=(--s3-driver "$S3_DRIVER")
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
$ATTACH_EXISTING && cmd+=(--attach-existing) || true

# Pass through auto-terminate if set (>0)
if [[ "$AUTO_TERMINATE_SECONDS" =~ ^[0-9]+$ ]] && [ "$AUTO_TERMINATE_SECONDS" -gt 0 ]; then
  cmd+=(--auto-terminate-seconds "$AUTO_TERMINATE_SECONDS")
fi

# Pass through min-hosts if set (>0)
if [[ "$MIN_HOSTS" =~ ^[0-9]+$ ]] && [ "$MIN_HOSTS" -gt 0 ]; then
  cmd+=(--min-hosts "$MIN_HOSTS")
fi

"${cmd[@]}"

echo
echo "=== Test Complete ==="
echo
