#!/usr/bin/env bash

# Run a short, seeded standalone DELETE workload through the public SPT CLI.
# The command creates a run-owned object set, deletes its frozen inventory,
# optionally verifies absence, and makes a best-effort cleanup pass for residuals.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SPT_BIN=${SPT_BIN:-"$ROOT_DIR/spt"}

if [[ -f "$ROOT_DIR/.env" ]]; then
	# shellcheck disable=SC1090
	source "$ROOT_DIR/.env"
fi

run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
HOSTS=${HOSTS:-"127.0.0.1"}
MIN_HOSTS=${MIN_HOSTS:-1}
ENDPOINTS=${S3_ENDPOINTS:-${S3_ENDPOINT:-""}}
ACCESS_KEY=${S3_ACCESS_KEY:-""}
SECRET_KEY=${S3_SECRET_KEY:-""}
BUCKET=${S3_BUCKET:-""}
THREADS=${THREADS:-4}
OBJECT_SIZE=${OBJECT_SIZE:-"1KiB"}
OBJECT_COUNT=${OBJECT_COUNT:-100}
DURATION=${DURATION:-""}
SEED_OBJECTS=${SEED_OBJECTS:-2500}
DELETE_BATCH_SIZE=${DELETE_BATCH_SIZE:-25}
DELETE_PREFIX=${DELETE_PREFIX:-"spt-delete-e2e/$run_stamp/"}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT_DIR/results"}
LABEL=${LABEL:-"delete-e2e"}
S3_DRIVER=${SPT_S3_DRIVER:-"default"}
MAX_FAILED_OBJECTS=${MAX_FAILED_OBJECTS:-0}
VERIFY=${VERIFY:-false}
CLEANUP=${CLEANUP:-true}
FORCE=${FORCE:-true}
VERBOSE=${VERBOSE:-true}
KEEP_SCENARIO=${KEEP_SCENARIO:-false}
GENERATE_ONLY=${GENERATE_ONLY:-false}
SPT_IMAGE=${SPT_IMAGE:-""}
SKIP_IMAGE_PULL=${SPT_SKIP_IMAGE_PULL:-false}
EXTRA_ARGS=()

require_value() {
	if (( $# < 2 )) || [[ -z "${2:-}" ]]; then
		echo "error: $1 requires a value" >&2
		exit 2
	fi
}

enabled() {
	case "${1,,}" in
		1|true|yes|on) return 0 ;;
		*) return 1 ;;
	esac
}

require_positive_integer() {
	local name=$1 value=$2
	if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
		echo "error: $name must be a positive integer" >&2
		exit 2
	fi
}

while (( $# > 0 )); do
	case "$1" in
		--hosts) require_value "$@"; HOSTS=$2; shift 2 ;;
		--min-hosts) require_value "$@"; MIN_HOSTS=$2; shift 2 ;;
		--s3-endpoint|--s3-endpoints) require_value "$@"; ENDPOINTS=$2; shift 2 ;;
		--s3-access-key) require_value "$@"; ACCESS_KEY=$2; shift 2 ;;
		--s3-secret-key) require_value "$@"; SECRET_KEY=$2; shift 2 ;;
		--s3-bucket) require_value "$@"; BUCKET=$2; shift 2 ;;
		--threads) require_value "$@"; THREADS=$2; shift 2 ;;
		--object-size) require_value "$@"; OBJECT_SIZE=$2; shift 2 ;;
		--object-count) require_value "$@"; OBJECT_COUNT=$2; DURATION=""; shift 2 ;;
		--duration) require_value "$@"; DURATION=$2; shift 2 ;;
		--seed-objects) require_value "$@"; SEED_OBJECTS=$2; shift 2 ;;
		--delete-batch-size) require_value "$@"; DELETE_BATCH_SIZE=$2; shift 2 ;;
		--prefix) require_value "$@"; DELETE_PREFIX=$2; shift 2 ;;
		--results-dir) require_value "$@"; RESULTS_DIR=$2; shift 2 ;;
		--label) require_value "$@"; LABEL=$2; shift 2 ;;
		--s3-driver) require_value "$@"; S3_DRIVER=$2; shift 2 ;;
		--max-failed-objects) require_value "$@"; MAX_FAILED_OBJECTS=$2; shift 2 ;;
		--image) require_value "$@"; SPT_IMAGE=$2; shift 2 ;;
		--skip-image-pull) SKIP_IMAGE_PULL=true; shift ;;
		--no-skip-image-pull) SKIP_IMAGE_PULL=false; shift ;;
		--verify) VERIFY=true; shift ;;
		--no-verify) VERIFY=false; shift ;;
		--cleanup) CLEANUP=true; shift ;;
		--no-cleanup) CLEANUP=false; shift ;;
		--force) FORCE=true; shift ;;
		--no-force) FORCE=false; shift ;;
		--verbose) VERBOSE=true; shift ;;
		--quiet|--no-verbose) VERBOSE=false; shift ;;
		--keep-scenario) KEEP_SCENARIO=true; shift ;;
		--no-keep-scenario) KEEP_SCENARIO=false; shift ;;
		--generate-only) GENERATE_ONLY=true; shift ;;
		--)
			shift
			EXTRA_ARGS=("$@")
			break
			;;
		-h|--help)
			cat <<EOF
Usage: $(basename "$0") [options] [-- additional-spt-flags]

Run a short headless standalone DELETE test. The default count mode creates
100 run-owned 1 KiB objects, deletes that frozen inventory in batches, and
cleans up residuals. Duration mode uses --seed-objects as its finite inventory
and fails if that inventory is exhausted before the deadline.

Connection:
  --hosts CSV                  Docker hosts (env: HOSTS; fallback: 127.0.0.1)
  --min-hosts N                Required host count (default: $MIN_HOSTS)
  --s3-endpoint URL            Single S3 endpoint
  --s3-endpoints CSV           One or more S3 endpoints
  --s3-access-key KEY          S3 access key
  --s3-secret-key KEY          S3 secret key
  --s3-bucket NAME             S3 bucket

Workload:
  --threads N                  Parallel engine operations (default: $THREADS)
  --object-size SIZE           Seed object size (default: $OBJECT_SIZE)
  --object-count N             Count-mode inventory (default: $OBJECT_COUNT)
  --duration DURATION          Timed DELETE mode, for example 10s
  --seed-objects N             Timed-mode finite inventory (default: $SEED_OBJECTS)
  --delete-batch-size N        Targets per DELETE request, 1-1000
                               (default: $DELETE_BATCH_SIZE)
  --prefix PREFIX              Run-owned namespace (default: generated uniquely)
  --max-failed-objects N       Operational failure budget (default: 0)
  --[no-]verify                Verify every selected identity is absent
                               (default: $VERIFY)
  --[no-]cleanup               Best-effort residual cleanup (default: $CLEANUP)

Driver and image:
  --s3-driver TYPE             default, aws, or rdma (default: $S3_DRIVER)
  --image IMAGE                Engine image override (env: SPT_IMAGE)
  --[no-]skip-image-pull       Use a preloaded local image

Output and behavior:
  --results-dir DIR            Auto-results parent (default: $RESULTS_DIR)
  --label LABEL                Results label (default: $LABEL)
  --generate-only              Generate the scenario without starting Docker
  --[no-]force                 Resolve local port conflicts (default: $FORCE)
  --[no-]verbose               Verbose CLI output (default: $VERBOSE)
  --[no-]keep-scenario         Preserve the generated scenario
  -- additional-spt-flags      Forward remaining flags directly to spt

Examples:
  $(basename "$0")
  $(basename "$0") --verify
  $(basename "$0") --object-count 500 --delete-batch-size 100
  $(basename "$0") --duration 10s --seed-objects 10000
  $(basename "$0") --image ghcr.io/dell/storage-performance-tool:spt_dev --skip-image-pull
EOF
			exit 0
			;;
		*)
			echo "error: unknown argument $1 (use -- to forward raw spt flags)" >&2
			exit 2
			;;
	esac
done

if [[ ! -x "$SPT_BIN" ]]; then
	echo "error: SPT binary not found at $SPT_BIN; run 'make -C cli build' first" >&2
	exit 1
fi
if [[ "$SPT_BIN" != /* ]]; then
	SPT_BIN="$(cd -- "$(dirname -- "$SPT_BIN")" && pwd)/$(basename -- "$SPT_BIN")"
fi

for required in ENDPOINTS ACCESS_KEY SECRET_KEY BUCKET DELETE_PREFIX; do
	if [[ -z "${!required}" ]]; then
		echo "error: $required is required; provide its option, environment value, or cli/.env entry" >&2
		exit 2
	fi
done

require_positive_integer "--threads" "$THREADS"
require_positive_integer "--min-hosts" "$MIN_HOSTS"
require_positive_integer "--delete-batch-size" "$DELETE_BATCH_SIZE"
if (( DELETE_BATCH_SIZE > 1000 )); then
	echo "error: --delete-batch-size must not exceed 1000" >&2
	exit 2
fi
if [[ ! "$MAX_FAILED_OBJECTS" =~ ^[0-9]+$ ]]; then
	echo "error: --max-failed-objects must be a non-negative integer" >&2
	exit 2
fi
if [[ -n "$DURATION" ]]; then
	require_positive_integer "--seed-objects" "$SEED_OBJECTS"
else
	require_positive_integer "--object-count" "$OBJECT_COUNT"
fi

echo "=== Seeded DELETE Test ==="
echo "SPT binary: $SPT_BIN"
echo "Hosts: $HOSTS (min-hosts: $MIN_HOSTS)"
echo "Endpoints: configured"
echo "Bucket: $BUCKET"
echo "Prefix: $DELETE_PREFIX"
if [[ -n "$DURATION" ]]; then
	echo "Mode: duration=$DURATION  SeedObjects: $SEED_OBJECTS"
else
	echo "Mode: object-count=$OBJECT_COUNT"
fi
echo "Threads: $THREADS  ObjectSize: $OBJECT_SIZE  DeleteBatchSize: $DELETE_BATCH_SIZE"
echo "Verify: $VERIFY  Cleanup: $CLEANUP  Results: $RESULTS_DIR  Label: $LABEL"
[[ "$S3_DRIVER" != "default" ]] && echo "S3 driver: $S3_DRIVER"
[[ -n "$SPT_IMAGE" ]] && echo "Image: $SPT_IMAGE"
enabled "$SKIP_IMAGE_PULL" && echo "Skip image pull: true"
enabled "$GENERATE_ONLY" && echo "Generate only: true"
echo

[[ -n "$SPT_IMAGE" ]] && export SPT_IMAGE
if enabled "$SKIP_IMAGE_PULL"; then
	export SPT_SKIP_IMAGE_PULL=true
else
	unset SPT_SKIP_IMAGE_PULL
fi

cd "$ROOT_DIR"

cmd=("$SPT_BIN" run delete
	--headless
	--auto-results=true
	--shutdown-on-complete=true
	--endpoints "$ENDPOINTS"
	--access-key "$ACCESS_KEY"
	--secret-key "$SECRET_KEY"
	--bucket "$BUCKET"
	--test-hosts "$HOSTS"
	--min-hosts "$MIN_HOSTS"
	--threads "$THREADS"
	--object-size "$OBJECT_SIZE"
	--prefix "$DELETE_PREFIX"
	--delete-batch-size "$DELETE_BATCH_SIZE"
	--max-failed-objects "$MAX_FAILED_OBJECTS"
	--results-dir "$RESULTS_DIR"
	--label "$LABEL")

if [[ -n "$DURATION" ]]; then
	cmd+=(--duration "$DURATION" --seed-objects "$SEED_OBJECTS")
else
	cmd+=(--object-count "$OBJECT_COUNT")
fi
[[ "$S3_DRIVER" != "default" ]] && cmd+=(--s3-driver "$S3_DRIVER")
enabled "$VERIFY" && cmd+=(--verify)
enabled "$CLEANUP" && cmd+=(--cleanup)
enabled "$FORCE" && cmd+=(--force)
enabled "$VERBOSE" && cmd+=(--verbose)
enabled "$KEEP_SCENARIO" && cmd+=(--keep-scenario)
enabled "$GENERATE_ONLY" && cmd+=(--generate-only)
cmd+=("${EXTRA_ARGS[@]}")

"${cmd[@]}"

echo
echo "=== Seeded DELETE Test Complete ==="
