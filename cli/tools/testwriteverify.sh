#!/usr/bin/env bash

# Run a short write/read integrity verification workload. Configuration is
# loaded from cli/.env when present and may be overridden by command options.

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
OBJECT_SIZE=${OBJECT_SIZE:-"1MiB"}
OBJECT_COUNT=${OBJECT_COUNT:-32}
DURATION=${DURATION:-""}
WRITE_VERIFY_PREFIX=${WRITE_VERIFY_PREFIX:-"spt-write-verify/$run_stamp/"}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT_DIR/results"}
LABEL=${LABEL:-"writeverify-local"}
MAX_CONSOLE_FAILURES=${SPT_INTEGRITY_MAX_CONSOLE_FAILURES:-20}
S3_DRIVER=${SPT_S3_DRIVER:-"default"}
PART_SIZE=${PART_SIZE:-""}
CHECKSUM=${SPT_CHECKSUM:-""}
SPT_IMAGE=${SPT_IMAGE:-""}
SKIP_IMAGE_PULL=${SPT_SKIP_IMAGE_PULL:-false}
FORCE=${FORCE:-true}
VERBOSE=${VERBOSE:-true}
CLEANUP=${CLEANUP:-true}
KEEP_SCENARIO=${KEEP_SCENARIO:-false}
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
		--prefix) require_value "$@"; WRITE_VERIFY_PREFIX=$2; shift 2 ;;
		--results-dir) require_value "$@"; RESULTS_DIR=$2; shift 2 ;;
		--label) require_value "$@"; LABEL=$2; shift 2 ;;
		--max-console-failures) require_value "$@"; MAX_CONSOLE_FAILURES=$2; shift 2 ;;
		--s3-driver) require_value "$@"; S3_DRIVER=$2; shift 2 ;;
		--part-size) require_value "$@"; PART_SIZE=$2; shift 2 ;;
		--checksum) require_value "$@"; CHECKSUM=$2; shift 2 ;;
		--image) require_value "$@"; SPT_IMAGE=$2; shift 2 ;;
		--skip-image-pull) SKIP_IMAGE_PULL=true; shift ;;
		--no-skip-image-pull) SKIP_IMAGE_PULL=false; shift ;;
		--force) FORCE=true; shift ;;
		--no-force) FORCE=false; shift ;;
		--verbose) VERBOSE=true; shift ;;
		--quiet|--no-verbose) VERBOSE=false; shift ;;
		--cleanup) CLEANUP=true; shift ;;
		--no-cleanup) CLEANUP=false; shift ;;
		--keep-scenario) KEEP_SCENARIO=true; shift ;;
		--no-keep-scenario) KEEP_SCENARIO=false; shift ;;
		--)
			shift
			EXTRA_ARGS=("$@")
			break
			;;
		-h|--help)
			cat <<EOF
Usage: $(basename "$0") [options] [-- additional-spt-flags]

Run a short headless write-verify workload. Successfully written objects are
read back and SHA-256 verified; --cleanup deletes only verified objects.

Connection:
  --hosts CSV                  Docker hosts (default: $HOSTS)
  --min-hosts N                Required host count (default: $MIN_HOSTS)
  --s3-endpoint URL            Single S3 endpoint
  --s3-endpoints CSV           One or more S3 endpoints
  --s3-access-key KEY          S3 access key
  --s3-secret-key KEY          S3 secret key
  --s3-bucket NAME             S3 bucket

Workload:
  --threads N                  Concurrency per node (default: $THREADS)
  --object-size SIZE           Object size (default: $OBJECT_SIZE)
  --object-count N             Objects to write and verify (default: $OBJECT_COUNT)
  --duration DURATION          Use a bounded write duration instead of a count
  --prefix PREFIX              Isolated object namespace
                               (default: $WRITE_VERIFY_PREFIX)
  --[no-]cleanup               Delete verified objects (default: $CLEANUP)
  --max-console-failures N     Failure samples printed (default: $MAX_CONSOLE_FAILURES)

Driver and image:
  --s3-driver TYPE             default, netty, aws, or rdma (default: $S3_DRIVER)
  --part-size SIZE             Enable multipart writes
  --checksum ALGORITHM         Transport checksum (integrity SHA-256 is always enabled)
  --image IMAGE                Engine image override (env: SPT_IMAGE)
  --[no-]skip-image-pull       Use a preloaded local image

Output and behavior:
  --results-dir DIR            Auto-results parent (default: $RESULTS_DIR)
  --label LABEL                Results label (default: $LABEL)
  --[no-]force                 Resolve local port conflicts (default: $FORCE)
  --[no-]verbose               Verbose CLI output (default: $VERBOSE)
  --[no-]keep-scenario         Preserve generated scenario (default: $KEEP_SCENARIO)
  -- additional-spt-flags      Forward remaining flags directly to spt

Inputs may also be supplied through cli/.env or S3_*, HOSTS, SPT_IMAGE,
SPT_SKIP_IMAGE_PULL, SPT_S3_DRIVER, and WRITE_VERIFY_PREFIX environment values.

Examples:
  $(basename "$0")
  $(basename "$0") --object-count 100 --object-size 4MiB
  $(basename "$0") --image ghcr.io/dell/storage-performance-tool:spt_dev \\
    --skip-image-pull
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

for required in ENDPOINTS ACCESS_KEY SECRET_KEY BUCKET WRITE_VERIFY_PREFIX; do
	if [[ -z "${!required}" ]]; then
		echo "error: $required is required; provide its option, environment value, or cli/.env entry" >&2
		exit 2
	fi
done

echo "=== Write-Verify Test ==="
echo "SPT binary: $SPT_BIN"
echo "Hosts: $HOSTS (min-hosts: $MIN_HOSTS)"
echo "Endpoints: configured"
echo "Bucket: $BUCKET"
echo "Prefix: $WRITE_VERIFY_PREFIX"
if [[ -n "$DURATION" ]]; then
	echo "Threads: $THREADS  ObjectSize: $OBJECT_SIZE  Duration: $DURATION"
else
	echo "Threads: $THREADS  ObjectSize: $OBJECT_SIZE  ObjectCount: $OBJECT_COUNT"
fi
echo "Cleanup: $CLEANUP  Results: $RESULTS_DIR  Label: $LABEL"
[[ "$S3_DRIVER" != "default" ]] && echo "S3 driver: $S3_DRIVER"
[[ -n "$PART_SIZE" ]] && echo "Part size: $PART_SIZE"
[[ -n "$CHECKSUM" ]] && echo "Transport checksum: $CHECKSUM"
[[ -n "$SPT_IMAGE" ]] && echo "Image: $SPT_IMAGE"
enabled "$SKIP_IMAGE_PULL" && echo "Skip image pull: true"
echo

[[ -n "$SPT_IMAGE" ]] && export SPT_IMAGE
if enabled "$SKIP_IMAGE_PULL"; then
	export SPT_SKIP_IMAGE_PULL=true
else
	unset SPT_SKIP_IMAGE_PULL
fi

# Keep generated scenarios and default results beneath cli/, independent of
# the caller's working directory.
cd "$ROOT_DIR"

cmd=("$SPT_BIN" run write-verify
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
	--prefix "$WRITE_VERIFY_PREFIX"
	--results-dir "$RESULTS_DIR"
	--label "$LABEL"
	--integrity-max-console-failures "$MAX_CONSOLE_FAILURES")

if [[ -n "$DURATION" ]]; then
	cmd+=(--duration "$DURATION")
else
	cmd+=(--object-count "$OBJECT_COUNT")
fi
[[ "$S3_DRIVER" != "default" ]] && cmd+=(--s3-driver "$S3_DRIVER")
[[ -n "$PART_SIZE" ]] && cmd+=(--part-size "$PART_SIZE")
[[ -n "$CHECKSUM" ]] && cmd+=(--checksum "$CHECKSUM")
enabled "$FORCE" && cmd+=(--force)
enabled "$VERBOSE" && cmd+=(--verbose)
enabled "$CLEANUP" && cmd+=(--cleanup)
enabled "$KEEP_SCENARIO" && cmd+=(--keep-scenario)
cmd+=("${EXTRA_ARGS[@]}")

"${cmd[@]}"

echo
echo "=== Write-Verify Test Complete ==="
