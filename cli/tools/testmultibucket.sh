#!/usr/bin/env bash

# Spread a short headless workload over a set of buckets (--bucket-count).
# Configuration is loaded from cli/.env when present and may be overridden by options.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SPT_BIN=${SPT_BIN:-"$ROOT_DIR/spt"}

if [[ -f "$ROOT_DIR/.env" ]]; then
	# shellcheck disable=SC1090,SC1091
	source "$ROOT_DIR/.env"
fi

run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BUCKET_COUNT=${BUCKET_COUNT:-16}
WORKLOAD=${WORKLOAD:-"write"}
HOSTS=${HOSTS:-"127.0.0.1"}
MIN_HOSTS=${MIN_HOSTS:-1}
ENDPOINTS=${S3_ENDPOINTS:-${S3_ENDPOINT:-""}}
ACCESS_KEY=${S3_ACCESS_KEY:-""}
SECRET_KEY=${S3_SECRET_KEY:-""}
BUCKET=${S3_BUCKET:-""}
THREADS=${THREADS:-8}
OBJECT_SIZE=${OBJECT_SIZE:-"1MiB"}
OBJECT_COUNT=${OBJECT_COUNT:-1600}
DURATION=${DURATION:-"30s"}
PREFIX=${MULTI_BUCKET_PREFIX:-"spt-multi-bucket/$run_stamp/"}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT_DIR/results"}
LABEL=${LABEL:-"multibucket"}
S3_DRIVER=${SPT_S3_DRIVER:-"default"}
SPT_IMAGE=${SPT_IMAGE:-""}
SKIP_IMAGE_PULL=${SPT_SKIP_IMAGE_PULL:-false}
FORCE=${FORCE:-true}
VERBOSE=${VERBOSE:-true}
CLEANUP=${CLEANUP:-true}
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

usage() {
	cat <<EOF
Usage: $(basename "$0") [BUCKETS] [options] [-- additional-spt-flags]

Run a short headless workload whose new objects are spread over BUCKETS buckets
(default: $BUCKET_COUNT), named <bucket>-0 ... <bucket>-<BUCKETS-1> after --s3-bucket.
SPT creates missing buckets of the set before the workload starts and never
deletes buckets; --cleanup deletes only the objects the run created.

Buckets and workload:
  BUCKETS, --buckets N         Bucket count, 1 to 1000 (default: $BUCKET_COUNT)
  --workload TYPE              write, write-verify, read (seeded), delete (seeded)
                               or mixed (seeded) (default: $WORKLOAD)
  --threads N                  Concurrency per node (default: $THREADS)
  --object-size SIZE           Object size (default: $OBJECT_SIZE)
  --object-count N             Objects for write, write-verify and delete, and the
                               seed of read (default: $OBJECT_COUNT)
  --duration DURATION          Measured duration of read and mixed (default: $DURATION)
  --prefix PREFIX              Key prefix for write and write-verify
                               (default: $PREFIX)
  --[no-]cleanup               Delete the created objects afterwards (default: $CLEANUP)

Connection:
  --hosts CSV                  Docker hosts (default: $HOSTS)
  --min-hosts N                Required host count (default: $MIN_HOSTS)
  --s3-endpoint URL            Single S3 endpoint
  --s3-endpoints CSV           One or more S3 endpoints
  --s3-access-key KEY          S3 access key
  --s3-secret-key KEY          S3 secret key
  --s3-bucket NAME             Base bucket the set is named after

Driver and image:
  --s3-driver TYPE             default, aws, or rdma (default: $S3_DRIVER)
  --image IMAGE                Engine image override (env: SPT_IMAGE)
  --[no-]skip-image-pull       Use a preloaded local image

Output and behavior:
  --results-dir DIR            Auto-results parent (default: $RESULTS_DIR)
  --label LABEL                Results label (default: $LABEL)
  --[no-]force                 Resolve local port conflicts (default: $FORCE)
  --[no-]verbose               Verbose CLI output (default: $VERBOSE)
  -- additional-spt-flags      Forward remaining flags directly to spt

Inputs may also be supplied through cli/.env or S3_*, HOSTS, BUCKET_COUNT,
WORKLOAD, SPT_IMAGE, SPT_SKIP_IMAGE_PULL, SPT_S3_DRIVER and MULTI_BUCKET_PREFIX
environment values.

Examples:
  $(basename "$0")
  $(basename "$0") 4 --workload mixed --duration 1m
  $(basename "$0") 32 --object-size 4MiB --s3-driver rdma
EOF
}

if (( $# > 0 )) && [[ "$1" =~ ^[0-9]+$ ]]; then
	BUCKET_COUNT=$1
	shift
fi

while (( $# > 0 )); do
	case "$1" in
		--buckets) require_value "$@"; BUCKET_COUNT=$2; shift 2 ;;
		--workload) require_value "$@"; WORKLOAD=$2; shift 2 ;;
		--hosts) require_value "$@"; HOSTS=$2; shift 2 ;;
		--min-hosts) require_value "$@"; MIN_HOSTS=$2; shift 2 ;;
		--s3-endpoint|--s3-endpoints) require_value "$@"; ENDPOINTS=$2; shift 2 ;;
		--s3-access-key) require_value "$@"; ACCESS_KEY=$2; shift 2 ;;
		--s3-secret-key) require_value "$@"; SECRET_KEY=$2; shift 2 ;;
		--s3-bucket) require_value "$@"; BUCKET=$2; shift 2 ;;
		--threads) require_value "$@"; THREADS=$2; shift 2 ;;
		--object-size) require_value "$@"; OBJECT_SIZE=$2; shift 2 ;;
		--object-count) require_value "$@"; OBJECT_COUNT=$2; shift 2 ;;
		--duration) require_value "$@"; DURATION=$2; shift 2 ;;
		--prefix) require_value "$@"; PREFIX=$2; shift 2 ;;
		--results-dir) require_value "$@"; RESULTS_DIR=$2; shift 2 ;;
		--label) require_value "$@"; LABEL=$2; shift 2 ;;
		--s3-driver) require_value "$@"; S3_DRIVER=$2; shift 2 ;;
		--image) require_value "$@"; SPT_IMAGE=$2; shift 2 ;;
		--skip-image-pull) SKIP_IMAGE_PULL=true; shift ;;
		--no-skip-image-pull) SKIP_IMAGE_PULL=false; shift ;;
		--force) FORCE=true; shift ;;
		--no-force) FORCE=false; shift ;;
		--verbose) VERBOSE=true; shift ;;
		--quiet|--no-verbose) VERBOSE=false; shift ;;
		--cleanup) CLEANUP=true; shift ;;
		--no-cleanup) CLEANUP=false; shift ;;
		--)
			shift
			EXTRA_ARGS=("$@")
			break
			;;
		-h|--help) usage; exit 0 ;;
		*)
			echo "error: unknown argument $1 (use -- to forward raw spt flags)" >&2
			exit 2
			;;
	esac
done

if [[ ! "$BUCKET_COUNT" =~ ^[0-9]+$ ]] || (( BUCKET_COUNT < 1 || BUCKET_COUNT > 1000 )); then
	echo "error: the bucket count must be between 1 and 1000, got $BUCKET_COUNT" >&2
	exit 2
fi
case "$WORKLOAD" in
	write|write-verify|read|delete|mixed) ;;
	*) echo "error: --workload must be write, write-verify, read, delete or mixed, got $WORKLOAD" >&2; exit 2 ;;
esac

if [[ ! -x "$SPT_BIN" ]]; then
	echo "error: SPT binary not found at $SPT_BIN; run 'make -C cli build' first" >&2
	exit 1
fi
if [[ "$SPT_BIN" != /* ]]; then
	SPT_BIN="$(cd -- "$(dirname -- "$SPT_BIN")" && pwd)/$(basename -- "$SPT_BIN")"
fi

for required in ENDPOINTS ACCESS_KEY SECRET_KEY BUCKET; do
	if [[ -z "${!required}" ]]; then
		echo "error: $required is required; provide its option, environment value, or cli/.env entry" >&2
		exit 2
	fi
done

base=${BUCKET#/}
base=${base%%/*}
echo "=== Multi-Bucket Test ==="
echo "SPT binary: $SPT_BIN"
echo "Hosts: $HOSTS (min-hosts: $MIN_HOSTS)"
echo "Endpoints: configured"
if (( BUCKET_COUNT > 1 )); then
	echo "Buckets: $BUCKET_COUNT ($base-0 ... $base-$((BUCKET_COUNT - 1)))"
else
	echo "Buckets: 1 ($base)"
fi
echo "Workload: $WORKLOAD  Threads: $THREADS  ObjectSize: $OBJECT_SIZE"
echo "Cleanup: $CLEANUP  Results: $RESULTS_DIR  Label: $LABEL"
[[ "$S3_DRIVER" != "default" ]] && echo "S3 driver: $S3_DRIVER"
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

cmd=("$SPT_BIN" run "$WORKLOAD"
	--headless
	--auto-results=true
	--shutdown-on-complete=true
	--endpoints "$ENDPOINTS"
	--access-key "$ACCESS_KEY"
	--secret-key "$SECRET_KEY"
	--bucket "$BUCKET"
	--bucket-count "$BUCKET_COUNT"
	--test-hosts "$HOSTS"
	--min-hosts "$MIN_HOSTS"
	--threads "$THREADS"
	--object-size "$OBJECT_SIZE"
	--results-dir "$RESULTS_DIR")

case "$WORKLOAD" in
	write|write-verify) cmd+=(--object-count "$OBJECT_COUNT" --prefix "$PREFIX") ;;
	read) cmd+=(--seed-objects "$OBJECT_COUNT" --duration "$DURATION") ;;
	# A seeded DELETE over several buckets sends one object per request.
	delete) cmd+=(--object-count "$OBJECT_COUNT" --delete-batch-size 1) ;;
	mixed) cmd+=(--duration "$DURATION") ;;
esac
# A custom label currently breaks the standalone DELETE summary, so a DELETE run keeps the
# default label.
[[ "$WORKLOAD" != "delete" ]] && cmd+=(--label "$LABEL")
[[ "$S3_DRIVER" != "default" ]] && cmd+=(--s3-driver "$S3_DRIVER")
enabled "$FORCE" && cmd+=(--force)
enabled "$VERBOSE" && cmd+=(--verbose)
enabled "$CLEANUP" && cmd+=(--cleanup)
cmd+=("${EXTRA_ARGS[@]}")

"${cmd[@]}"

echo
echo "=== Multi-Bucket Test Complete ==="
