#!/usr/bin/env bash

# Run a seeded standalone DELETE scenario by invoking spt.jar directly.
# One scenario process creates a canonical manifest, deletes that exact frozen
# inventory, optionally verifies absence, and optionally cleans up residuals.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd -- "$ENGINE_ROOT/.." && pwd)"

declare -A caller_env
for env_name in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_AUTH_VERSION \
		S3_DRIVER DELETE_CONCURRENCY DELETE_OBJECT_SIZE DELETE_OBJECT_COUNT \
		DELETE_DURATION DELETE_SEED_OBJECTS DELETE_BATCH_SIZE DELETE_PREFIX \
		DELETE_VERIFY DELETE_CLEANUP DELETE_VERIFICATION_TIMEOUT_MS \
		DELETE_RESULTS_DIR DELETE_RUN_ID SPT_JAVA_OPTS SPT_JAR JAVA_BIN; do
	[[ -n "${!env_name+set}" ]] && caller_env[$env_name]="${!env_name}"
done

if [[ -f "$GIT_ROOT/.env" ]]; then
	# shellcheck disable=SC1091
	source "$GIT_ROOT/.env"
elif [[ -f "$ENGINE_ROOT/.env" ]]; then
	# shellcheck disable=SC1091
	source "$ENGINE_ROOT/.env"
fi

for env_name in "${!caller_env[@]}"; do
	export "$env_name=${caller_env[$env_name]}"
done
unset caller_env env_name

run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
SPT_JAR=${SPT_JAR:-"$ENGINE_ROOT/bundle/build/dist/spt.jar"}
JAVA_BIN=${JAVA_BIN:-java}
S3_ENDPOINTS=${S3_ENDPOINTS:-"http://127.0.0.1:9000"}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-"AWS_ACCESS_KEY_ID_EXAMPLE"}
S3_SECRET_KEY=${S3_SECRET_KEY:-"AWS_SECRET_ACCESS_KEY_EXAMPLE"}
S3_BUCKET=${S3_BUCKET:-"testdelete"}
S3_AUTH_VERSION=${S3_AUTH_VERSION:-4}
S3_DRIVER=${S3_DRIVER:-"s3"}
DELETE_CONCURRENCY=${DELETE_CONCURRENCY:-4}
DELETE_OBJECT_SIZE=${DELETE_OBJECT_SIZE:-"1KiB"}
DELETE_OBJECT_COUNT=${DELETE_OBJECT_COUNT:-100}
DELETE_DURATION=${DELETE_DURATION:-""}
DELETE_SEED_OBJECTS=${DELETE_SEED_OBJECTS:-2500}
DELETE_BATCH_SIZE=${DELETE_BATCH_SIZE:-25}
DELETE_PREFIX=${DELETE_PREFIX:-"spt-delete-e2e/$run_stamp/"}
DELETE_VERIFY=${DELETE_VERIFY:-false}
DELETE_CLEANUP=${DELETE_CLEANUP:-true}
DELETE_VERIFICATION_TIMEOUT_MS=${DELETE_VERIFICATION_TIMEOUT_MS:-30000}
DELETE_RESULTS_DIR=${DELETE_RESULTS_DIR:-"$SCRIPT_DIR/results"}
DELETE_RUN_ID=${DELETE_RUN_ID:-"$(date +%s%3N)"}
SPT_JAVA_OPTS=${SPT_JAVA_OPTS:-""}
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
		--s3-endpoint|--s3-endpoints) require_value "$@"; S3_ENDPOINTS=$2; shift 2 ;;
		--s3-access-key) require_value "$@"; S3_ACCESS_KEY=$2; shift 2 ;;
		--s3-secret-key) require_value "$@"; S3_SECRET_KEY=$2; shift 2 ;;
		--s3-bucket) require_value "$@"; S3_BUCKET=$2; shift 2 ;;
		--auth-version) require_value "$@"; S3_AUTH_VERSION=$2; shift 2 ;;
		--s3-driver) require_value "$@"; S3_DRIVER=$2; shift 2 ;;
		--threads) require_value "$@"; DELETE_CONCURRENCY=$2; shift 2 ;;
		--object-size) require_value "$@"; DELETE_OBJECT_SIZE=$2; shift 2 ;;
		--object-count) require_value "$@"; DELETE_OBJECT_COUNT=$2; DELETE_DURATION=""; shift 2 ;;
		--duration) require_value "$@"; DELETE_DURATION=$2; shift 2 ;;
		--seed-objects) require_value "$@"; DELETE_SEED_OBJECTS=$2; shift 2 ;;
		--delete-batch-size) require_value "$@"; DELETE_BATCH_SIZE=$2; shift 2 ;;
		--prefix) require_value "$@"; DELETE_PREFIX=$2; shift 2 ;;
		--verification-timeout-ms) require_value "$@"; DELETE_VERIFICATION_TIMEOUT_MS=$2; shift 2 ;;
		--results-dir) require_value "$@"; DELETE_RESULTS_DIR=$2; shift 2 ;;
		--run-id) require_value "$@"; DELETE_RUN_ID=$2; shift 2 ;;
		--jar) require_value "$@"; SPT_JAR=$2; shift 2 ;;
		--java) require_value "$@"; JAVA_BIN=$2; shift 2 ;;
		--verify) DELETE_VERIFY=true; shift ;;
		--no-verify) DELETE_VERIFY=false; shift ;;
		--cleanup) DELETE_CLEANUP=true; shift ;;
		--no-cleanup) DELETE_CLEANUP=false; shift ;;
		--)
			shift
			EXTRA_ARGS=("$@")
			break
			;;
		-h|--help)
			cat <<EOF
Usage: $(basename "$0") [options] [-- additional-engine-flags]

Run a local seeded standalone DELETE scenario through a direct
"java -jar spt.jar" invocation. Count mode is the default. Duration mode uses
--seed-objects as a finite inventory and fails if it empties before the limit.

Connection:
  --s3-endpoint URL            Single S3 endpoint
  --s3-endpoints CSV           One or more S3 endpoints
  --s3-access-key KEY          S3 access key
  --s3-secret-key KEY          S3 secret key
  --s3-bucket NAME             S3 bucket (env: S3_BUCKET; fallback: testdelete)
  --auth-version N             S3 signature version (default: $S3_AUTH_VERSION)
  --s3-driver TYPE             s3, s3-aws, or s3-rdma (default: $S3_DRIVER)

Workload:
  --threads N                  Parallel engine operations (default: $DELETE_CONCURRENCY)
  --object-size SIZE           Seed object size (default: $DELETE_OBJECT_SIZE)
  --object-count N             Count-mode inventory (default: $DELETE_OBJECT_COUNT)
  --duration DURATION          Timed DELETE mode, for example 10s
  --seed-objects N             Timed-mode finite inventory
                               (default: $DELETE_SEED_OBJECTS)
  --delete-batch-size N        Targets per DELETE request, 1-1000
                               (default: $DELETE_BATCH_SIZE)
  --prefix PREFIX              Run-owned namespace (default: generated uniquely)
  --[no-]verify                Verify every selected identity is absent
                               (default: $DELETE_VERIFY)
  --[no-]cleanup               Best-effort residual cleanup (default: $DELETE_CLEANUP)
  --verification-timeout-ms N  Verification deadline (default: $DELETE_VERIFICATION_TIMEOUT_MS)

Runtime and output:
  --results-dir DIR            Results parent (default: $DELETE_RESULTS_DIR)
  --run-id N                   Positive engine run ID (default: generated)
  --jar PATH                   spt.jar path (default: $SPT_JAR)
  --java PATH                  Java executable (default: $JAVA_BIN)
  -- additional-engine-flags   Forward remaining flags directly to spt.jar

Environment variables use the displayed S3_* and DELETE_* names. SPT_JAVA_OPTS
may contain simple whitespace-separated JVM options.

Examples:
  $(basename "$0")
  $(basename "$0") --verify
  $(basename "$0") --object-count 500 --delete-batch-size 100
  $(basename "$0") --duration 10s --seed-objects 10000
  $(basename "$0") --s3-driver s3-aws
EOF
			exit 0
			;;
		*)
			echo "error: unknown argument $1 (use -- to forward raw engine flags)" >&2
			exit 2
			;;
	esac
done

for required in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET DELETE_PREFIX; do
	if [[ -z "${!required}" ]]; then
		echo "error: $required must not be empty" >&2
		exit 2
	fi
done

require_positive_integer "--threads" "$DELETE_CONCURRENCY"
require_positive_integer "--delete-batch-size" "$DELETE_BATCH_SIZE"
require_positive_integer "--verification-timeout-ms" "$DELETE_VERIFICATION_TIMEOUT_MS"
require_positive_integer "--run-id" "$DELETE_RUN_ID"
if (( DELETE_BATCH_SIZE > 1000 )); then
	echo "error: --delete-batch-size must not exceed 1000" >&2
	exit 2
fi
if [[ -n "$DELETE_DURATION" ]]; then
	require_positive_integer "--seed-objects" "$DELETE_SEED_OBJECTS"
	seed_count=$DELETE_SEED_OBJECTS
else
	require_positive_integer "--object-count" "$DELETE_OBJECT_COUNT"
	seed_count=$DELETE_OBJECT_COUNT
fi

case "$S3_DRIVER" in
	s3|s3-aws|s3-rdma) ;;
	*)
		echo "error: --s3-driver must be s3, s3-aws, or s3-rdma" >&2
		exit 2
		;;
esac

if [[ ! -f "$SPT_JAR" ]]; then
	echo "spt.jar not found at $SPT_JAR; building the distribution..." >&2
	(cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi
if [[ ! -f "$SPT_JAR" ]]; then
	echo "error: spt.jar was not produced at $SPT_JAR" >&2
	exit 1
fi

run_dir="$DELETE_RESULTS_DIR/testdelete-$run_stamp"
scenario_file="$run_dir/delete-scenario.js"
mkdir -p "$run_dir"

cat > "$scenario_file" <<'SCENARIO'
// Generated by engine/tools/testdelete.sh
// Seeded standalone DELETE through the direct spt.jar scenario API.
var System = java.lang.System;
var TimeUnit = java.util.concurrent.TimeUnit;
function envValue(name) {
  var value = System.getenv(name);
  return value == null ? "" : String(value);
}
var concurrency = parseInt(envValue("SPT_DELETE_CONCURRENCY"), 10);
var objectSize = envValue("SPT_DELETE_OBJECT_SIZE");
var seedCount = parseInt(envValue("SPT_DELETE_SEED_COUNT"), 10);
var batchSize = parseInt(envValue("SPT_DELETE_BATCH_SIZE"), 10);
var driver = envValue("SPT_DELETE_DRIVER");
var bucketPath = "/" + envValue("SPT_DELETE_BUCKET");
var prefix = envValue("SPT_DELETE_PREFIX");
var duration = envValue("SPT_DELETE_DURATION");
var durationMode = duration !== "";
var verify = envValue("SPT_DELETE_VERIFY") === "true";
var cleanup = envValue("SPT_DELETE_CLEANUP") === "true";
var verificationTimeoutMillis = parseInt(envValue("SPT_DELETE_VERIFICATION_TIMEOUT_MS"), 10);
var homeDir = org.apache.logging.log4j.ThreadContext.get("home_dir");
var seedStep = envValue("SPT_DELETE_SEED_STEP");
var deleteStep = envValue("SPT_DELETE_DELETE_STEP");
var cleanupStep = envValue("SPT_DELETE_CLEANUP_STEP");
var writtenFile = homeDir + "/log/" + seedStep + "/written.csv";
var residualFile = homeDir + "/log/" + deleteStep + "/items.csv";
var setupStartedNanos = com.dell.spt.base.load.step.DurationTime.monotonicEpochNanos();
var seedStartedNanos = System.nanoTime();

CreateLoad.config({
  "storage": {
    "driver": {"type": driver, "limit": {"concurrency": concurrency}},
    "integrity": {
      "mode": "metadata", "algorithm": "sha256",
      "input": {"provenance": "none", "expectedProducerId": ""},
      "output": {"requireExactCount": true}
    }
  },
  "item": {
    "type": "data",
    "data": {"size": objectSize},
    "naming": {"prefix": prefix},
    "output": {"path": bucketPath, "file": writtenFile}
  },
  "load": {
    "op": {"type": "create", "limit": {"count": seedCount}},
    "step": {"id": seedStep}
  },
  "output": {"metrics": {"summary": {"persist": true}}}
}).run();

var seedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - seedStartedNanos);
var selection = com.dell.spt.base.item.op.deletion.StandaloneDeleteSelection.fromManifest(writtenFile);
var deleteStepConfig = {"id": deleteStep};
if (durationMode) {
  deleteStepConfig["limit"] = {"time": duration};
}

var benchmarkFailure = null;
try {
  DeleteLoad.config({
    "storage": {
      "driver": {"type": driver, "limit": {"concurrency": concurrency}},
      "integrity": {
        "mode": "metadata", "algorithm": "sha256",
        "input": {"provenance": "engine_step", "expectedProducerId": seedStep}
      }
    },
    "item": {"type": "data", "input": {"file": writtenFile}},
    "load": {
      "batch": {"size": batchSize},
      "op": {
        "type": "delete",
        "delete": {
          "standalone": true,
          "batchSize": batchSize,
          "duration": durationMode,
          "selectionOrder": "canonical",
          "selected": selection.selected(),
          "selectedCurrentKey": selection.selectedCurrentKey(),
          "selectedExactVersion": selection.selectedExactVersion(),
          "selectedBuckets": selection.selectedBuckets(),
          "seedMillis": seedMillis,
          "workflowStartedEpochNanos": setupStartedNanos,
          "preValidation": false,
          "postVerification": verify,
          "verificationTimeoutMillis": verificationTimeoutMillis
        },
        "failureBudget": {
          "mode": "fixed", "maxFailedObjects": 0,
          "maxFailurePercent": 0.0, "graceSeconds": 0
        },
        "recycle": {"mode": false},
        "retry": false,
        "wait": {"finish": true}
      },
      "step": deleteStepConfig
    },
    "output": {"metrics": {"summary": {"persist": true}}}
  }).run();
} catch (failure) {
  com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer.rethrowIfInterrupted(failure);
  benchmarkFailure = failure;
}

if (cleanup) {
  var cleanupFailure = null;
  var cleanupLoad = null;
  var cleanupStartedNanos = System.nanoTime();
  try {
    cleanupLoad = DeleteLoad.config({
      "storage": {"driver": {"type": driver, "limit": {"concurrency": concurrency}}},
      "item": {"type": "data", "input": {"file": residualFile}},
      "load": {
        "op": {
          "type": "delete",
          "limit": {"fail": {"count": 0, "rate": false}},
          "retry": false,
          "wait": {"finish": true}
        },
        "step": {"id": cleanupStep}
      },
      "output": {"metrics": {"summary": {"persist": true}}}
    });
    cleanupLoad.run();
  } catch (failure) {
    cleanupFailure = failure;
  }
  var cleanupNanos = System.nanoTime() - cleanupStartedNanos;
  com.dell.spt.base.item.op.deletion.SeededDeleteCleanupFinalizer.finish(
      cleanupStep, cleanupNanos, benchmarkFailure, cleanupFailure, cleanupLoad, residualFile);
} else if (benchmarkFailure !== null) {
  throw benchmarkFailure;
}
SCENARIO

export SPT_HOME="$run_dir/spt-home"
export SPT_DELETE_CONCURRENCY="$DELETE_CONCURRENCY"
export SPT_DELETE_OBJECT_SIZE="$DELETE_OBJECT_SIZE"
export SPT_DELETE_SEED_COUNT="$seed_count"
export SPT_DELETE_BATCH_SIZE="$DELETE_BATCH_SIZE"
export SPT_DELETE_DRIVER="$S3_DRIVER"
export SPT_DELETE_BUCKET="$S3_BUCKET"
export SPT_DELETE_PREFIX="$DELETE_PREFIX"
export SPT_DELETE_DURATION="$DELETE_DURATION"
if enabled "$DELETE_VERIFY"; then
	export SPT_DELETE_VERIFY=true
else
	export SPT_DELETE_VERIFY=false
fi
if enabled "$DELETE_CLEANUP"; then
	export SPT_DELETE_CLEANUP=true
else
	export SPT_DELETE_CLEANUP=false
fi
export SPT_DELETE_VERIFICATION_TIMEOUT_MS="$DELETE_VERIFICATION_TIMEOUT_MS"
export SPT_DELETE_SEED_STEP="testdelete-$run_stamp-1-seed"
export SPT_DELETE_DELETE_STEP="testdelete-$run_stamp-2-delete"
export SPT_DELETE_CLEANUP_STEP="testdelete-$run_stamp-3-cleanup"

node_addrs="${S3_ENDPOINTS//http:\/\/}"
node_addrs="${node_addrs//https:\/\/}"
java_args=()
if [[ -n "$SPT_JAVA_OPTS" ]]; then
	read -r -a java_args <<< "$SPT_JAVA_OPTS"
fi

echo "=== Direct spt.jar Seeded DELETE Test ==="
echo "JAR: $SPT_JAR"
echo "Endpoints: configured"
echo "Bucket: $S3_BUCKET"
echo "Prefix: $DELETE_PREFIX"
echo "Driver: $S3_DRIVER"
if [[ -n "$DELETE_DURATION" ]]; then
	echo "Mode: duration=$DELETE_DURATION  SeedObjects: $seed_count"
else
	echo "Mode: object-count=$seed_count"
fi
echo "Concurrency: $DELETE_CONCURRENCY  ObjectSize: $DELETE_OBJECT_SIZE  DeleteBatchSize: $DELETE_BATCH_SIZE"
echo "Verify: $DELETE_VERIFY  Cleanup: $DELETE_CLEANUP"
echo "Results: $run_dir"
echo

"$JAVA_BIN" "${java_args[@]}" -jar "$SPT_JAR" \
	--run-id="$DELETE_RUN_ID" \
	--run-scenario="$scenario_file" \
	--storage-net-node-addrs="$node_addrs" \
	--storage-auth-version="$S3_AUTH_VERSION" \
	--storage-auth-uid="$S3_ACCESS_KEY" \
	--storage-auth-secret="$S3_SECRET_KEY" \
	"${EXTRA_ARGS[@]}" 2>&1 | tee "$run_dir/console.log"

echo
echo "=== Direct spt.jar Seeded DELETE Test Complete ==="
echo "Results preserved in: $run_dir"
