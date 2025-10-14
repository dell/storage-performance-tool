#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$REPO_ROOT/bundle/build/dist/run.sh"

# Allow callers to override defaults via environment variables.
: "${S3_ENDPOINTS:=http://10.247.70.222:9020,http://10.247.70.223:9020,http://10.247.70.225:9020}"
: "${S3_ACCESS_KEY:=spt}"
: "${S3_SECRET_KEY:=nT2YxHnCxDVmsuFCzvOFWxLR73P8zDZWP8B7rwsU}"
: "${S3_BUCKET:=mh-testwrites}"
: "${LIST_CONCURRENCY:=1}"
: "${LIST_PAGE_SIZE:=1000}"
: "${LIST_OP_LIMIT_COUNT:=0}"
: "${LIST_PREFIX:=}"
# New delimiter-first knobs (script drives only what's needed by the algorithm)
: "${LIST_MODE:=}"
: "${LIST_SEED_CHARSET:=0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ}"
# Delimiter-first algorithm knobs (wired to CLI)
: "${LIST_DELIMITERS:=/-_.}"
: "${LIST_SPLIT_PAGES:=50}"
: "${LIST_QUEUE_MAX:=10000000}"
: "${LIST_DISABLE_TIMING_FILES:=true}"
# Optional run timebox (seconds). If set, wraps invocation with GNU timeout.
: "${LIST_TIMEOUT:=}"
# Optional internal step time limit (e.g. 120s). Takes effect even without GNU timeout.
: "${LIST_STEP_TIME_LIMIT:=}"
# Optional HTTP timeout override in milliseconds for S3 requests
# Default 5 minutes to accommodate slow delimiter seeding and large buckets
: "${LIST_HTTP_TIMEOUT_MS:=300000}"

# Strip URL schemes because Spt expects host:port entries for node addresses.
NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

CLI_ARGS=(
  "--storage-driver-type=s3"
  "--storage-net-node-addrs=${NODE_ADDRS}"
  "--storage-net-ssl-enabled=false"
  "--storage-auth-uid=${S3_ACCESS_KEY}"
  "--storage-auth-secret=${S3_SECRET_KEY}"
  "--storage-auth-version=4"
  "--log-level=info"
  "--item-type=path"
  "--item-input-path=/${S3_BUCKET}"
  "--load-op-type=list"
  "--storage-driver-limit-concurrency=${LIST_CONCURRENCY}"
  "--load-batch-size=${LIST_PAGE_SIZE}"
  "--load-op-list-max_keys=${LIST_PAGE_SIZE}"
  "--load-op-limit-count=${LIST_OP_LIMIT_COUNT}"
)

# Apply step time limit if requested
if [[ -n "${LIST_STEP_TIME_LIMIT}" ]]; then
  CLI_ARGS+=("--load-step-limit-time=${LIST_STEP_TIME_LIMIT}")
fi

# Determine sharding mode default: AUTO when concurrency>1 else NONE
EFFECTIVE_MODE="${LIST_MODE}"
if [[ -z "${EFFECTIVE_MODE}" ]]; then
  if (( LIST_CONCURRENCY > 1 )); then
    EFFECTIVE_MODE="auto"
  else
    EFFECTIVE_MODE="none"
  fi
fi
CLI_ARGS+=("--load-op-list-sharding-mode=${EFFECTIVE_MODE}")

# Continue assembling remaining CLI args. Keep to broadly supported keys.
CLI_ARGS+=("--load-op-list-sharding-charset=${LIST_SEED_CHARSET}")

# Wire delimiter-first knobs through to the CLI
CLI_ARGS+=("--load-op-list-sharding-delimiters=${LIST_DELIMITERS}")
CLI_ARGS+=("--load-op-list-sharding-split-pages=${LIST_SPLIT_PAGES}")
CLI_ARGS+=("--load-op-list-sharding-queue_max=${LIST_QUEUE_MAX}")

if [[ "${LIST_DISABLE_TIMING_FILES}" == "true" || "${LIST_DISABLE_TIMING_FILES}" == "1" ]]; then
  CLI_ARGS+=("--output-metrics-timing-persist=false")
fi

if [[ -n "${LIST_PREFIX}" ]]; then
  CLI_ARGS+=("--item-naming-prefix=${LIST_PREFIX}")
fi

# Optional HTTP timeout override
if [[ -n "${LIST_HTTP_TIMEOUT_MS}" ]]; then
  CLI_ARGS+=("--storage-net-timeoutMilliSec=${LIST_HTTP_TIMEOUT_MS}")
fi

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi

declare -a REMAPPED_ARGS=()
for arg in "$@"; do
	case "${arg,,}" in
		--debug)
			REMAPPED_ARGS+=("--log-level=debug")
			;;
		--log-level=debug)
			REMAPPED_ARGS+=("--log-level=debug")
			;;
		--log-level=*)
			REMAPPED_ARGS+=("$arg")
			;;
		*)
			REMAPPED_ARGS+=("$arg")
			;;
	esac
done

# If LIST_TIMEOUT is set, wrap with GNU timeout for safety
if [[ -n "${LIST_TIMEOUT}" ]]; then
  exec timeout -k 5 "${LIST_TIMEOUT}" "$RUN_SCRIPT" "${CLI_ARGS[@]}" "${REMAPPED_ARGS[@]}"
else
  exec "$RUN_SCRIPT" "${CLI_ARGS[@]}" "${REMAPPED_ARGS[@]}"
fi
