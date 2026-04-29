#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"
RUN_SCRIPT="$ENGINE_ROOT/bundle/build/dist/run.sh"

# Snapshot any variables the caller already exported so that sourcing the
# .env file (which uses plain assignments) does not clobber them.
declare -A _caller_env
for _v in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_AUTH_VERSION \
          JARTEST_CONCURRENCY JARTEST_OBJECT_SIZE JARTEST_OP_LIMIT_COUNT JARTEST_TIME_LIMIT_MINS; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

# Load repo-local environment defaults if present so users can keep
# credentials and tuning knobs out of the script itself.
# Check the git repo root first (canonical location), then engine root.
if [[ -f "$GIT_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$GIT_ROOT/.env"
elif [[ -f "$ENGINE_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ENGINE_ROOT/.env"
fi

# Restore caller-provided overrides so they take precedence over .env.
for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

# Fall back to built-in defaults for anything still unset.
: "${S3_ENDPOINTS:=http://127.0.0.1:9000}"
: "${S3_ACCESS_KEY:=AWS_ACCESS_KEY_ID_EXAMPLE}"
: "${S3_SECRET_KEY:=AWS_SECRET_ACCESS_KEY_EXAMPLE}"
: "${S3_BUCKET:=testwrites}"
: "${S3_AUTH_VERSION:=4}"
: "${JARTEST_CONCURRENCY:=4}"
: "${JARTEST_OBJECT_SIZE:=1MB}"
: "${JARTEST_OP_LIMIT_COUNT:=5000}"

# If time limit is explicitly set, set operations limit to 0 (infinite) unless explicitly defined
if [[ -n "${JARTEST_TIME_LIMIT_MINS:-}" ]]; then
  JARTEST_OP_LIMIT_COUNT=0
fi

# Convert comma-separated endpoints into host:port pairs expected by Spt.
NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi

echo "Using S3-AWS driver with legacy S3 parameters..." >&2

EXTRA_ARGS=()
if [[ -n "${JARTEST_TIME_LIMIT_MINS:-}" ]]; then
  EXTRA_ARGS+=("--load-step-limit-time=${JARTEST_TIME_LIMIT_MINS}m")
fi

exec "$RUN_SCRIPT" \
  --storage-driver-type=s3-aws \
  --storage-net-node-addrs="${NODE_ADDRS}" \
  --storage-auth-version="${S3_AUTH_VERSION}" \
  --storage-auth-uid="${S3_ACCESS_KEY}" \
  --storage-auth-secret="${S3_SECRET_KEY}" \
  --item-output-path="/${S3_BUCKET}" \
  --item-data-size="${JARTEST_OBJECT_SIZE}" \
  --load-op-limit-count="${JARTEST_OP_LIMIT_COUNT}" \
  --storage-driver-limit-concurrency="${JARTEST_CONCURRENCY}" \
  "${EXTRA_ARGS[@]}" \
  "$@"
