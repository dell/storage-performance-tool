#!/usr/bin/env bash
#
# RDMA write test script - performs 60 seconds of 1MB object writes using s3-rdma driver
#
set -euo pipefail

# Require Java 21
export JAVA_HOME="${JAVA_HOME:-/opt/java}"
export PATH="$JAVA_HOME/bin:$PATH"

# RDMA plugin paths (for cuObject RDMA provider)
export CUFILE_ENV_PATH_JSON="${CUFILE_ENV_PATH_JSON:-/home/mike/repos/rdma-object-client/cuobj/cuobj.json}"
export CUOBJ_S3_PLUGIN="${CUOBJ_S3_PLUGIN:-/home/mike/repos/aws-c-s3/plugins/cuobject/build/libcuobject_s3_plugin.so}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/home/mike/repos/rdma-object-client/cuobj/lib"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$REPO_ROOT/bundle/build/dist/run.sh"

# Load repo-local environment defaults if present
if [[ -f "$REPO_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
fi

# Default configuration - can be overridden via environment or .env file
: "${S3_ENDPOINTS:=http://127.0.0.1:9020}"
: "${S3_ACCESS_KEY:=AWS_ACCESS_KEY_ID_EXAMPLE}"
: "${S3_SECRET_KEY:=AWS_SECRET_ACCESS_KEY_EXAMPLE}"
: "${S3_BUCKET:=rdmatest}"
: "${S3_AUTH_VERSION:=4}"

# RDMA test specific defaults
: "${RDMA_CONCURRENCY:=16}"
: "${RDMA_OBJECT_SIZE:=1MB}"
: "${RDMA_TIME_LIMIT:=60s}"
: "${RDMA_THRESHOLD_BYTES:=1048576}"
: "${RDMA_FALLBACK_ENABLED:=true}"

# Convert comma-separated endpoints into host:port pairs expected by Spt
NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi

echo "=== RDMA Write Test ===" >&2
echo "Endpoints:   ${S3_ENDPOINTS}" >&2
echo "Bucket:      ${S3_BUCKET}" >&2
echo "Concurrency: ${RDMA_CONCURRENCY}" >&2
echo "Object Size: ${RDMA_OBJECT_SIZE}" >&2
echo "Duration:    ${RDMA_TIME_LIMIT}" >&2
echo "Threshold:   ${RDMA_THRESHOLD_BYTES} bytes" >&2
echo "=======================" >&2

exec "$RUN_SCRIPT" \
  --storage-driver-type=s3-rdma \
  --storage-net-node-addrs="${NODE_ADDRS}" \
  --storage-auth-version="${S3_AUTH_VERSION}" \
  --storage-auth-uid="${S3_ACCESS_KEY}" \
  --storage-auth-secret="${S3_SECRET_KEY}" \
  --item-output-path="/${S3_BUCKET}" \
  --storage-driver-limit-concurrency="${RDMA_CONCURRENCY}" \
  --item-data-size="${RDMA_OBJECT_SIZE}" \
  --load-step-limit-time="${RDMA_TIME_LIMIT}" \
  "$@"
