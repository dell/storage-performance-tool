#!/usr/bin/env bash
#
# RDMA write test script - performs 60 seconds of 1MB object writes using s3-rdma driver
# V2: Uses NVIDIA cuObject from CUDA 13.1.1+ for RDMA acceleration
#
set -euo pipefail

# Require Java 21
export JAVA_HOME="${JAVA_HOME:-/opt/java}"
export PATH="$JAVA_HOME/bin:$PATH"

# CUDA toolkit path for cuObject (V2 mode)
CUDA_HOME="${CUDA_HOME:-/usr/local/cuda-13.1}"

# V1 cuObject standalone library path (no CUDA runtime required)
CUOBJ_V1_DIR="${CUOBJ_V1_DIR:-$HOME/repos/rdma-object-client/cuobj}"

# Native library paths for RDMA support
NATIVE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../extensions/storage-drivers/implementations/s3-rdma/src/main/native/build" 2>/dev/null && pwd)" || true

# Set up LD_LIBRARY_PATH for both V1 and V2 modes
if [[ -d "${CUOBJ_V1_DIR}/lib" ]]; then
  # V1 mode: Use standalone cuObject library
  export LD_LIBRARY_PATH="${CUOBJ_V1_DIR}/lib:${LD_LIBRARY_PATH:-}${NATIVE_LIB_DIR:+:$NATIVE_LIB_DIR}"
  # Use V1 cuFile configuration if available
  if [[ -f "${CUOBJ_V1_DIR}/cuobj.json" ]]; then
    export CUFILE_ENV_PATH_JSON="${CUOBJ_V1_DIR}/cuobj.json"
    echo "Using V1 cuObject config: ${CUFILE_ENV_PATH_JSON}" >&2
  fi
  echo "Using V1 cuObject library from: ${CUOBJ_V1_DIR}/lib" >&2
else
  # V2 mode: Use CUDA 13.1+ cuObject
  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:${CUDA_HOME}/targets/x86_64-linux/lib:${CUDA_HOME}/lib64${NATIVE_LIB_DIR:+:$NATIVE_LIB_DIR}"
fi

# Set java.library.path to find spt_rdma_native
if [[ -n "${NATIVE_LIB_DIR:-}" && -f "${NATIVE_LIB_DIR}/libspt_rdma_native.so" ]]; then
  export SPT_JAVA_OPTS="${SPT_JAVA_OPTS:-} -Djava.library.path=${NATIVE_LIB_DIR}"
  echo "Using native RDMA library from: ${NATIVE_LIB_DIR}" >&2
else
  echo "Warning: Native RDMA library not found - will run in HTTP fallback mode" >&2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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

SPT_JAR="$REPO_ROOT/bundle/build/dist/spt.jar"
EXT_DIR="$REPO_ROOT/bundle/build/dist/ext"

if [[ ! -f "$SPT_JAR" ]]; then
  echo "SPT jar not found at $SPT_JAR; building distribution..." >&2
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi

# Set up extension directory
USER_EXT_DIR="$HOME/.spt/5.1.1/ext"
mkdir -p "$USER_EXT_DIR"
if [[ -d "$EXT_DIR" ]]; then
  for ext in "$EXT_DIR"/*.jar; do
    [[ -f "$ext" ]] && ln -sf "$ext" "$USER_EXT_DIR/" 2>/dev/null || true
  done
fi

# Build JVM options
JAVA_OPTS="-XX:MaxDirectMemorySize=2g -Xshare:off"
if [[ -n "${NATIVE_LIB_DIR:-}" && -f "${NATIVE_LIB_DIR}/libspt_rdma_native.so" ]]; then
  JAVA_OPTS="$JAVA_OPTS -Djava.library.path=${NATIVE_LIB_DIR}"
fi

echo "=== RDMA Write Test (V2 cuObject) ===" >&2
echo "Endpoints:   ${S3_ENDPOINTS}" >&2
echo "Bucket:      ${S3_BUCKET}" >&2
echo "Concurrency: ${RDMA_CONCURRENCY}" >&2
echo "Object Size: ${RDMA_OBJECT_SIZE}" >&2
echo "Duration:    ${RDMA_TIME_LIMIT}" >&2
echo "Threshold:   ${RDMA_THRESHOLD_BYTES} bytes" >&2
echo "Native lib:  ${NATIVE_LIB_DIR:-not found}" >&2
echo "======================================" >&2

exec java $JAVA_OPTS -jar "$SPT_JAR" \
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
