#!/usr/bin/env bash
#
# RDMA write test script - performs 60 seconds of 1MB object writes using s3-rdma driver
# V3: Uses direct libibverbs for RDMA acceleration (no CUDA required)
#
set -euo pipefail

# Require Java 21
export JAVA_HOME="${JAVA_HOME:-/opt/java}"
export PATH="$JAVA_HOME/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# V3 native library path (direct libibverbs, no CUDA)
NATIVE_V3_DIR="${REPO_ROOT}/extensions/storage-drivers/implementations/s3-rdma/src/main/native/build_v3"

# Legacy V2/V1 paths (for fallback reference)
NATIVE_V2_DIR="${REPO_ROOT}/extensions/storage-drivers/implementations/s3-rdma/src/main/native/build"
CUOBJ_V1_DIR="${CUOBJ_V1_DIR:-$HOME/repos/rdma-object-client/cuobj}"

# Determine which native library to use
NATIVE_LIB_DIR=""
NATIVE_LIB_NAME=""

if [[ -f "${NATIVE_V3_DIR}/libspt_rdma_v3.so" ]]; then
  NATIVE_LIB_DIR="${NATIVE_V3_DIR}"
  NATIVE_LIB_NAME="spt_rdma_v3"
  echo "Using V3 native library (libibverbs): ${NATIVE_LIB_DIR}" >&2
elif [[ -f "${NATIVE_V2_DIR}/libspt_rdma_native.so" ]]; then
  NATIVE_LIB_DIR="${NATIVE_V2_DIR}"
  NATIVE_LIB_NAME="spt_rdma_native"
  echo "Using V2 native library: ${NATIVE_LIB_DIR}" >&2
else
  echo "Warning: No native RDMA library found - will run in HTTP fallback mode" >&2
fi

# Set up LD_LIBRARY_PATH for rdma-core libraries
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/lib64${NATIVE_LIB_DIR:+:$NATIVE_LIB_DIR}"

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
: "${RDMA_DEVICE:=auto}"
: "${RDMA_POOL_SIZE:=64}"
: "${RDMA_BUFFER_SIZE:=33554432}"

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
if [[ -n "${NATIVE_LIB_DIR:-}" ]]; then
  JAVA_OPTS="$JAVA_OPTS -Djava.library.path=${NATIVE_LIB_DIR}"
fi

# Check for RDMA hardware
echo "=== RDMA Hardware Check ===" >&2
if command -v ibv_devinfo &>/dev/null; then
  if ibv_devinfo 2>/dev/null | grep -q "PORT_ACTIVE"; then
    echo "RDMA device found and active" >&2
    ibv_devinfo 2>/dev/null | grep -E "hca_id|port:|state:" | head -6 >&2
  else
    echo "Warning: No active RDMA ports found" >&2
  fi
else
  echo "Warning: ibv_devinfo not found - cannot verify RDMA hardware" >&2
fi

echo "" >&2
echo "=== RDMA Write Test (V3 libibverbs) ===" >&2
echo "Endpoints:   ${S3_ENDPOINTS}" >&2
echo "Bucket:      ${S3_BUCKET}" >&2
echo "Concurrency: ${RDMA_CONCURRENCY}" >&2
echo "Object Size: ${RDMA_OBJECT_SIZE}" >&2
echo "Duration:    ${RDMA_TIME_LIMIT}" >&2
echo "Threshold:   ${RDMA_THRESHOLD_BYTES} bytes" >&2
echo "Device:      ${RDMA_DEVICE}" >&2
echo "Pool Size:   ${RDMA_POOL_SIZE}" >&2
echo "Native lib:  ${NATIVE_LIB_DIR:-not found}" >&2
echo "==========================================" >&2

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
