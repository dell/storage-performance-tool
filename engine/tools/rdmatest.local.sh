#!/usr/bin/env bash
#
# RDMA write test script - performs 60 seconds of 1MB object writes using s3-rdma driver
# Uses direct libibverbs for RDMA acceleration (no CUDA, no GPU required)
#
# Prerequisites:
#   - rdma-core packages installed (libibverbs, libmlx5, librdmacm)
#   - Mellanox ConnectX-4+ NIC with active port
#   - libspt_rdma.so built (cmake && make in src/main/native/)
#   - SPT distribution built (./gradlew :bundle:build)
#
# Usage:
#   ./tools/rdmatest.local.sh                    # defaults
#   RDMA_LOCAL_IP=10.1.2.3 ./tools/rdmatest.local.sh  # specify RDMA interface IP
#   ./tools/rdmatest.local.sh --load-op-type=read      # pass extra SPT args
#
set -euo pipefail

# Require Java 21
export JAVA_HOME="${JAVA_HOME:-/opt/java}"
export PATH="$JAVA_HOME/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Native library location (direct libibverbs, no CUDA)
NATIVE_LIB_DIR="${REPO_ROOT}/extensions/storage-drivers/implementations/s3-rdma/src/main/native/build"

if [[ ! -f "${NATIVE_LIB_DIR}/libspt_rdma.so" ]]; then
  echo "Error: Native library not found at ${NATIVE_LIB_DIR}/libspt_rdma.so" >&2
  echo "Build it with: cd extensions/storage-drivers/implementations/s3-rdma/src/main/native && mkdir -p build && cd build && cmake .. && make" >&2
  exit 1
fi
echo "Using native library: ${NATIVE_LIB_DIR}" >&2

# Set up LD_LIBRARY_PATH for rdma-core libraries
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/lib64:${NATIVE_LIB_DIR}"

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

# RDMA specific defaults
: "${RDMA_CONCURRENCY:=16}"
: "${RDMA_OBJECT_SIZE:=1MB}"
: "${RDMA_TIME_LIMIT:=60s}"
: "${RDMA_THRESHOLD_BYTES:=1048576}"
: "${RDMA_FALLBACK_ENABLED:=true}"
: "${RDMA_DEVICE:=auto}"
: "${RDMA_LOCAL_IP:=}"
: "${RDMA_LOG_LEVEL:=WARN}"

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
JAVA_OPTS="$JAVA_OPTS -Djava.library.path=${NATIVE_LIB_DIR}"

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
echo "=== RDMA Write Test ===" >&2
echo "Endpoints:   ${S3_ENDPOINTS}" >&2
echo "Bucket:      ${S3_BUCKET}" >&2
echo "Concurrency: ${RDMA_CONCURRENCY}" >&2
echo "Object Size: ${RDMA_OBJECT_SIZE}" >&2
echo "Duration:    ${RDMA_TIME_LIMIT}" >&2
echo "Threshold:   ${RDMA_THRESHOLD_BYTES} bytes" >&2
echo "Device:      ${RDMA_DEVICE}" >&2
echo "Local IP:    ${RDMA_LOCAL_IP:-auto}" >&2
echo "Log Level:   ${RDMA_LOG_LEVEL}" >&2
echo "Native lib:  ${NATIVE_LIB_DIR}" >&2
echo "==========================================" >&2

# Build RDMA CLI args
RDMA_ARGS=(
  "--storage-rdma-threshold-bytes=${RDMA_THRESHOLD_BYTES}"
  "--storage-rdma-fallback-enabled=${RDMA_FALLBACK_ENABLED}"
  "--storage-rdma-device=${RDMA_DEVICE}"
  "--storage-rdma-log-level=${RDMA_LOG_LEVEL}"
)
if [[ -n "${RDMA_LOCAL_IP}" ]]; then
  RDMA_ARGS+=("--storage-rdma-local-ip=${RDMA_LOCAL_IP}")
fi

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
  "${RDMA_ARGS[@]}" \
  "$@"
