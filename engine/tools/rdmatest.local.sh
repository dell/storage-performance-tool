#!/usr/bin/env bash
#
# RDMA test script - performs object writes and/or reads using s3-rdma driver
# Uses direct libibverbs for RDMA acceleration (no CUDA, no GPU required)
#
# Prerequisites:
#   - rdma-core packages installed (libibverbs, libmlx5, librdmacm)
#   - Mellanox ConnectX-4+ NIC with active port
#   - libspt_rdma.so built (cmake && make in src/main/native/)
#   - SPT distribution built (./gradlew :bundle:build)
#
# Usage:
#   ./tools/rdmatest.local.sh                          # write then read (default)
#   ./tools/rdmatest.local.sh write                    # write only
#   ./tools/rdmatest.local.sh read                     # read only (objects must exist)
#   RDMA_LOCAL_IP=10.1.2.3 ./tools/rdmatest.local.sh  # specify RDMA interface IP
#   RDMA_CONCURRENCY=32 ./tools/rdmatest.local.sh      # override concurrency
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

# Save any command-line env overrides before sourcing .env
# (.env uses unconditional assignment, so we restore overrides afterward)
declare -A _cli_overrides=()
for _var in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_AUTH_VERSION \
            RDMA_CONCURRENCY RDMA_OBJECT_SIZE RDMA_TIME_LIMIT RDMA_THRESHOLD_BYTES \
            RDMA_FALLBACK_ENABLED RDMA_DEVICE RDMA_LOCAL_IP RDMA_LOG_LEVEL; do
  if [[ -n "${!_var+set}" ]]; then
    _cli_overrides[$_var]="${!_var}"
  fi
done

# Load repo-local environment defaults if present
if [[ -f "$REPO_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
fi

# Restore command-line overrides (take precedence over .env)
for _var in "${!_cli_overrides[@]}"; do
  export "$_var=${_cli_overrides[$_var]}"
done
unset _cli_overrides _var

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
: "${RDMA_FALLBACK_ENABLED:=false}"
: "${RDMA_DEVICE:=auto}"
: "${RDMA_LOCAL_IP:=}"
: "${RDMA_LOG_LEVEL:=WARN}"

# Parse test mode from first positional arg
MODE="${1:-both}"
shift 2>/dev/null || true

case "$MODE" in
  write|read|both) ;;
  -h|--help)
    echo "Usage: $0 [write|read|both] [extra SPT args...]"
    echo ""
    echo "Modes:"
    echo "  write  - Create objects via RDMA PUT"
    echo "  read   - Read objects via RDMA GET (objects must already exist)"
    echo "  both   - Write then read (default)"
    echo ""
    echo "Environment variables:"
    echo "  S3_ENDPOINTS         Comma-separated endpoints (default: http://127.0.0.1:9020)"
    echo "  S3_BUCKET            Bucket name (default: rdmatest)"
    echo "  RDMA_CONCURRENCY     Parallel operations (default: 16)"
    echo "  RDMA_OBJECT_SIZE     Object size (default: 1MB)"
    echo "  RDMA_TIME_LIMIT      Duration per phase (default: 60s)"
    echo "  RDMA_LOCAL_IP        Local RDMA interface IP (default: auto)"
    echo "  RDMA_LOG_LEVEL       Native log level (default: WARN)"
    exit 0
    ;;
  *)
    echo "Unknown mode: $MODE (expected: write, read, or both)" >&2
    exit 1
    ;;
esac

# Convert comma-separated endpoints into host:port pairs expected by Spt
NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

SPT_JAR="$REPO_ROOT/bundle/build/dist/spt.jar"
EXT_DIR="$REPO_ROOT/bundle/build/dist/ext"

if [[ ! -f "$SPT_JAR" ]]; then
  echo "SPT jar not found at $SPT_JAR; building distribution..." >&2
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi


# Build JVM options
JAVA_OPTS="-XX:MaxDirectMemorySize=4g -Xshare:off"
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

# Build RDMA CLI args (shared between write and read)
# Note: config keys use camelCase (not dashes) because '-' is the CLI path separator
RDMA_ARGS=(
  "--storage-rdma-thresholdBytes=${RDMA_THRESHOLD_BYTES}"
  "--storage-rdma-fallback=${RDMA_FALLBACK_ENABLED}"
  "--storage-rdma-device=${RDMA_DEVICE}"
  "--storage-rdma-logLevel=${RDMA_LOG_LEVEL}"
)
if [[ -n "${RDMA_LOCAL_IP}" ]]; then
  RDMA_ARGS+=("--storage-rdma-localIp=${RDMA_LOCAL_IP}")
fi

# Items file for passing created items from write to read phase
RESULTS_DIR="${SCRIPT_DIR}/results/rdmatest-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
ITEMS_FILE="${RESULTS_DIR}/items.csv"

echo "== Results: ${RESULTS_DIR} ==" >&2

# Common SPT args
COMMON_ARGS=(
  --storage-driver-type=s3-rdma
  "--storage-net-node-addrs=${NODE_ADDRS}"
  "--storage-auth-version=${S3_AUTH_VERSION}"
  "--storage-auth-uid=${S3_ACCESS_KEY}"
  "--storage-auth-secret=${S3_SECRET_KEY}"
  "--storage-driver-limit-concurrency=${RDMA_CONCURRENCY}"
  "--item-data-size=${RDMA_OBJECT_SIZE}"
  "--load-step-limit-time=${RDMA_TIME_LIMIT}"
  "${RDMA_ARGS[@]}"
)

print_header() {
  local op_label="$1"
  echo "" >&2
  echo "=== RDMA ${op_label} Test ===" >&2
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
}

run_write() {
  print_header "Write (PUT)"
  java $JAVA_OPTS -jar "$SPT_JAR" \
    "${COMMON_ARGS[@]}" \
    "--item-output-path=/${S3_BUCKET}" \
    "--item-output-file=${ITEMS_FILE}" \
    "$@" 2>&1 | tee "${RESULTS_DIR}/write.log"
}

run_read() {
  local items_arg=()
  if [[ -f "$ITEMS_FILE" ]]; then
    local count
    count=$(wc -l < "$ITEMS_FILE")
    echo "Using items file: ${ITEMS_FILE} (${count} items)" >&2
    items_arg=("--item-input-file=${ITEMS_FILE}")
  else
    echo "No items file found, listing from bucket /${S3_BUCKET}" >&2
    items_arg=("--item-input-path=/${S3_BUCKET}")
  fi
  print_header "Read (GET)"
  java $JAVA_OPTS -jar "$SPT_JAR" \
    "${COMMON_ARGS[@]}" \
    --load-op-type=read \
    "${items_arg[@]}" \
    "$@" 2>&1 | tee "${RESULTS_DIR}/read.log"
}

case "$MODE" in
  write)
    run_write "$@"
    ;;
  read)
    run_read "$@"
    ;;
  both)
    run_write "$@"
    echo "" >&2
    echo "========== Write phase complete, starting read phase ==========" >&2
    run_read "$@"
    ;;
esac

echo "== Results saved to: ${RESULTS_DIR} =="
