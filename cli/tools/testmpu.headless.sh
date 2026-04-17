#!/usr/bin/env bash
#
# Script to run a multipart upload write test with the new MPU concurrency limits
# in headless mode. 
#
# This script sets:
#   - 10 total objects, 25MB each
#   - 1MB part sizes (triggering MPU)
#   - 8 global driver threads
#   - 2 max concurrent MPU objects
#   - 3 max concurrent parts per MPU object
#
# Usage:
#   ./tools/testmpu.headless.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SPT_BIN="$ROOT_DIR/spt"

if [[ ! -x "$SPT_BIN" ]]; then
  echo "SPT binary not found at $SPT_BIN. Please run 'make build' in the cli directory." >&2
  exit 1
fi

# Read caller environment overrides
declare -A _caller_env
for _v in SPT_IMAGE SPT_SKIP_IMAGE_PULL S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

if [[ -f "$ROOT_DIR/../.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/../.env"
fi

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v


: "${S3_ENDPOINTS:=http://127.0.0.1:9000}"
: "${S3_ACCESS_KEY:=admin}"
: "${S3_SECRET_KEY:=admin123}"
: "${S3_BUCKET:=spttest}"

RESULTS_DIR="$SCRIPT_DIR/results/mpu-headless-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo "=========================================================================="
echo " Running Headless MPU Workload with Concurrency Limits"
echo "=========================================================================="
echo " - Object Count: 10"
echo " - Object Size: 25MB"
echo " - Part Size: 5MB"
echo " - Global Threads: 8"
echo " - Max Concurrent MPU Objects: 2"
echo " - Max Concurrent MPU Parts per Object: 3"
echo " - Endpoint: $S3_ENDPOINTS"
echo " - Results: $RESULTS_DIR"
echo "=========================================================================="

"$SPT_BIN" run write \
  --endpoints "$S3_ENDPOINTS" \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket "$S3_BUCKET" \
  --prefix "mpu-limit-test/" \
  --object-count 10 \
  --object-size 25MB \
  --part-size 5MB \
  --threads 8 \
  --mpu-concurrent-objects 2 \
  --mpu-concurrent-parts 3 \
  --headless \
  --output-dir "$RESULTS_DIR" \
  "$@"

echo ""
echo "Test completed. Results saved to $RESULTS_DIR"
