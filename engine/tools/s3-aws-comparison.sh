#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$ENGINE_ROOT/bundle/build/dist/run.sh"

# Configuration
S3_ENDPOINT="${S3_ENDPOINT:-http://your-s3-endpoint:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-your-access-key}"
S3_SECRET_KEY="${S3_SECRET_KEY:-your-secret-key}"
S3_BUCKET="${S3_BUCKET:-your-bucket}"
S3_AUTH_VERSION="${S3_AUTH_VERSION:-4}"
OBJECT_SIZE="${OBJECT_SIZE:-1K}"
OPERATION_COUNT="${OPERATION_COUNT:-5000}"

# Thread counts to test
THREAD_COUNTS=(1 4 8 32)

# Operations to test
OPERATIONS=("write" "read")

# Output file
OUTPUT_FILE="${OUTPUT_FILE:-s3-aws-comparison-results.tsv}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "S3-AWS Performance Comparison Script"
echo "===================================="
echo "S3 Endpoint: $S3_ENDPOINT"
echo "Object Size: $OBJECT_SIZE"
echo "Operations per test: $OPERATION_COUNT"
echo "Thread counts: ${THREAD_COUNTS[*]}"
echo "Output file: $OUTPUT_FILE"
echo ""

# Check if run script exists
if [[ ! -x "$RUN_SCRIPT" ]]; then
    echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
    (cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi

# Create output file with header
echo -e "operation\tthreads\tthroughput_ops\tmedian_latency_ms\tfailures" > "$OUTPUT_FILE"

# Function to run test
run_test() {
    local op=$1
    local threads=$2
    
    echo "Running $op test with $threads thread(s)..."
    
    # Convert endpoint format
    NODE_ADDRS="${S3_ENDPOINT//http:\/\/}"
    NODE_ADDRS="${NODE_ADDRS//https:\/\/}"
    
    # Run the test
    local result
    result=$($RUN_SCRIPT \
        --storage-driver-type=s3-aws \
        --storage-net-node-addrs="${NODE_ADDRS}" \
        --storage-auth-version="${S3_AUTH_VERSION}" \
        --storage-auth-uid="${S3_ACCESS_KEY}" \
        --storage-auth-secret="${S3_SECRET_KEY}" \
        --item-output-path="/${S3_BUCKET}" \
        --item-data-size="${OBJECT_SIZE}" \
        --load-op-type="${op}" \
        --load-op-limit-count="${OPERATION_COUNT}" \
        --storage-driver-limit-concurrency="${threads}" \
        2>&1)
    
    # Extract metrics from output
    local throughput=$(echo "$result" | grep -A 20 "# Results" | grep "Mean:" | awk '{print $4}' | tr -d ',')
    local latency=$(echo "$result" | grep -A 20 "# Results" | grep "Avg:" | awk '{print $3}' | tr -d ',')
    local failures=$(echo "$result" | grep -A 20 "# Results" | grep "Failed:" | awk '{print $2}')
    
    # Convert latency from microseconds to milliseconds
    if [[ -n "$latency" ]]; then
        latency=$(echo "scale=3; $latency / 1000" | bc)
    fi
    
    # Handle missing values
    throughput=${throughput:-0}
    latency=${latency:-0}
    failures=${failures:-0}
    
    echo "  Throughput: ${throughput} ops/s, Latency: ${latency}ms, Failures: ${failures}"
    
    # Write to output file
    echo -e "${op}\t${threads}\t${throughput}\t${latency}\t${failures}" >> "$OUTPUT_FILE"
}

# Run tests for each operation and thread count
for op in "${OPERATIONS[@]}"; do
    echo ""
    echo "Testing operation: $op"
    echo "--------------------"
    
    for threads in "${THREAD_COUNTS[@]}"; do
        run_test "$op" "$threads"
        echo ""
    done
done

echo ""
echo "===================================="
echo "Comparison complete!"
echo "Results saved to: $OUTPUT_FILE"
echo ""
echo "Summary:"
cat "$OUTPUT_FILE"
