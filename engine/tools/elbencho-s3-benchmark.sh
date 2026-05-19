#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ELBENCHO_BIN="${ENGINE_ROOT}/../elbencho/bin/elbencho"

# Configuration - matching SPT baseline
S3_ENDPOINT="${S3_ENDPOINT:-http://10.246.190.64:8333}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-admin123}"
S3_BUCKET="${S3_BUCKET:-spttest}"
S3_REGION="${S3_REGION:-us-east-1}"
TEST_DURATION_SECONDS="${TEST_DURATION_SECONDS:-60}"

# Test matrix from story requirements
OBJECT_SIZES=("10K" "1M")
OPERATIONS=("create" "read" "mixed")
CONCURRENCY_LEVELS=(1 8 32)

# Output files
OUTPUT_DIR="${OUTPUT_DIR:-./elbencho-s3-results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="${OUTPUT_DIR}/comparison_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

TSV_FILE="${RESULTS_DIR}/results.tsv"
SUMMARY_FILE="${RESULTS_DIR}/summary.md"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Elbencho S3 Benchmark Script"
echo "=========================================="
echo "S3 Endpoint: $S3_ENDPOINT"
echo "Bucket: $S3_BUCKET"
echo "Test Duration: ${TEST_DURATION_SECONDS} seconds per test"
echo ""
echo "Test Matrix:"
echo "  Object Sizes: ${OBJECT_SIZES[*]}"
echo "  Operations: ${OPERATIONS[*]}"
echo "  Concurrency Levels: ${CONCURRENCY_LEVELS[*]}"
echo ""
echo "Elbencho Configuration:"
echo "  S3 + AWS CRT support enabled"
echo "  Latency statistics enabled"
echo "  Matching SPT baseline methodology"
echo ""
echo "Results Directory: $RESULTS_DIR"
echo "=========================================="
echo ""

# Check if elbencho binary exists
if [[ ! -x "$ELBENCHO_BIN" ]]; then
    echo -e "${RED}Error: Elbencho binary not found at $ELBENCHO_BIN${NC}" >&2
    echo "Please build elbencho with S3 support first" >&2
    exit 1
fi

# Function to convert object size to elbencho format
convert_size() {
    local size=$1
    case $size in
        "10K")
            echo "10K"
            ;;
        "1M")
            echo "1M"
            ;;
        *)
            echo "$size"
            ;;
    esac
}

# Create TSV file with header
echo -e "operation\tobject_size\tconcurrency\tthroughput_ops\tmb_per_sec\tlatency_mean_ms\tlatency_min_ms\tlatency_max_ms\ttotal_ops\tduration_seconds" > "$TSV_FILE"

# Function to run test
run_test() {
    local op=$1
    local size=$2
    local concurrency=$3
    local test_name="${op}_${size}_t${concurrency}"
    
    echo -e "${BLUE}Running: $test_name${NC}"
    
    # Convert size format
    local size_fmt=$(convert_size "$size")
    
    # Calculate number of objects to create sufficient workload for ~1 minute
    # Using conservative estimates based on SPT baseline
    local target_ops_per_second
    case $size in
        "10K")
            case $op in
                "create") target_ops_per_second=300 ;;
                "read") target_ops_per_second=350 ;;
                "mixed") target_ops_per_second=350 ;;
            esac
            ;;
        "1M")
            case $op in
                "create") target_ops_per_second=70 ;;
                "read") target_ops_per_second=150 ;;
                "mixed") target_ops_per_second=100 ;;
            esac
            ;;
    esac
    
    local total_target_ops=$((target_ops_per_second * TEST_DURATION_SECONDS))
    local num_files_per_dir=$((total_target_ops / concurrency))
    
    # Ensure minimum files and reasonable maximum
    if [[ $num_files_per_dir -lt 1000 ]]; then
        num_files_per_dir=1000
    fi
    if [[ $num_files_per_dir -gt 50000 ]]; then
        num_files_per_dir=50000
    fi
    
    local num_dirs=1
    
    # Build base command
    local cmd=("$ELBENCHO_BIN" \
        --s3endpoints "$S3_ENDPOINT" \
        --s3key "$S3_ACCESS_KEY" \
        --s3secret "$S3_SECRET_KEY" \
        --s3region "$S3_REGION" \
        --lat \
        -t "$concurrency" \
        -n "$num_dirs" \
        -N "$num_files_per_dir" \
        -s "$size_fmt")
    
    # Add operation type
    if [[ "$op" == "create" ]]; then
        cmd+=(-w)
    elif [[ "$op" == "read" ]]; then
        cmd+=(-r)
    elif [[ "$op" == "mixed" ]]; then
        # For mixed, we'll run separate create and read phases
        echo -e "${YELLOW}  Mixed: Running create phase${NC}"
        
        # Create phase
        local create_cmd=("${cmd[@]}" -w)
        local create_output
        create_output=$("${create_cmd[@]}" "s3://${S3_BUCKET}/elbencho_${size}_mixed_create" 2>&1)
        echo "$create_output" > "${RESULTS_DIR}/${test_name}_create_raw.log"
        
        # Read phase
        echo -e "${YELLOW}  Mixed: Running read phase${NC}"
        local read_cmd=("${cmd[@]}" -r)
        local read_output
        read_output=$("${read_cmd[@]}" "s3://${S3_BUCKET}/elbencho_${size}_mixed_create" 2>&1)
        echo "$read_output" > "${RESULTS_DIR}/${test_name}_read_raw.log"
        
        # Parse create results
        local create_throughput=$(echo "$create_output" | grep "Objects/s" | awk '{print $3}' || echo "0")
        local create_bw=$(echo "$create_output" | grep "Throughput MiB/s" | awk '{print $4}' || echo "0")
        local create_latency=$(echo "$create_output" | grep "Objects latency" | sed 's/.*avg=\([0-9.]*\)ms.*/\1/' || echo "0")
        local create_latency_min=$(echo "$create_output" | grep "Objects latency" | sed 's/.*min=\([0-9.]*\)ms.*/\1/' || echo "0")
        local create_latency_max=$(echo "$create_output" | grep "Objects latency" | sed 's/.*max=\([0-9.]*\)ms.*/\1/' || echo "0")
        local create_total=$(echo "$create_output" | grep "Objects total" | awk '{print $4}' || echo "0")
        
        # Parse read results
        local read_throughput=$(echo "$read_output" | grep "Objects/s" | awk '{print $3}' || echo "0")
        local read_bw=$(echo "$read_output" | grep "Throughput MiB/s" | awk '{print $4}' || echo "0")
        local read_latency=$(echo "$read_output" | grep "Objects latency" | sed 's/.*avg=\([0-9.]*\)ms.*/\1/' || echo "0")
        local read_latency_min=$(echo "$read_output" | grep "Objects latency" | sed 's/.*min=\([0-9.]*\)ms.*/\1/' || echo "0")
        local read_latency_max=$(echo "$read_output" | grep "Objects latency" | sed 's/.*max=\([0-9.]*\)ms.*/\1/' || echo "0")
        local read_total=$(echo "$read_output" | grep "Objects total" | awk '{print $4}' || echo "0")
        
        # Calculate mixed metrics (average)
        local throughput=$(awk "BEGIN {printf \"%.3f\", ($create_throughput + $read_throughput) / 2}")
        local bandwidth=$(awk "BEGIN {printf \"%.3f\", ($create_bw + $read_bw) / 2}")
        local latency=$(awk "BEGIN {printf \"%.3f\", ($create_latency + $read_latency) / 2}")
        local latency_min=$(awk "BEGIN {printf \"%.3f\", ($create_latency_min + $read_latency_min) / 2}")
        local latency_max=$(awk "BEGIN {printf \"%.3f\", ($create_latency_max + $read_latency_max) / 2}")
        local total_ops=$((create_total + read_total))
        local duration=$((TEST_DURATION_SECONDS * 2))
        
        echo "  Results: ${throughput} ops/s, ${bandwidth} MB/s, ${latency}ms mean latency, ${total_ops} ops"
        echo "  Create: ${create_throughput} ops/s, ${create_bw} MB/s, ${create_latency}ms latency, ${create_total} ops"
        echo "  Read: ${read_throughput} ops/s, ${read_bw} MB/s, ${read_latency}ms latency, ${read_total} ops"
        
        # Write to TSV file
        echo -e "${op}\t${size}\t${concurrency}\t${throughput}\t${bandwidth}\t${latency}\t${latency_min}\t${latency_max}\t${total_ops}\t${duration}" >> "$TSV_FILE"
        return
    fi
    
    # Run the test and capture output
    local output
    local test_path="s3://${S3_BUCKET}/elbencho_${size}_${op}"
    output=$("${cmd[@]}" "$test_path" 2>&1)
    
    # Save raw output for debugging
    echo "$output" > "${RESULTS_DIR}/${test_name}_raw.log"
    
    # Extract metrics from elbencho output
    local throughput=$(echo "$output" | grep "Objects/s" | awk '{print $3}' || echo "0")
    local bandwidth=$(echo "$output" | grep "Throughput MiB/s" | awk '{print $4}' || echo "0")
    local latency_avg=$(echo "$output" | grep "Objects latency" | sed 's/.*avg=\([0-9.]*\)ms.*/\1/' || echo "0")
    local latency_min=$(echo "$output" | grep "Objects latency" | sed 's/.*min=\([0-9.]*\)ms.*/\1/' || echo "0")
    local latency_max=$(echo "$output" | grep "Objects latency" | sed 's/.*max=\([0-9.]*\)ms.*/\1/' || echo "0")
    local total_ops=$(echo "$output" | grep "Objects total" | awk '{print $4}' || echo "0")
    
    # Handle missing values
    throughput=${throughput:-0}
    bandwidth=${bandwidth:-0}
    latency_avg=${latency_avg:-0}
    latency_min=${latency_min:-0}
    latency_max=${latency_max:-0}
    total_ops=${total_ops:-0}
    duration=$TEST_DURATION_SECONDS
    
    echo "  Results: ${throughput} ops/s, ${bandwidth} MB/s, ${latency_avg}ms mean latency, ${total_ops} ops"
    
    # Write to TSV file
    echo -e "${op}\t${size}\t${concurrency}\t${throughput}\t${bandwidth}\t${latency_avg}\t${latency_min}\t${latency_max}\t${total_ops}\t${duration}" >> "$TSV_FILE"
}

# Record start time
START_TIME=$(date)
START_TIMESTAMP=$(date +%s)

echo "Starting Elbencho S3 benchmark tests..."
echo ""

# Run tests for each combination
total_tests=$((${#OBJECT_SIZES[*]} * ${#OPERATIONS[*]} * ${#CONCURRENCY_LEVELS[*]}))
current_test=0

for size in "${OBJECT_SIZES[@]}"; do
    echo -e "${YELLOW}Testing object size: $size${NC}"
    echo "--------------------"
    
    for op in "${OPERATIONS[@]}"; do
        echo "Operation: $op"
        
        for concurrency in "${CONCURRENCY_LEVELS[@]}"; do
            current_test=$((current_test + 1))
            echo -e "[$current_test/$total_tests] Concurrency: $concurrency"
            
            run_test "$op" "$size" "$concurrency"
            echo ""
            
            # Short cooldown between tests
            sleep 2
        done
    done
done

# Record end time
END_TIME=$(date)
END_TIMESTAMP=$(date +%s)

echo ""
echo "=========================================="
echo "Elbencho S3 Benchmark Complete!"
echo "=========================================="
echo "Results Directory: $RESULTS_DIR"
echo "Raw Results: $TSV_FILE"
echo ""
echo "Total Tests Run: $total_tests"
echo "Total Duration: $(( (END_TIMESTAMP - START_TIMESTAMP) / 60 )) minutes"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo "1. Review the raw results: cat $TSV_FILE"
echo "2. Compare with SPT baseline results"
echo "3. Update comparison document with Elbencho data"
echo ""