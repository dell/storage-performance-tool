#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$ENGINE_ROOT/bundle/build/dist/run.sh"

# Configuration - can be overridden via environment variables
S3_ENDPOINT="${S3_ENDPOINT:-http://10.246.190.64:8333}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-admin123}"
S3_BUCKET="${S3_BUCKET:-spttest}"
S3_AUTH_VERSION="${S3_AUTH_VERSION:-4}"
TEST_DURATION_MINUTES="${TEST_DURATION_MINUTES:-1}"

# Test matrix from story requirements
OBJECT_SIZES=("10K" "1M")
OPERATIONS=("create" "read" "mixed")
CONCURRENCY_LEVELS=(1 8 32)

# Output files
OUTPUT_DIR="${OUTPUT_DIR:-./s3-aws-performance-results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="${OUTPUT_DIR}/comparison_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

TSV_FILE="${RESULTS_DIR}/results.tsv"
SUMMARY_FILE="${RESULTS_DIR}/summary.md"

# Item files for read operations (create phase saves object lists, read phase uses them)
ITEM_FILES_DIR="${RESULTS_DIR}/item_files"
mkdir -p "$ITEM_FILES_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "S3-AWS Performance Comparison Script"
echo "=========================================="
echo "S3 Endpoint: $S3_ENDPOINT"
echo "Bucket: $S3_BUCKET"
echo "Test Duration: ${TEST_DURATION_MINUTES} minutes per test"
echo ""
echo "Test Matrix:"
echo "  Object Sizes: ${OBJECT_SIZES[*]}"
echo "  Operations: ${OPERATIONS[*]}"
echo "  Concurrency Levels: ${CONCURRENCY_LEVELS[*]}"
echo ""
echo "Deterministic Object Naming with Item Files:"
echo "  Phase 1: Populate data with create operations, save object lists to item files"
echo "  Phase 2: Test all operations (create, read, mixed) using saved item files"
echo "  Naming: --item-naming-type=serial --item-naming-seed=0 --item-naming-prefix=perf_{size}_"
echo "  Read: Use --item-input-file with --load-op-recycle-mode=true"
echo "  Mixed: Sequential create+read operations (50/50 mix approximation)"
echo ""
echo "Results Directory: $RESULTS_DIR"
echo "=========================================="
echo ""

# Check if run script exists
if [[ ! -x "$RUN_SCRIPT" ]]; then
    echo -e "${YELLOW}Run script not found at $RUN_SCRIPT; building distribution...${NC}" >&2
    (cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi

# Create TSV file with header
echo -e "operation\tobject_size\tconcurrency\tthroughput_ops\tmb_per_sec\tlatency_mean_ms\tlatency_p50_ms\tlatency_p75_ms\tlatency_p95_ms\ttotal_ops\ttotal_errors\tduration_seconds" > "$TSV_FILE"

# Function to run test
run_test() {
    local op=$1
    local size=$2
    local concurrency=$3
    local phase=$4  # "phase1" for data population, "phase2" for testing
    local test_name="${op}_${size}_t${concurrency}"

    echo -e "${BLUE}Running: $test_name${NC}"

    # Convert endpoint format
    NODE_ADDRS="${S3_ENDPOINT//http:\/\/}"
    NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

    # Handle mixed operations with sequential create+read approach
    if [[ "$op" == "mixed" && "$phase" == "phase2" ]]; then
        # Mixed workload: run half-duration create + half-duration read
        local mixed_duration=$((TEST_DURATION_MINUTES / 2))
        if [[ $mixed_duration -lt 1 ]]; then
            mixed_duration=1
        fi

        echo -e "${YELLOW}  Mixed: Running ${mixed_duration}min create + ${mixed_duration}min read${NC}"

        # Create item file for mixed operations
        local mixed_item_file="${ITEM_FILES_DIR}/mixed_${size}_t${concurrency}.csv"

        # Run create phase and save item file
        local create_cmd=("$RUN_SCRIPT" \
            --storage-driver-type=s3-aws \
            --storage-net-node-addrs="${NODE_ADDRS}" \
            --storage-auth-version="${S3_AUTH_VERSION}" \
            --storage-auth-uid="${S3_ACCESS_KEY}" \
            --storage-auth-secret="${S3_SECRET_KEY}" \
            --item-output-path="/${S3_BUCKET}" \
            --item-data-size="${size}" \
            --storage-driver-limit-concurrency="${concurrency}" \
            --load-step-limit-time="${mixed_duration}m" \
            --item-naming-type=serial \
            --item-naming-seed=0 \
            --item-naming-prefix="mixed_${size}_" \
            --item-output-file="${mixed_item_file}" \
            --load-op-type=create)

        local create_output
        create_output=$("${create_cmd[@]}" 2>&1)
        echo "$create_output" > "${RESULTS_DIR}/${test_name}_create_raw.log"

        # Run read phase using the item file from create phase
        local read_cmd=("$RUN_SCRIPT" \
            --storage-driver-type=s3-aws \
            --storage-net-node-addrs="${NODE_ADDRS}" \
            --storage-auth-version="${S3_AUTH_VERSION}" \
            --storage-auth-uid="${S3_ACCESS_KEY}" \
            --storage-auth-secret="${S3_SECRET_KEY}" \
            --item-output-path="/${S3_BUCKET}" \
            --item-data-size="${size}" \
            --storage-driver-limit-concurrency="${concurrency}" \
            --load-step-limit-time="${mixed_duration}m" \
            --item-naming-type=serial \
            --item-naming-seed=0 \
            --item-naming-prefix="mixed_${size}_" \
            --item-input-file="${mixed_item_file}" \
            --load-op-recycle-mode=true \
            --load-op-type=read)

        local read_output
        read_output=$("${read_cmd[@]}" 2>&1)
        echo "$read_output" > "${RESULTS_DIR}/${test_name}_read_raw.log"

        # Combine results for mixed metrics
        local create_throughput=$(echo "$create_output" | awk '/Throughput \[op\/s\]:/{found=1} found && /Mean:/{print $2; exit}' | tr -d ',' || echo "0")
        local create_bandwidth=$(echo "$create_output" | awk '/Bandwidth \[MB\/s\]:/{found=1} found && /Mean:/{print $2; exit}' | tr -d ',' || echo "0")
        local create_latency=$(echo "$create_output" | awk '/Operations Duration \[us\]:/{found=1} found && /Avg:/{print $NF; exit}' | tr -d ',' || echo "0")
        local create_ops=$(echo "$create_output" | awk '/Successful:/{print $NF; exit}' | tr -d ',' || echo "0")
        local create_errors=$(echo "$create_output" | awk '/Failed:/{print $NF; exit}' | tr -d ',' || echo "0")

        local read_throughput=$(echo "$read_output" | awk '/Throughput \[op\/s\]:/{found=1} found && /Mean:/{print $2; exit}' | tr -d ',' || echo "0")
        local read_bandwidth=$(echo "$read_output" | awk '/Bandwidth \[MB\/s\]:/{found=1} found && /Mean:/{print $2; exit}' | tr -d ',' || echo "0")
        local read_latency=$(echo "$read_output" | awk '/Operations Duration \[us\]:/{found=1} found && /Avg:/{print $NF; exit}' | tr -d ',' || echo "0")
        local read_ops=$(echo "$read_output" | awk '/Successful:/{print $NF; exit}' | tr -d ',' || echo "0")
        local read_errors=$(echo "$read_output" | awk '/Failed:/{print $NF; exit}' | tr -d ',' || echo "0")

        # Calculate mixed metrics (weighted average)
        local total_duration=$((mixed_duration * 2))
        local throughput=$(awk "BEGIN {printf \"%.3f\", ($create_throughput + $read_throughput) / 2}")
        local bandwidth=$(awk "BEGIN {printf \"%.3f\", ($create_bandwidth + $read_bandwidth) / 2}")
        local latency_us=$(awk "BEGIN {printf \"%.0f\", ($create_latency + $read_latency) / 2}")
        local latency_ms=$(awk "BEGIN {printf \"%.3f\", $latency_us / 1000}")
        local total_ops=$((create_ops + read_ops))
        local total_errors=$((create_errors + read_errors))

        # Extract percentiles from create (write) phase for mixed
        local latency_p50=$(echo "$create_output" | awk '/Operations Duration \[us\]:/{found=1} found && /Quantile 0.5:/{print $NF; exit}' | tr -d ',' || echo "0")
        local latency_p75=$(echo "$create_output" | awk '/Operations Duration \[us\]:/{found=1} found && /Quantile 0.75:/{print $NF; exit}' | tr -d ',' || echo "0")
        local latency_p95=$(echo "$create_output" | awk '/Operations Duration \[us\]:/{found=1} found && /Quantile 0.95:/{print $NF; exit}' | tr -d ',' || echo "0")

        # Convert latency percentiles to milliseconds
        if [[ -n "$latency_p50" && "$latency_p50" != "0" ]]; then
            latency_p50=$(awk "BEGIN {printf \"%.3f\", $latency_p50 / 1000}")
        fi
        if [[ -n "$latency_p75" && "$latency_p75" != "0" ]]; then
            latency_p75=$(awk "BEGIN {printf \"%.3f\", $latency_p75 / 1000}")
        fi
        if [[ -n "$latency_p95" && "$latency_p95" != "0" ]]; then
            latency_p95=$(awk "BEGIN {printf \"%.3f\", $latency_p95 / 1000}")
        fi

        echo "  Results: ${throughput} ops/s, ${bandwidth} MB/s, ${latency_ms}ms mean latency, ${total_ops} ops, ${total_errors} errors"
        echo "  Create: ${create_throughput} ops/s, ${create_ops} ops, ${create_errors} errors"
        echo "  Read: ${read_throughput} ops/s, ${read_ops} ops, ${read_errors} errors"

        # Write to TSV file
        echo -e "${op}\t${size}\t${concurrency}\t${throughput}\t${bandwidth}\t${latency_ms}\t${latency_p50}\t${latency_p75}\t${latency_p95}\t${total_ops}\t${total_errors}\t${total_duration}" >> "$TSV_FILE"
        return
    fi

    # Build command based on operation type
    local cmd=("$RUN_SCRIPT" \
        --storage-driver-type=s3-aws \
        --storage-net-node-addrs="${NODE_ADDRS}" \
        --storage-auth-version="${S3_AUTH_VERSION}" \
        --storage-auth-uid="${S3_ACCESS_KEY}" \
        --storage-auth-secret="${S3_SECRET_KEY}" \
        --item-output-path="/${S3_BUCKET}" \
        --item-data-size="${size}" \
        --storage-driver-limit-concurrency="${concurrency}" \
        --load-step-limit-time="${TEST_DURATION_MINUTES}m" \
        --item-naming-type=serial \
        --item-naming-seed=0 \
        --item-naming-prefix="perf_${size}_")

    # Handle item files for Phase 1 data population and Phase 2 read operations
    if [[ "$phase" == "phase1" ]]; then
        # Phase 1: Save object list for later use in read operations
        local item_file="${ITEM_FILES_DIR}/items_${size}.csv"
        cmd+=("--item-output-file=${item_file}")
    elif [[ "$phase" == "phase2" ]]; then
        # Phase 2: Use saved item files for read operations
        local item_file="${ITEM_FILES_DIR}/items_${size}.csv"
        if [[ "$op" == "read" ]]; then
            if [[ -f "$item_file" ]]; then
                cmd+=("--item-input-file=${item_file}")
                cmd+=("--load-op-recycle-mode=true")
            else
                echo -e "${RED}Warning: Item file not found: $item_file${NC}"
                echo -e "${RED}Read operations may fail without object list${NC}"
            fi
        fi
    fi

    # Set operation type
    cmd+=(--load-op-type="$op")

    # Run the test and capture output
    local output
    output=$("${cmd[@]}" 2>&1)

    # Save raw output for debugging
    echo "$output" > "${RESULTS_DIR}/${test_name}_raw.log"

    # Extract metrics using more robust parsing
    # Extract throughput (Mean value from Throughput section)
    local throughput=$(echo "$output" | awk '/Throughput \[op\/s\]:/{found=1} found && /Mean:/{print $2; exit}' | tr -d ',' || echo "0")

    # Extract bandwidth (Mean value from Bandwidth section)
    local bandwidth=$(echo "$output" | awk '/Bandwidth \[MB\/s\]:/{found=1} found && /Mean:/{print $2; exit}' | tr -d ',' || echo "0")

    # Extract latency metrics from Operations Duration section
    local latency_mean=$(echo "$output" | awk '/Operations Duration \[us\]:/{found=1} found && /Avg:/{print $NF; exit}' | tr -d ',' || echo "0")
    local latency_p50=$(echo "$output" | awk '/Operations Duration \[us\]:/{found=1} found && /Quantile 0.5:/{print $NF; exit}' | tr -d ',' || echo "0")
    local latency_p75=$(echo "$output" | awk '/Operations Duration \[us\]:/{found=1} found && /Quantile 0.75:/{print $NF; exit}' | tr -d ',' || echo "0")
    local latency_p95=$(echo "$output" | awk '/Operations Duration \[us\]:/{found=1} found && /Quantile 0.95:/{print $NF; exit}' | tr -d ',' || echo "0")

    # Extract operation counts
    local total_ops=$(echo "$output" | awk '/Successful:/{print $NF; exit}' | tr -d ',' || echo "0")
    local total_errors=$(echo "$output" | awk '/Failed:/{print $NF; exit}' | tr -d ',' || echo "0")
    local duration=$(echo "$output" | awk '/Elapsed:/{print $NF; exit}' | tr -d ',' || echo "0")

    # Convert latency from microseconds to milliseconds
    if [[ -n "$latency_mean" && "$latency_mean" != "0" ]]; then
        latency_mean=$(awk "BEGIN {printf \"%.3f\", $latency_mean / 1000}")
    fi
    if [[ -n "$latency_p50" && "$latency_p50" != "0" ]]; then
        latency_p50=$(awk "BEGIN {printf \"%.3f\", $latency_p50 / 1000}")
    fi
    if [[ -n "$latency_p75" && "$latency_p75" != "0" ]]; then
        latency_p75=$(awk "BEGIN {printf \"%.3f\", $latency_p75 / 1000}")
    fi
    if [[ -n "$latency_p95" && "$latency_p95" != "0" ]]; then
        latency_p95=$(awk "BEGIN {printf \"%.3f\", $latency_p95 / 1000}")
    fi

    # Handle missing values
    throughput=${throughput:-0}
    bandwidth=${bandwidth:-0}
    latency_mean=${latency_mean:-0}
    latency_p50=${latency_p50:-0}
    latency_p75=${latency_p75:-0}
    latency_p95=${latency_p95:-0}
    total_ops=${total_ops:-0}
    total_errors=${total_errors:-0}
    duration=${duration:-0}

    echo "  Results: ${throughput} ops/s, ${bandwidth} MB/s, ${latency_mean}ms mean latency, ${total_ops} ops, ${total_errors} errors"

    # Write to TSV file
    echo -e "${op}\t${size}\t${concurrency}\t${throughput}\t${bandwidth}\t${latency_mean}\t${latency_p50}\t${latency_p75}\t${latency_p95}\t${total_ops}\t${total_errors}\t${duration}" >> "$TSV_FILE"
}

# Function to generate summary markdown
generate_summary() {
    local start_time=$1
    local end_time=$2
    
    cat > "$SUMMARY_FILE" << EOF
# S3-AWS Performance Comparison Results

**Test Date:** $(date)
**S3 Endpoint:** $S3_ENDPOINT
**Bucket:** $S3_BUCKET
**Test Duration:** ${TEST_DURATION_MINUTES} minutes per test

## Test Configuration

- **Object Sizes:** ${OBJECT_SIZES[*]}
- **Operations:** ${OPERATIONS[*]}
- **Concurrency Levels:** ${CONCURRENCY_LEVELS[*]}
- **Total Tests:** $((${#OBJECT_SIZES[*]} * ${#OPERATIONS[*]} * ${#CONCURRENCY_LEVELS[*]}))

## Test Approach

**Phase 1:** Data population with create operations (32 threads, deterministic serial naming, save object lists to item files)
**Phase 2:** Full test matrix for all operations and concurrency levels (read uses saved item files with recycle mode, mixed uses sequential create+read)

## Results Summary

### Raw Results

\`\`\`tsv
operation\tobject_size\tconcurrency\tthroughput_ops\tmb_per_sec\tlatency_mean_ms\tlatency_p50_ms\tlatency_p75_ms\tlatency_p95_ms\ttotal_ops\ttotal_errors\tduration_seconds
$(tail -n +2 "$TSV_FILE")
\`\`\`

### Performance by Object Size

#### 10K Objects (Small Object Workload)

| Operation | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) | P50 Latency (ms) | P75 Latency (ms) | P95 Latency (ms) |
|-----------|-------------|-------------------|-----------------|------------------|-----------------|-----------------|-----------------|
$(grep "10K" "$TSV_FILE" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n", $1, $3, $4, $5, $6, $7, $8, $9}')

#### 1M Objects (Large Object Workload)

| Operation | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) | P50 Latency (ms) | P75 Latency (ms) | P95 Latency (ms) |
|-----------|-------------|-------------------|-----------------|------------------|-----------------|-----------------|-----------------|
$(grep "1M" "$TSV_FILE" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n", $1, $3, $4, $5, $6, $7, $8, $9}')

### Performance by Operation

#### Create Operations

| Object Size | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) |
|-------------|-------------|-------------------|-----------------|------------------|
$(grep "^create" "$TSV_FILE" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s |\n", $2, $3, $4, $5, $6}')

#### Read Operations

| Object Size | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) |
|-------------|-------------|-------------------|-----------------|------------------|
$(grep "^read" "$TSV_FILE" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s |\n", $2, $3, $4, $5, $6}')

#### Mixed Operations

| Object Size | Concurrency | Throughput (ops/s) | Bandwidth (MB/s) | Mean Latency (ms) |
|-------------|-------------|-------------------|-----------------|------------------|
$(grep "^mixed" "$TSV_FILE" | awk -F'\t' '{printf "| %s | %s | %s | %s | %s |\n", $2, $3, $4, $5, $6}')

### Key Observations

<!-- Add manual analysis here based on the results -->

### Test Environment

- **SPT Version:** $(cd "$ENGINE_ROOT" && git describe --tags --always 2>/dev/null || echo "unknown")
- **Branch:** $(cd "$ENGINE_ROOT" && git branch --show-current 2>/dev/null || echo "unknown")
- **Commit:** $(cd "$ENGINE_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
- **Test Start:** $start_time
- **Test End:** $end_time
- **Total Duration:** $(( ($(date +%s) - $(date -d "$start_time" +%s)) / 60 )) minutes

### Raw Data Files

- Full results: \`results.tsv\`
- Individual test logs: \`*_raw.log\`

EOF
}

# Record start time
START_TIME=$(date)
START_TIMESTAMP=$(date +%s)

echo "Starting performance comparison tests..."
echo ""

# Phase 1: Populate data with create operations
echo -e "${YELLOW}=========================================="
echo "PHASE 1: Populating Data"
echo "==========================================${NC}"
echo ""

for size in "${OBJECT_SIZES[@]}"; do
    echo -e "${YELLOW}Populating data for ${size} objects...${NC}"

    # Use highest concurrency to populate quickly
    run_test "create" "$size" 32 "phase1"
    echo ""

    # Short cooldown
    sleep 2
done

echo -e "${GREEN}Data population complete!${NC}"
echo ""

# Phase 2: Test all operations
echo -e "${YELLOW}=========================================="
echo "PHASE 2: Testing All Operations"
echo "==========================================${NC}"
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

            run_test "$op" "$size" "$concurrency" "phase2"
            echo ""

            # Optional: cooldown between tests
            # sleep 2
        done
    done
done

# Record end time
END_TIME=$(date)
END_TIMESTAMP=$(date +%s)

# Generate summary
echo "Generating summary report..."
generate_summary "$START_TIME" "$END_TIME"

echo ""
echo "=========================================="
echo "Performance Comparison Complete!"
echo "=========================================="
echo "Results Directory: $RESULTS_DIR"
echo "Summary Report: $SUMMARY_FILE"
echo "Raw Results: $TSV_FILE"
echo ""
echo "Total Tests Run: $((total_tests + ${#OBJECT_SIZES[*]}))"  # +2 for Phase 1 population
echo "Total Duration: $(( (END_TIMESTAMP - START_TIMESTAMP) / 60 )) minutes"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo "1. Review the summary report: cat $SUMMARY_FILE"
echo "2. Analyze results for performance bottlenecks"
echo "3. Use this as your 'before' baseline for optimization validation"
echo ""
