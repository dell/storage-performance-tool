#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ELBENCHO_BIN="${ENGINE_ROOT}/../elbencho/bin/elbencho"

# Configuration - matching SPT baseline
S3_ENDPOINT="${S3_ENDPOINT:-http://your-s3-endpoint:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-your-access-key}"
S3_SECRET_KEY="${S3_SECRET_KEY:-your-secret-key}"
S3_BUCKET="${S3_BUCKET:-your-bucket}"
S3_REGION="${S3_REGION:-us-east-1}"

# Output files
OUTPUT_DIR="${OUTPUT_DIR:-./elbencho-s3-results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="${OUTPUT_DIR}/simple_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Simple Elbencho S3 Benchmark"
echo "=========================================="
echo "S3 Endpoint: $S3_ENDPOINT"
echo "Bucket: $S3_BUCKET"
echo ""
echo "Running key performance tests to establish Elbencho baseline..."
echo "Results Directory: $RESULTS_DIR"
echo "=========================================="
echo ""

# Test 1: 10K objects, create, high concurrency
echo -e "${BLUE}Test 1: 10K objects, create, 32 threads${NC}"
$ELBENCHO_BIN \
    --s3endpoints "$S3_ENDPOINT" \
    --s3key "$S3_ACCESS_KEY" \
    --s3secret "$S3_SECRET_KEY" \
    --s3region "$S3_REGION" \
    --lat \
    -t 32 \
    -n 1 \
    -N 10000 \
    -s 10K \
    -w \
    "s3://${S3_BUCKET}/test_10k" > "${RESULTS_DIR}/elbencho_10K_create_t32.log" 2>&1

echo -e "${GREEN}Test 1 complete${NC}"
echo ""

# Test 2: 10K objects, read, high concurrency  
echo -e "${BLUE}Test 2: 10K objects, read, 32 threads${NC}"
$ELBENCHO_BIN \
    --s3endpoints "$S3_ENDPOINT" \
    --s3key "$S3_ACCESS_KEY" \
    --s3secret "$S3_SECRET_KEY" \
    --s3region "$S3_REGION" \
    --lat \
    -t 32 \
    -n 1 \
    -N 10000 \
    -s 10K \
    -r \
    "s3://${S3_BUCKET}/test_10k" > "${RESULTS_DIR}/elbencho_10K_read_t32.log" 2>&1

echo -e "${GREEN}Test 2 complete${NC}"
echo ""

# Test 3: 1M objects, create, high concurrency
echo -e "${BLUE}Test 3: 1M objects, create, 32 threads${NC}"
$ELBENCHO_BIN \
    --s3endpoints "$S3_ENDPOINT" \
    --s3key "$S3_ACCESS_KEY" \
    --s3secret "$S3_SECRET_KEY" \
    --s3region "$S3_REGION" \
    --lat \
    -t 32 \
    -n 1 \
    -N 1000 \
    -s 1M \
    -w \
    "s3://${S3_BUCKET}/test_1M" > "${RESULTS_DIR}/elbencho_1M_create_t32.log" 2>&1

echo -e "${GREEN}Test 3 complete${NC}"
echo ""

# Test 4: 1M objects, read, high concurrency
echo -e "${BLUE}Test 4: 1M objects, read, 32 threads${NC}"
$ELBENCHO_BIN \
    --s3endpoints "$S3_ENDPOINT" \
    --s3key "$S3_ACCESS_KEY" \
    --s3secret "$S3_SECRET_KEY" \
    --s3region "$S3_REGION" \
    --lat \
    -t 32 \
    -n 1 \
    -N 1000 \
    -s 1M \
    -r \
    "s3://${S3_BUCKET}/test_1M" > "${RESULTS_DIR}/elbencho_1M_read_t32.log" 2>&1

echo -e "${GREEN}Test 4 complete${NC}"
echo ""

echo "=========================================="
echo "Elbencho Simple Benchmark Complete!"
echo "=========================================="
echo "Results Directory: $RESULTS_DIR"
echo ""
echo "Key test logs:"
echo "  10K create (32 threads): ${RESULTS_DIR}/elbencho_10K_create_t32.log"
echo "  10K read (32 threads): ${RESULTS_DIR}/elbencho_10K_read_t32.log"
echo "  1M create (32 threads): ${RESULTS_DIR}/elbencho_1M_create_t32.log"
echo "  1M read (32 threads): ${RESULTS_DIR}/elbencho_1M_read_t32.log"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Review the log files to extract key metrics"
echo "2. Compare with SPT baseline numbers"
echo "3. Update comparison document with findings"
echo ""