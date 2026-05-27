#!/bin/bash
# Script to run the AWS CRT benchmark with different thread counts

cd "$(dirname "$0")"
export JAVA_HOME=/opt/jdk-21.0.2+13
export PATH=$JAVA_HOME/bin:$PATH

echo "================================"
echo "AWS CRT Concurrent Benchmark Suite"
echo "================================"

# Test with different thread counts
THREAD_COUNTS=(1 8 16 32)

for THREAD_COUNT in "${THREAD_COUNTS[@]}"; do
    echo ""
    echo "================================"
    echo "Testing with $THREAD_COUNT threads"
    echo "================================"
    ./gradlew run -PappArgs="$THREAD_COUNT"
    echo ""
done

echo "================================"
echo "Benchmark suite completed"
echo "================================"
