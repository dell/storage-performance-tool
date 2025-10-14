#!/bin/bash
# Script to extract test statistics and coverage percentage

# Get test statistics from XML files
TOTAL_TESTS=0
FAILED_TESTS=0
PASSED_TESTS=0

# Find all test result XML files
for xml in $(find . -path "*/build/test-results/test/*.xml" -type f 2>/dev/null); do
    if [ -f "$xml" ]; then
        # Extract test counts from each XML file
        tests=$(grep -o 'tests="[0-9]*"' "$xml" | head -1 | grep -o '[0-9]*')
        failures=$(grep -o 'failures="[0-9]*"' "$xml" | head -1 | grep -o '[0-9]*')
        errors=$(grep -o 'errors="[0-9]*"' "$xml" | head -1 | grep -o '[0-9]*')
        
        if [ -n "$tests" ]; then
            TOTAL_TESTS=$((TOTAL_TESTS + tests))
            if [ -n "$failures" ]; then
                FAILED_TESTS=$((FAILED_TESTS + failures))
            fi
            if [ -n "$errors" ]; then
                FAILED_TESTS=$((FAILED_TESTS + errors))
            fi
        fi
    fi
done

PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS))

# Get coverage percentage from HTML report
COVERAGE_FILE="build/reports/jacoco/aggregate/html/index.html"
if [ -f "$COVERAGE_FILE" ]; then
    # Extract instruction coverage percentage from the Total row
    # The Total row has multiple percentages - instruction coverage is the first ctr2 after "Total"
    # Format: <td>Total</td><td class="bar">35,779 of 40,816</td><td class="ctr2">12%</td>
    TOTAL_LINE=$(grep '<td>Total</td>' "$COVERAGE_FILE")
    if [ -n "$TOTAL_LINE" ]; then
        # Split by ctr2 and get the first percentage after Total
        COVERAGE=$(echo "$TOTAL_LINE" | sed 's/<td>Total<\/td>/\n&/' | tail -1 | sed 's/<td class="ctr2">/\n&/g' | grep 'ctr2' | head -1 | sed 's/.*>\([0-9]*\)%.*/\1/')
    else
        COVERAGE="0"
    fi
else
    COVERAGE="N/A"
fi

# Output results
echo "================================"
echo "Test & Coverage Report"
echo "================================"
echo "Total Tests:    $TOTAL_TESTS"
echo "Passed Tests:   $PASSED_TESTS"
echo "Failed Tests:   $FAILED_TESTS"
echo "--------------------------------"
echo "Overall Coverage: ${COVERAGE}%"
echo "================================"