#!/bin/bash

# test_summary.sh - Create a concise summary of Go test results
# Uses jq for JSON parsing to provide clean test summaries

set -e

# Colors - auto-detect terminal support
if [ -t 1 ] && [ "${TERM}" != "dumb" ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    GRAY='\033[0;90m'
    NC='\033[0m' # No Color
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; BOLD=''; NC=''; GRAY=''
fi

# Box drawing characters
BOX_H="════"
BOX_V="║"
BOX_TL="╔"
BOX_TR="╗"
BOX_BL="╚"
BOX_BR="╝"

TEMP_FILE=$(mktemp)
RESULTS_FILE="test-results.json"
FAILURES_FILE="test-failures.txt"

# Cleanup function
cleanup() {
    rm -f "$TEMP_FILE"
}
trap cleanup EXIT

# Check for jq
check_jq() {
    if ! command -v jq &> /dev/null; then
        echo -e "${RED}⚠️  jq is not installed. Test summary requires jq.${NC}"
        echo ""
        echo "Install jq with one of:"
        echo "  • Ubuntu/Debian: sudo apt-get install jq"
        echo "  • macOS:         brew install jq"
        echo "  • Fedora:        sudo dnf install jq"
        echo "  • openSUSE:      sudo zypper install jq"
        echo ""
        echo -e "${YELLOW}Or run: make test-verbose for standard output${NC}"
        exit 1
    fi
}

# Draw a box with title
draw_box() {
    local title="$1"
    local width=54
    local padding=$(( (width - ${#title} - 2) / 2 ))
    
    echo -e "${BLUE}${BOX_TL}$(printf "%${width}s" | tr ' ' '=')${BOX_TR}${NC}"
    printf "${BLUE}${BOX_V}${NC}%*s${BOLD}%s${NC}%*s${BLUE}${BOX_V}${NC}\n" \
        $padding "" "$title" $((width - padding - ${#title})) ""
    echo -e "${BLUE}${BOX_BL}$(printf "%${width}s" | tr ' ' '=')${BOX_BR}${NC}"
}

# Parse test results from JSON
parse_results() {
    local json_file="$1"
    
    if [ ! -f "$json_file" ]; then
        echo "Error: Test results file not found: $json_file"
        exit 1
    fi
    
    # Count results by action type - use jq -s to read the entire file as an array
    local total_tests=$(jq -s '[.[] | select(.Action == "pass" or .Action == "fail" or .Action == "skip")] | length' "$json_file")
    local passed_tests=$(jq -s '[.[] | select(.Action == "pass")] | length' "$json_file")
    local failed_tests=$(jq -s '[.[] | select(.Action == "fail")] | length' "$json_file")
    local skipped_tests=$(jq -s '[.[] | select(.Action == "skip")] | length' "$json_file")
    
    # Count packages
    local total_packages=$(jq -s '[.[] | .Package] | unique | length' "$json_file")
    
    # Calculate duration (sum of all elapsed times from final test events)
    local total_duration=$(jq -s '[.[] | select(.Action == "pass" or .Action == "fail") | .Elapsed // 0] | add // 0' "$json_file")
    
    # Calculate percentages (use awk for more portable math)
    local pass_pct="0.0"
    local fail_pct="0.0"
    local skip_pct="0.0"
    if [ "$total_tests" -gt 0 ]; then
        pass_pct=$(awk "BEGIN {printf \"%.1f\", $passed_tests * 100.0 / $total_tests}")
        fail_pct=$(awk "BEGIN {printf \"%.1f\", $failed_tests * 100.0 / $total_tests}")
        skip_pct=$(awk "BEGIN {printf \"%.1f\", $skipped_tests * 100.0 / $total_tests}")
    fi
    
    echo
    draw_box "TEST SUMMARY"
    echo
    
    printf "Total:    %s tests across %s packages\n" "$total_tests" "$total_packages"
    printf "${GREEN}✓ Passed: %s (%.1f%%)${NC}\n" "$passed_tests" "$pass_pct"
    if [ "$failed_tests" -gt 0 ]; then
        printf "${RED}✗ Failed: %s (%.1f%%)${NC}\n" "$failed_tests" "$fail_pct"
    else
        printf "${GREEN}✗ Failed: %s (%.1f%%)${NC}\n" "$failed_tests" "$fail_pct"
    fi
    if [ "$skipped_tests" -gt 0 ]; then
        printf "${YELLOW}⊘ Skipped: %s (%.1f%%)${NC}\n" "$skipped_tests" "$skip_pct"
    else
        printf "${GRAY}⊘ Skipped: %s (%.1f%%)${NC}\n" "$skipped_tests" "$skip_pct"
    fi
    printf "⏱  Duration: %.2fs\n" "$total_duration"
    echo
    
    # Show failures if any
    if [ "$failed_tests" -gt 0 ]; then
        draw_box "FAILURES"
        echo
        
        # Get failed tests with package info
        jq -s -r '.[] | select(.Action == "fail") | "✗ \(.Test)\n  Package: \(.Package)\n"' "$json_file" > "$FAILURES_FILE"
        cat "$FAILURES_FILE" | while IFS= read -r line; do
            if [[ $line == ✗* ]]; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${GRAY}$line${NC}"
            fi
        done
        echo
    fi
    
    # Show skipped tests if any
    if [ "$skipped_tests" -gt 0 ]; then
        draw_box "SKIPPED TESTS"
        echo
        
        # Get individual test skips (with Test field)
        local individual_skips=$(jq -s -r '.[] | select(.Action == "skip" and .Test) | "⊘ \(.Test)\n  Package: \(.Package)\n  Reason: Conditional skip (likely missing dependencies)"' "$json_file")
        
        # Get package-level skips (no Test field) 
        local package_skips=$(jq -s -r '.[] | select(.Action == "skip" and .Test == null) | .Package' "$json_file")
        
        # Show individual test skips
        if [ -n "$individual_skips" ]; then
            echo -e "${YELLOW}Individual Test Skips:${NC}"
            echo "$individual_skips" | while IFS= read -r line; do
                if [[ $line == ⊘* ]]; then
                    echo -e "${YELLOW}$line${NC}"
                else
                    echo -e "${GRAY}$line${NC}"
                fi
            done
            echo
        fi
        
        # Show package-level skips
        if [ -n "$package_skips" ]; then
            echo -e "${GRAY}Package-Level Skips (No Test Files):${NC}"
            echo "$package_skips" | while IFS= read -r pkg; do
                echo -e "${GRAY}  • $pkg${NC}"
            done
            echo
        fi
        
        echo -e "${GRAY}💡 Skipped tests are normal and indicate:${NC}"
        echo -e "${GRAY}   • Directories without test files (package-level skips)${NC}"
        echo -e "${GRAY}   • Tests that require specific dependencies or environments${NC}"
        echo -e "${GRAY}   • Conditional tests that gracefully skip when requirements aren't met${NC}"
        echo
    fi
    
    # Package breakdown
    draw_box "PACKAGE BREAKDOWN"
    echo
    
    # Generate package statistics
    jq -s -r '[.[] | select(.Action == "pass" or .Action == "fail") | 
           {package: .Package, test: .Test, result: .Action}] |
           group_by(.package) | 
           map({
               package: .[0].package,
               total: length,
               passed: [.[] | select(.result == "pass")] | length,
               failed: [.[] | select(.result == "fail")] | length
           }) | 
           sort_by(.package) |
           .[] | 
           if .failed > 0 then
               "FAIL \(.package) \(.passed)/\(.total) (\((.passed * 100 / .total * 10 + 0.5) / 10)%) ← \(.failed) failures"
           else
               "PASS \(.package) \(.passed)/\(.total) (100%)"
           end' "$json_file" | \
    while IFS= read -r line; do
        if [[ $line == FAIL* ]]; then
            # Extract package name and stats for failed packages
            local pkg_info="${line#FAIL }"
            echo -e "${RED}✗ $pkg_info${NC}"
        else
            # Extract package name and stats for passed packages  
            local pkg_info="${line#PASS }"
            echo -e "${GREEN}✓ $pkg_info${NC}"
        fi
    done
    
    echo
    echo -e "${GRAY}💡 Run 'make test-verbose' for detailed output${NC}"
    echo -e "${GRAY}📄 Full results saved to: $RESULTS_FILE${NC}"
    if [ "$failed_tests" -gt 0 ]; then
        echo -e "${GRAY}❌ Failure details saved to: $FAILURES_FILE${NC}"
    fi
    echo
}

# Run tests and generate summary
run_tests() {
    echo "Running tests with JSON output..."
    
    # Run tests with JSON output, save both to file and display progress
    go test -mod=mod -json ./... | tee "$RESULTS_FILE" | \
    grep -E '"Action":"(run|pass|fail|skip)"' | \
    jq -r 'select(.Action == "run") | "Running: \(.Test // .Package)"' | \
    while IFS= read -r line; do
        echo -e "${GRAY}$line${NC}"
    done
    
    echo "Tests completed. Generating summary..."
    parse_results "$RESULTS_FILE"
}

# Main execution
main() {
    case "${1:-}" in
        --parse-only)
            check_jq
            if [ -f "$RESULTS_FILE" ]; then
                parse_results "$RESULTS_FILE"
            else
                echo "No test results found. Run 'make test' first."
                exit 1
            fi
            ;;
        --help|-h)
            echo "Usage: $0 [--parse-only] [--help]"
            echo ""
            echo "  --parse-only  Parse existing test-results.json file"
            echo "  --help        Show this help message"
            ;;
        "")
            check_jq
            run_tests
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
}

main "$@"