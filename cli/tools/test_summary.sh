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
BOX_V="║"
BOX_TL="╔"
BOX_TR="╗"
BOX_BL="╚"
BOX_BR="╝"

RESULTS_FILE="test-results.json"
METADATA_FILE="test-results.meta.json"
FAILURES_FILE="test-failures.txt"

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
    
    # A final event is an individual test result only when Go supplies a test name.
    # Named parents and named subtests each have their own final event and are counted.
    local total_tests
    local passed_tests
    local failed_tests
    local skipped_tests
    local package_failures
    local package_skips
    total_tests=$(jq -s '[.[] | select(.Test != null and (.Action == "pass" or .Action == "fail" or .Action == "skip"))] | length' "$json_file")
    passed_tests=$(jq -s '[.[] | select(.Test != null and .Action == "pass")] | length' "$json_file")
    failed_tests=$(jq -s '[.[] | select(.Test != null and .Action == "fail")] | length' "$json_file")
    skipped_tests=$(jq -s '[.[] | select(.Test != null and .Action == "skip")] | length' "$json_file")
    package_failures=$(jq -s '[.[] | select(.Test == null and .Action == "fail")] | length' "$json_file")
    package_skips=$(jq -s '[.[] | select(.Test == null and .Action == "skip")] | length' "$json_file")
    
    # Count packages
    local total_packages
    total_packages=$(jq -s '[.[] | .Package | select(. != null)] | unique | length' "$json_file")

    local wall_clock_duration=""
    if [ -f "$METADATA_FILE" ]; then
        wall_clock_duration=$(jq -er '.wallClockSeconds | numbers' "$METADATA_FILE" 2>/dev/null || true)
    fi
    
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
    if [ "$package_failures" -gt 0 ]; then
        printf "${RED}✗ Package/build failures: %s${NC}\n" "$package_failures"
    fi
    if [ -n "$wall_clock_duration" ]; then
        printf "⏱  Wall-clock duration: %.2fs\n" "$wall_clock_duration"
    else
        printf "⏱  Wall-clock duration: unavailable (results predate metadata)\n"
    fi
    echo

    # The report always describes the results currently being parsed, never a prior run.
    rm -f "$FAILURES_FILE"

    # Show named test failures if any.
    if [ "$failed_tests" -gt 0 ]; then
        draw_box "TEST FAILURES"
        echo

        jq -s -r '.[] | select(.Action == "fail" and .Test != null) | "✗ \(.Test)\n  Package: \(.Package)\n"' "$json_file" > "$FAILURES_FILE"
        while IFS= read -r line; do
            if [[ $line == ✗* ]]; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${GRAY}$line${NC}"
            fi
        done < "$FAILURES_FILE"
        echo
    fi

    # Report null-test failure events separately and retain useful compiler/setup output.
    if [ "$package_failures" -gt 0 ]; then
        draw_box "PACKAGE / BUILD FAILURES"
        echo

        local package_failure_report
        package_failure_report=$(jq -s -r '
            . as $events
            | [.[] | select(.Action == "fail" and .Test == null)]
            | unique_by(.Package)
            | .[]
            | .Package as $package
            | ([$events[]
                | select(
                    (.Package == $package and .Action == "output")
                    or (.ImportPath == $package and .Action == "build-output"))
                | .Output]
                | join("")
                | split("\n")
                | map(select(length > 0))
                | .[-20:]) as $output
            | "✗ \($package)\n  Package/build failure"
                + (if ($output | length) > 0
                   then "\n" + ($output | map("  " + .) | join("\n"))
                   else "\n  No package output was captured; inspect test-results.json."
                   end)
                + "\n"' "$json_file")
        printf '%s\n' "$package_failure_report" >> "$FAILURES_FILE"
        while IFS= read -r line; do
            if [[ $line == ✗* ]]; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${GRAY}$line${NC}"
            fi
        done <<< "$package_failure_report"
        echo
    fi

    # Show skipped tests and package-level skips without counting package events as tests.
    if [ "$skipped_tests" -gt 0 ] || [ "$package_skips" -gt 0 ]; then
        draw_box "SKIPPED TESTS"
        echo

        # Get individual test skips (with Test field)
        local individual_skip_report
        local package_skip_report
        individual_skip_report=$(jq -s -r '.[] | select(.Action == "skip" and .Test != null) | "⊘ \(.Test)\n  Package: \(.Package)\n  Reason: Conditional skip (likely missing dependencies)"' "$json_file")
        package_skip_report=$(jq -s -r '.[] | select(.Action == "skip" and .Test == null) | .Package' "$json_file")

        # Show individual test skips
        if [ -n "$individual_skip_report" ]; then
            echo -e "${YELLOW}Individual Test Skips:${NC}"
            while IFS= read -r line; do
                if [[ $line == ⊘* ]]; then
                    echo -e "${YELLOW}$line${NC}"
                else
                    echo -e "${GRAY}$line${NC}"
                fi
            done <<< "$individual_skip_report"
            echo
        fi

        # Show package-level skips
        if [ -n "$package_skip_report" ]; then
            echo -e "${GRAY}Package-Level Skips (No Test Files):${NC}"
            while IFS= read -r pkg; do
                echo -e "${GRAY}  • $pkg${NC}"
            done <<< "$package_skip_report"
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
    jq -s -r '[.[] | select(.Test != null and (.Action == "pass" or .Action == "fail" or .Action == "skip")) |
           {package: .Package, test: .Test, result: .Action}] |
           group_by(.package) | 
           map({
               package: .[0].package,
               total: length,
               passed: [.[] | select(.result == "pass")] | length,
               failed: [.[] | select(.result == "fail")] | length,
               skipped: [.[] | select(.result == "skip")] | length
           }) | 
           sort_by(.package) |
           .[] | 
           if .failed > 0 then
               "FAIL \(.package) \(.passed)/\(.total) (\((((.passed * 1000 / .total) + 0.5) | floor) / 10)%) ← \(.failed) failures"
               + (if .skipped > 0 then ", \(.skipped) skipped" else "" end)
           elif .skipped > 0 then
               "PASS \(.package) \(.passed)/\(.total) (\((((.passed * 1000 / .total) + 0.5) | floor) / 10)%) ← \(.skipped) skipped"
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
    if [ "$failed_tests" -gt 0 ] || [ "$package_failures" -gt 0 ]; then
        echo -e "${GRAY}❌ Failure details saved to: $FAILURES_FILE${NC}"
    fi
    echo
}

wall_clock_now() {
    local bash_time="${EPOCHREALTIME:-}"
    if [ -n "$bash_time" ]; then
        printf '%s\n' "$bash_time"
    else
        date +%s
    fi
}

# Run tests and generate summary
run_tests() {
    echo "Running tests with JSON output..."

    # Clear reports before invoking Go so interrupted or successful runs cannot retain
    # failure details or timing metadata from an earlier execution.
    rm -f "$FAILURES_FILE" "$METADATA_FILE"

    # Run tests with JSON output, save both to file and display progress
    local gowork_setting
    local -a go_cmd=(go test -json ./...)
    gowork_setting=$(go env GOWORK 2>/dev/null || true)
    # Only pass -mod=mod when not using a workspace.
    if [[ -z "${gowork_setting}" || "${gowork_setting}" == "off" ]]; then
        go_cmd=(go test -mod=mod -json ./...)
    fi
    local wall_clock_start
    local wall_clock_end
    local wall_clock_duration
    local -a pipeline_status
    wall_clock_start=$(wall_clock_now)

    # Do not enable pipefail here: an empty progress stream is valid. Capture every
    # stage immediately and preserve the go test status explicitly below.
    set +e
    "${go_cmd[@]}" | tee "$RESULTS_FILE" | \
    jq -r 'select(.Action == "run") | "Running: \(.Test // .Package)"' | \
    while IFS= read -r line; do
        echo -e "${GRAY}$line${NC}"
    done
    pipeline_status=("${PIPESTATUS[@]}")
    set -e

    wall_clock_end=$(wall_clock_now)
    wall_clock_duration=$(awk -v start="$wall_clock_start" -v end="$wall_clock_end" 'BEGIN {printf "%.6f", end - start}')
    jq -n --argjson wall_clock_seconds "$wall_clock_duration" \
        '{wallClockSeconds: $wall_clock_seconds}' > "$METADATA_FILE"

    echo "Tests completed. Generating summary..."
    parse_results "$RESULTS_FILE"

    local go_test_status=${pipeline_status[0]}
    if [ "$go_test_status" -ne 0 ]; then
        return "$go_test_status"
    fi

    # A progress-display failure must not replace a nonzero Go status, but it should
    # still fail an otherwise successful run because the results may be incomplete.
    local pipeline_stage_status
    for pipeline_stage_status in "${pipeline_status[@]:1}"; do
        if [ "$pipeline_stage_status" -ne 0 ]; then
            return "$pipeline_stage_status"
        fi
    done
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
