#!/usr/bin/env bash

# coverage_summary.sh - Run tests with coverage and print a clean summary
# - Generates coverage.out and coverage.html
# - Prints overall coverage and per-package coverage breakdown

set -euo pipefail

# Colors (similar to tools/test_summary.sh)
if [ -t 1 ] && [ "${TERM:-}" != "dumb" ]; then
  RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; GRAY='\033[0;90m'; NC='\033[0m'
else
  RED=''; GREEN=''; YELLOW=''; BLUE=''; BOLD=''; GRAY=''; NC=''
fi

BOX_H="======================================================"
BOX_V="║"; BOX_TL="╔"; BOX_TR="╗"; BOX_BL="╚"; BOX_BR="╝"

draw_box() {
  local title="$1"
  local width=54
  printf "${BLUE}%s%s%s${NC}\n" "$BOX_TL" "${BOX_H:0:$width}" "$BOX_TR"
  # centered title
  local padding=$(( (width - ${#title}) / 2 ))
  printf "${BLUE}${BOX_V}${NC}%*s${BOLD}%s${NC}%*s${BLUE}${BOX_V}${NC}\n" \
    "$padding" "" "$title" "$((width - padding - ${#title}))" ""
  printf "${BLUE}%s%s%s${NC}\n" "$BOX_BL" "${BOX_H:0:$width}" "$BOX_BR"
}

run_tests_with_coverage() {
  echo "Running tests with coverage..."
  # Exclude 'tools' packages from coverage to avoid diluting app coverage
  PKGS=$(go list ./... | grep -v "/tools")
  go test -mod=mod -coverprofile=coverage.out $PKGS
  GOFLAGS=-mod=mod go tool cover -html=coverage.out -o coverage.html
  echo "Coverage report generated: coverage.html"
}

# Parse coverage.out and compute per-package and overall coverage
summarize_coverage() {
  local profile="coverage.out"
  if [ ! -f "$profile" ]; then
    echo -e "${RED}coverage.out not found. Did tests fail early?${NC}"
    exit 1
  fi

  # Print pretty summary using the Go helper (no jq required)
  echo
  go run ./tools/coverage/covsum -profile "$profile" -sort pkg
}

main() {
  run_tests_with_coverage
  echo "Generating coverage summary..."
  summarize_coverage
}

main "$@"
