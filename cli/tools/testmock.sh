#!/bin/bash

# Run spt mock workload with convenient defaults.

set -e

print_usage() {
  cat <<EOF
Usage: tools/testmock.sh [--headless] [--help]

Runs a mock workload with default parameters.

Options:
  --headless   Run in headless mode (no TUI)
  -h, --help   Show this help and exit
EOF
}

HEADLESS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headless)
      HEADLESS=true
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo >&2
      print_usage >&2
      exit 1
      ;;
  esac
done

echo "=== Run Mock test ==="
echo

CMD=(
  ./spt --debug run mock
  --threads 4
  --object-size 1MB
  --duration 5m
)

if [[ "$HEADLESS" == true ]]; then
  CMD+=(--headless)
fi

echo "> ${CMD[*]}"
"${CMD[@]}"

echo
echo "=== Test Complete ==="
echo
