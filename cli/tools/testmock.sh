#!/bin/bash

# Run spt mock workload with convenient defaults.

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"

print_usage() {
  cat <<EOF
Usage: tools/testmock.sh [--headless] [--image IMAGE] [--skip-image-pull] [--help]

Runs a mock workload with default parameters.

Options:
  --headless          Run in headless mode (no TUI)
  --image IMAGE       Override SPT Docker image (env: SPT_IMAGE)
  --skip-image-pull   Use locally cached image, skip pull (env: SPT_SKIP_IMAGE_PULL)
  -h, --help          Show this help and exit
EOF
}

HEADLESS=false
SPT_IMAGE=${SPT_IMAGE:-""}
SKIP_IMAGE_PULL=${SPT_SKIP_IMAGE_PULL:-false}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headless)
      HEADLESS=true
      shift
      ;;
    --image)
      SPT_IMAGE="${2:-}"; shift 2
      ;;
    --skip-image-pull)
      SKIP_IMAGE_PULL=true; shift
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

# Export image settings so the spt CLI picks them up
[ -n "$SPT_IMAGE" ] && export SPT_IMAGE
if [ "$SKIP_IMAGE_PULL" = true ]; then
  export SPT_SKIP_IMAGE_PULL=true
fi

echo "=== Run Mock test ==="
echo

CMD=(
  "$ROOT_DIR"/spt --debug run mock
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
