#!/bin/bash

# Run spt list workload with convenient defaults (env must supply S3 flags).

set -e

print_usage() {
  cat <<EOF
Usage: tools/testlist.sh [options]

Runs a LIST workload with default parameters. S3 endpoint, credentials, and
bucket are read from the environment (e.g., ./.env).

Options:
  --headless                 Run in headless mode (no TUI)
  --prefix VALUE             Optional prefix filter for listings
  --bucket NAME              Target bucket override (default uses S3_BUCKET env)
  --s3-driver TYPE           Storage driver: default, aws, rdma (env: SPT_S3_DRIVER)
  --auto-terminate-seconds N Stop after N seconds (default: $AUTO_TERMINATE_SECONDS)
  -h, --help                 Show this help and exit
EOF
}

HEADLESS=false
PREFIX=${PREFIX:-""}
BUCKET=${BUCKET:-${S3_BUCKET:-""}}
THREADS=${THREADS:-""}
AUTO_TERMINATE_SECONDS=${AUTO_TERMINATE_SECONDS:-120}
S3_DRIVER=${SPT_S3_DRIVER:-"default"}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --headless)
      HEADLESS=true
      shift
      ;;
    --prefix)
      PREFIX="${2:-}"
      shift 2
      ;;
  --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --threads)
      THREADS="${2:-}"
      shift 2
      ;;
    --auto-terminate-seconds)
      AUTO_TERMINATE_SECONDS="${2:-0}"
      shift 2
      ;;
    --s3-driver)
      S3_DRIVER="${2:-}"
      shift 2
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

echo "=== Run LIST workload ==="
echo

CMD=(
  ./spt --debug run list
)

if [[ "$HEADLESS" == true ]]; then
  CMD+=(--headless)
fi

if [[ -n "$PREFIX" ]]; then
  CMD+=(--prefix "$PREFIX")
fi

if [[ -n "$BUCKET" ]]; then
  CMD+=(--bucket "$BUCKET")
fi

# Concurrency: prefer explicit flag; otherwise pick up THREADS from env
if [[ -n "$THREADS" ]]; then
  if [[ "$THREADS" =~ ^[0-9]+$ ]] && [ "$THREADS" -gt 0 ]; then
    CMD+=(--threads "$THREADS")
  else
    echo "Warning: THREADS is set but invalid: '$THREADS' (expected positive integer); using spt default (1)" >&2
  fi
fi

if [[ "$AUTO_TERMINATE_SECONDS" =~ ^[0-9]+$ ]] && [ "$AUTO_TERMINATE_SECONDS" -gt 0 ]; then
  CMD+=(--auto-terminate-seconds "$AUTO_TERMINATE_SECONDS")
fi

# Storage driver (only pass when non-default to avoid requiring the flag on older builds)
if [[ "$S3_DRIVER" != "default" ]]; then
  CMD+=(--s3-driver "$S3_DRIVER")
fi

[[ -n "$PREFIX" ]] && echo "Prefix: $PREFIX"
[[ -n "$BUCKET" ]] && echo "Bucket: $BUCKET"
[[ -n "$THREADS" ]] && echo "Threads: $THREADS"
[[ "$S3_DRIVER" != "default" ]] && echo "S3 Driver: $S3_DRIVER"
if [[ "$AUTO_TERMINATE_SECONDS" =~ ^[0-9]+$ ]] && [ "$AUTO_TERMINATE_SECONDS" -gt 0 ]; then
  echo "Auto-terminate: ${AUTO_TERMINATE_SECONDS}s"
fi
echo

echo "> ${CMD[*]}"
"${CMD[@]}"

echo
echo "=== Test Complete ==="
echo
