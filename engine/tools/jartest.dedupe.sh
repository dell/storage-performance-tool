#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_SCRIPT="${SCRIPT_DIR}/jartest.local.sh"

if [[ ! -x "${LOCAL_SCRIPT}" ]]; then
  echo "Expected executable script at ${LOCAL_SCRIPT}" >&2
  exit 1
fi

MODE="${1:-all}"
shift 2>/dev/null || true

: "${JARTEST_CONCURRENCY:=1}"
: "${JARTEST_OBJECT_SIZE:=4MB}"
: "${JARTEST_OP_LIMIT_COUNT:=100}"
: "${JARTEST_READ_BACK:=true}"
: "${JARTEST_READ_LIMIT_COUNT:=${JARTEST_OP_LIMIT_COUNT}}"
: "${S3_DRIVER:=s3}"

run_profile() {
  local label="$1"
  local dedupable="$2"
  local compressibility="$3"
  local verify="$4"
  shift 4
  echo ""
  echo "=== jartest.dedupe profile: ${label} ==="
  JARTEST_DEDUPABLE="${dedupable}" \
  JARTEST_COMPRESSIBILITY="${compressibility}" \
  JARTEST_VERIFY="${verify}" \
  "${LOCAL_SCRIPT}" "$@"
}

case "${MODE}" in
  all)
    run_profile "baseline-dedupable" true 0 false "$@"
    run_profile "compressibility-75" true 75 false "$@"
    run_profile "dedupe-off-stamped" false 0 false "$@"
    run_profile "dedupe-off-verify-requested" false 0 true "$@"
    ;;
  baseline)
    run_profile "baseline-dedupable" true 0 false "$@"
    ;;
  compress75)
    run_profile "compressibility-75" true 75 false "$@"
    ;;
  stamped)
    run_profile "dedupe-off-stamped" false 0 false "$@"
    ;;
  stamped-verify)
    run_profile "dedupe-off-verify-requested" false 0 true "$@"
    ;;
  *)
    cat <<'USAGE' >&2
Usage: jartest.dedupe.sh [all|baseline|compress75|stamped|stamped-verify] [extra SPT args...]

Defaults (override via env):
  JARTEST_CONCURRENCY=1
  JARTEST_OBJECT_SIZE=4MB
  JARTEST_OP_LIMIT_COUNT=100
  JARTEST_READ_BACK=true
  JARTEST_READ_LIMIT_COUNT=JARTEST_OP_LIMIT_COUNT
USAGE
    exit 1
    ;;
esac
