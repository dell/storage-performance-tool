#!/usr/bin/env bash
set -euo pipefail

# End-to-end integration test for the "spt run mixed" workflow.
# Uses --generate-only to validate scenario generation without Docker or a real
# S3 endpoint, making it suitable for CI.
#
# Usage:
#   ./tools/test_mixed_e2e.sh                    # default (build + test)
#   ./tools/test_mixed_e2e.sh --skip-build       # skip CLI build step
#   ./tools/test_mixed_e2e.sh -h                 # help

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"

print_usage() {
  cat <<EOF
Usage: tools/test_mixed_e2e.sh [options]

Runs generate-only integration tests for the mixed workload scenario.
No Docker, S3 endpoint, or running engine required.

Options:
  --skip-build   Skip the CLI build step (use existing binary)
  -h, --help     Show this help and exit
EOF
}

SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=true; shift ;;
    -h|--help) print_usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; echo >&2; print_usage >&2; exit 1 ;;
  esac
done

# ── Counters ────────────────────────────────────────────────────────────────
PASS=0
FAIL=0
CHECKS=()

pass() { PASS=$((PASS + 1)); CHECKS+=("[PASS] $1"); printf '[PASS] %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); CHECKS+=("[FAIL] $1"); printf '[FAIL] %s\n' "$1"; }

# ── Build ───────────────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" != true ]]; then
  echo "=== Building CLI binary ==="
  (cd "$ROOT_DIR" && go build -o ./spt .)
  echo "Build OK"
  echo
fi

SPT="$ROOT_DIR/spt"
if [[ ! -x "$SPT" ]]; then
  echo "ERROR: CLI binary not found at $SPT" >&2
  echo "Run without --skip-build or build manually: cd cli && go build -o ./spt ." >&2
  exit 2
fi

# Temp directory for generated scenario files; cleaned up at exit
WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/spt_mixed_e2e.XXXXXX")
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

# Common flags shared across generate-only invocations
COMMON_FLAGS=(
  --generate-only
  --endpoints http://localhost:9999
  --access-key testkey
  --secret-key testsecret
  --bucket testbucket
  --threads 2
  --object-size 1KB
  --auto-results=false
)

# ── Test 1: Default distribution with cleanup ───────────────────────────────
echo "=== Test 1: Default distribution (45/30/15/10) with seed + cleanup ==="
SCENARIO_OUT="$WORKDIR/test1.out"
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 15s \
  --seed-objects 100 \
  --cleanup \
  --put-distrib 15 \
  --get-distrib 45 \
  --delete-distrib 10 \
  --stat-distrib 30 \
) > "$SCENARIO_OUT" 2>&1 || true

# Scenario content is printed to stdout by --generate-only
if grep -q "MixedLoad" "$SCENARIO_OUT"; then
  pass "Test1: scenario contains MixedLoad step"
else
  fail "Test1: MixedLoad not found in scenario output"
fi

if grep -q '"get": 45' "$SCENARIO_OUT"; then
  pass "Test1: GET weight is 45"
else
  fail "Test1: GET weight 45 not found"
fi

if grep -q '"put": 15' "$SCENARIO_OUT"; then
  pass "Test1: PUT weight is 15"
else
  fail "Test1: PUT weight 15 not found"
fi

if grep -q '"delete": 10' "$SCENARIO_OUT"; then
  pass "Test1: DELETE weight is 10"
else
  fail "Test1: DELETE weight 10 not found"
fi

if grep -q '"stat": 30' "$SCENARIO_OUT"; then
  pass "Test1: STAT weight is 30"
else
  fail "Test1: STAT weight 30 not found"
fi

if grep -q "PreconditionLoad" "$SCENARIO_OUT"; then
  pass "Test1: scenario contains PreconditionLoad (seed phase)"
else
  fail "Test1: PreconditionLoad (seed phase) not found"
fi

if grep -q "cleanup" "$SCENARIO_OUT"; then
  pass "Test1: scenario contains cleanup phase"
else
  fail "Test1: cleanup phase not found"
fi

if grep -q "DeleteLoad" "$SCENARIO_OUT"; then
  pass "Test1: scenario contains DeleteLoad (cleanup)"
else
  fail "Test1: DeleteLoad (cleanup) not found"
fi
echo

# ── Test 2: Custom distribution with external items files ───────────────────
echo "=== Test 2: Custom distribution (40/30/30) with external items files ==="
# Create fake items files so the CLI doesn't complain about missing paths
FAKE_ITEMS="$WORKDIR/fake-items.csv"
echo "item/path/001,0,1024,0/0" > "$FAKE_ITEMS"

SCENARIO_OUT2="$WORKDIR/test2.out"
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 15s \
  --read-items-file "$FAKE_ITEMS" \
  --delete-items-file "$FAKE_ITEMS" \
  --put-distrib 30 \
  --get-distrib 40 \
  --delete-distrib 30 \
  --stat-distrib 0 \
) > "$SCENARIO_OUT2" 2>&1 || true

if grep -q "MixedLoad" "$SCENARIO_OUT2"; then
  pass "Test2: scenario contains MixedLoad step"
else
  fail "Test2: MixedLoad not found in scenario output"
fi

if grep -q '"get": 40' "$SCENARIO_OUT2"; then
  pass "Test2: GET weight is 40"
else
  fail "Test2: GET weight 40 not found"
fi

if grep -q '"put": 30' "$SCENARIO_OUT2"; then
  pass "Test2: PUT weight is 30"
else
  fail "Test2: PUT weight 30 not found"
fi

if grep -q '"delete": 30' "$SCENARIO_OUT2"; then
  pass "Test2: DELETE weight is 30"
else
  fail "Test2: DELETE weight 30 not found"
fi

# With --read-items-file provided, seed phase should be skipped
if grep -q "PreconditionLoad" "$SCENARIO_OUT2"; then
  fail "Test2: PreconditionLoad present (should be skipped with --read-items-file)"
else
  pass "Test2: seed phase correctly skipped with --read-items-file"
fi

# Cleanup should NOT be present when using external item files
if grep -q "cleanup" "$SCENARIO_OUT2"; then
  fail "Test2: cleanup present (should be absent with external items files)"
else
  pass "Test2: cleanup correctly absent with external items files"
fi
echo

# ── Test 3: No cleanup (seed phase present, no cleanup phases) ──────────────
echo "=== Test 3: Seed phase present, cleanup absent ==="
SCENARIO_OUT3="$WORKDIR/test3.out"
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 10s \
  --seed-objects 50 \
  --put-distrib 50 \
  --get-distrib 50 \
  --delete-distrib 0 \
  --stat-distrib 0 \
) > "$SCENARIO_OUT3" 2>&1 || true

if grep -q "PreconditionLoad" "$SCENARIO_OUT3"; then
  pass "Test3: seed phase present"
else
  fail "Test3: seed phase missing"
fi

if grep -q "MixedLoad" "$SCENARIO_OUT3"; then
  pass "Test3: MixedLoad step present"
else
  fail "Test3: MixedLoad step missing"
fi

# No --cleanup flag, so DeleteLoad should not appear
if grep -q "DeleteLoad" "$SCENARIO_OUT3"; then
  fail "Test3: DeleteLoad found (should be absent without --cleanup)"
else
  pass "Test3: no cleanup phases without --cleanup"
fi
echo

# ── Test 4: Validation — distribution doesn't sum to 100 ───────────────────
echo "=== Test 4: Validation — distribution must sum to 100 ==="
SCENARIO_OUT4="$WORKDIR/test4.out"
set +e
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 10s \
  --seed-objects 50 \
  --put-distrib 10 \
  --get-distrib 10 \
  --delete-distrib 10 \
) > "$SCENARIO_OUT4" 2>&1
EXIT_CODE=$?
set -e

if [[ $EXIT_CODE -ne 0 ]] && grep -qi "sum to 30.*must equal 100\|must equal 100" "$SCENARIO_OUT4"; then
  pass "Test4: correctly rejected distribution summing to 30"
else
  fail "Test4: expected validation error for distribution sum != 100 (exit=$EXIT_CODE)"
fi
echo

# ── Test 5: Validation — delete > put without items file ────────────────────
echo "=== Test 5: Validation — delete-distrib > put-distrib without --delete-items-file ==="
SCENARIO_OUT5="$WORKDIR/test5.out"
set +e
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 10s \
  --seed-objects 50 \
  --put-distrib 10 \
  --get-distrib 10 \
  --delete-distrib 80 \
  --stat-distrib 0 \
) > "$SCENARIO_OUT5" 2>&1
EXIT_CODE=$?
set -e

if [[ $EXIT_CODE -ne 0 ]] && grep -qi "delete.*exceeds put\|PUT must sustain" "$SCENARIO_OUT5"; then
  pass "Test5: correctly rejected delete-distrib > put-distrib"
else
  fail "Test5: expected validation error for delete > put (exit=$EXIT_CODE)"
fi
echo

# ── Test 6: Validation — duration required for mixed ────────────────────────
echo "=== Test 6: Validation — --duration is required ==="
SCENARIO_OUT6="$WORKDIR/test6.out"
set +e
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --seed-objects 50 \
  --put-distrib 25 \
  --get-distrib 60 \
  --delete-distrib 15 \
  --stat-distrib 0 \
) > "$SCENARIO_OUT6" 2>&1
EXIT_CODE=$?
set -e

if [[ $EXIT_CODE -ne 0 ]] && grep -qi "duration.*required\|--duration" "$SCENARIO_OUT6"; then
  pass "Test6: correctly rejected mixed workload without --duration"
else
  fail "Test6: expected validation error for missing --duration (exit=$EXIT_CODE)"
fi
echo

# ── Test 7: Validation — at least two non-zero distributions ────────────────
echo "=== Test 7: Validation — at least two non-zero operation types ==="
SCENARIO_OUT7="$WORKDIR/test7.out"
set +e
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 10s \
  --seed-objects 50 \
  --put-distrib 0 \
  --get-distrib 100 \
  --delete-distrib 0 \
  --stat-distrib 0 \
) > "$SCENARIO_OUT7" 2>&1
EXIT_CODE=$?
set -e

if [[ $EXIT_CODE -ne 0 ]] && grep -qi "at least two" "$SCENARIO_OUT7"; then
  pass "Test7: correctly rejected single-operation distribution"
else
  fail "Test7: expected validation error for single non-zero operation (exit=$EXIT_CODE)"
fi
echo

# ── Test 8: Validation — --cleanup incompatible with external items ─────────
echo "=== Test 8: Validation — --cleanup conflicts with external items files ==="
SCENARIO_OUT8="$WORKDIR/test8.out"
set +e
(cd "$WORKDIR" && "$SPT" run mixed \
  "${COMMON_FLAGS[@]}" \
  --duration 10s \
  --read-items-file "$FAKE_ITEMS" \
  --put-distrib 40 \
  --get-distrib 60 \
  --delete-distrib 0 \
  --stat-distrib 0 \
  --cleanup \
) > "$SCENARIO_OUT8" 2>&1
EXIT_CODE=$?
set -e

if [[ $EXIT_CODE -ne 0 ]] && grep -qi "cleanup.*cannot.*item\|cannot.*cleanup" "$SCENARIO_OUT8"; then
  pass "Test8: correctly rejected --cleanup with --read-items-file"
else
  fail "Test8: expected validation error for --cleanup + external items (exit=$EXIT_CODE)"
fi
echo

# ── Cleanup generated scenario files ────────────────────────────────────────
# The generate-only flag writes spt-scenario-*.js in the cwd ($WORKDIR).
# The trap already removes $WORKDIR, but also clean any stray files in ROOT_DIR.
rm -f "$ROOT_DIR"/spt-scenario-*.js 2>/dev/null || true

# ── Summary ─────────────────────────────────────────────────────────────────
echo "========================================"
echo "  Mixed Workload E2E Test Summary"
echo "========================================"
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  TOTAL: $((PASS + FAIL))"
echo "========================================"
for c in "${CHECKS[@]}"; do
  echo "  $c"
done
echo "========================================"
echo

if [[ $FAIL -gt 0 ]]; then
  echo "RESULT: FAIL"
  exit 1
fi

echo "RESULT: PASS"
exit 0
