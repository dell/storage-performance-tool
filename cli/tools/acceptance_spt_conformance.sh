#!/usr/bin/env bash
set -euo pipefail

# Spt v5 API conformance validator for the results fast-path
# Validates the 4 phases described in planning/RESULTS_GATHER_OPTIONS.md context update:
# 1) Stable JSON /status with linger
# 2) Non-empty /metrics/json at idle with terminal:true and zero last rates
# 3) HEAD support for /logs + uniform 404 for unknown logger
# 4) Per-step /logs/<stepId>/index.json with items and accurate headers

# How To Run (examples):
# - Quick local smoke (5s linger):
#     API_LINGER_EXPECT_SEC=5 RUN_LIMIT_COUNT=50 tools/acceptance_spt_conformance.sh
# - Full linger spec (20s), short mock workload:
#     API_LINGER_EXPECT_SEC=20 RUN_LIMIT_COUNT=200 tools/acceptance_spt_conformance.sh
# - Validate an already running node (no launch):
#     SKIP_LAUNCH=1 BASE_URL=http://entry:9999 tools/acceptance_spt_conformance.sh
# - Custom spt path:
#     SPT_BIN=./dist/spt tools/acceptance_spt_conformance.sh
#
# Notes on JSON parsing:
# - Uses jq for JSON parsing (required by other repo tools as well).
#   If jq is missing, install it or adjust PATH.
#
# Env vars:
#   BASE_URL                  Default http://localhost:9999
#   API_LINGER_EXPECT_SEC     Default 5s (set 20 to match server default linger)
#   RUN_LIMIT_COUNT           Default 200 (ops); keeps the run short. Ignored if SKIP_LAUNCH=1.
#   SPT_BIN                  Default ./spt

BASE_URL=${BASE_URL:-http://localhost:9999}
API_LINGER_EXPECT_SEC=${API_LINGER_EXPECT_SEC:-5}
RUN_LIMIT_COUNT=${RUN_LIMIT_COUNT:-200}
SPT_BIN=${SPT_BIN:-./spt}

WORKDIR="tools/_tmp/accept_conformance"
mkdir -p "$WORKDIR"
LOG="$WORKDIR/run.log"
PASS=()
FAIL=()

note_pass() { PASS+=("$1"); printf '[PASS] %s\n' "$1"; }
note_fail() { FAIL+=("$1"); printf '[FAIL] %s\n' "$1"; }

sanitize() {
  sed -E \
    -e 's/(--access-key)(=|\s+)[^[:space:]]+/\1\2****/g' \
    -e 's/(--secret-key)(=|\s+)[^[:space:]]+/\1\2****/g' \
    -e 's/(access[_-]?key)(=|:|\s+)[^[:space:]\"]+/\1\2****/gi' \
    -e 's/(secret[_-]?key)(=|:|\s+)[^[:space:]\"]+/\1\2****/gi'
}

fetch_json() { # url outpath -> prints http_code
  local url=$1 out=$2
  local code
  code=$(curl -sS -m 5 -w '%{http_code}' -H 'Accept: application/json' "$url" -o "$out" || echo 000)
  echo "$code"
}

# ensure jq exists early
if ! command -v jq >/dev/null 2>&1; then
  echo "jq not found in PATH; please install jq to run this script." >&2
  exit 2
fi

RUN_PID=""

if [[ "${SKIP_LAUNCH:-}" != 1 ]]; then
  echo "== Launching mock workload via spt (background) ==" | tee "$LOG"
  set +e
  timeout 180 "$SPT_BIN" --headless --verbose \
    run mock --api-port 9999 --force --auto-results=false -n "$RUN_LIMIT_COUNT" 2>&1 | sanitize | tee -a "$LOG" &
  RUN_PID=$!
  set -e
else
  echo "== SKIP_LAUNCH=1: validating against $BASE_URL only ==" | tee "$LOG"
fi

echo "== Waiting for API ready at $BASE_URL ==" | tee -a "$LOG"
for i in $(seq 1 60); do
  code=$(curl -sS -m 1 -w '%{http_code}' -H 'Accept: application/json' "$BASE_URL/status" -o "$WORKDIR/status_boot.json" || echo 000)
  [[ "$code" == 200 ]] && break || true
  sleep 1
done

echo "== Phase 1.A: /status returns JSON once ready =="
tmp=$(mktemp "$WORKDIR/.status.run.XXXX.json")
code=$(fetch_json "$BASE_URL/status" "$tmp")
if [[ "$code" == 200 ]]; then
  state=$(jq -r '.state // ""' "$tmp" | tr '[:lower:]' '[:upper:]')
  [[ -n "$state" ]] && note_pass "/status 200 JSON (state=$state)" || note_fail "/status 200 but missing state"
else
  note_fail "/status not 200 (HTTP $code)"
fi

echo "== Requesting clean shutdown via /shutdown (retry up to 30s) =="
sh_ok=0
for i in $(seq 1 30); do
  sh_code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/shutdown" || echo 000)
  if [[ "$sh_code" == 200 || "$sh_code" == 202 || "$sh_code" == 204 ]]; then sh_ok=1; break; fi
  sleep 1
done
if [[ "$sh_ok" == 0 ]]; then
  for i in $(seq 1 10); do
    sh_code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/shutdown" || echo 000)
    if [[ "$sh_code" == 200 || "$sh_code" == 202 || "$sh_code" == 204 ]]; then sh_ok=1; break; fi
    sleep 1
  done
fi
[[ $sh_ok == 1 ]] && note_pass "/shutdown accepted (HTTP $sh_code)" || note_fail "/shutdown not accepted (last HTTP $sh_code)"

echo "== Phase 1.B: /status lingers with terminal state =="
seen_terminal=0
terminal_state=""
for i in $(seq 1 60); do
  tmp=$(mktemp "$WORKDIR/.status.term.XXXX.json")
  code=$(fetch_json "$BASE_URL/status" "$tmp")
  if [[ "$code" == 200 ]]; then
    state=$(jq -r '.state // ""' "$tmp" | tr '[:lower:]' '[:upper:]')
    case "$state" in
      IDLE|COMPLETED|FAILED|STOPPED) seen_terminal=1; terminal_state="$state"; break;;
    esac
  fi
  sleep 1
done
[[ $seen_terminal == 1 ]] && note_pass "status entered terminal state ($terminal_state)" || note_fail "status did not enter terminal state"

# Verify linger window: keep getting 200 JSON for >= API_LINGER_EXPECT_SEC
if [[ $seen_terminal == 1 ]]; then
  linger_ok=1
  for i in $(seq 1 "$API_LINGER_EXPECT_SEC"); do
    code=$(curl -sS -m 1 -o /dev/null -w '%{http_code}' -H 'Accept: application/json' "$BASE_URL/status" || echo 000)
    if [[ "$code" != 200 ]]; then linger_ok=0; break; fi
    sleep 1
  done
  [[ $linger_ok == 1 ]] && note_pass "/status lingers for >= ${API_LINGER_EXPECT_SEC}s" || note_fail "/status did not linger ${API_LINGER_EXPECT_SEC}s"
fi

echo "== Phase 2: /metrics/json idle snapshot + terminal markers =="
non_empty_idle=0
terminal_flag=0
stable_ts=0
last_ts=""
for i in $(seq 1 10); do
  tmpm=$(mktemp "$WORKDIR/.metrics.XXXX.json")
  code=$(fetch_json "$BASE_URL/metrics/json" "$tmpm")
  if [[ "$code" == 200 ]]; then
    count=$(jq -r 'length' "$tmpm" 2>/dev/null || echo 0)
    if [[ "$count" -gt 0 ]]; then
      non_empty_idle=1
      ts=$(jq -r '[.[].timestamp // 0] | max' "$tmpm")
      if [[ -n "$last_ts" && "$ts" == "$last_ts" ]]; then stable_ts=1; fi
      last_ts="$ts"
      has_term=$(jq -r 'any(.[]; (.terminal==true) and ((.operations.success_rate_last // 0)==0) and ((.bandwidth.bytes_rate_last // 0)==0)) | if . then 1 else 0 end' "$tmpm")
      [[ "$has_term" == "1" ]] && terminal_flag=1
    fi
  fi
  sleep 1
done
[[ $non_empty_idle == 1 ]] && note_pass "metrics/json non-empty at idle" || note_fail "metrics/json empty at idle"
[[ $terminal_flag == 1 ]] && note_pass "terminal:true with zero last rates present" || note_fail "missing terminal:true/zero last rates"
[[ $stable_ts == 1 ]] && note_pass "timestamp stable at idle" || note_fail "timestamp not stable at idle"

echo "== Phase 3 & 4: /logs HEAD + per-step index.json =="
STEP_ID=$(jq -r '[ .[] | select(.terminal==true and (.step_id // "") != "") | .step_id ] | first // ""' "$tmpm")

if [[ -z "$STEP_ID" ]]; then
  note_fail "no terminal step_id found in metrics.json"
else
  code=$(fetch_json "$BASE_URL/logs/$STEP_ID/index.json" "$WORKDIR/index.json")
  if [[ "$code" == 200 ]]; then
    items_count=$(jq -r '(.items // []) | length' "$WORKDIR/index.json")
    [[ "$items_count" -ge 0 ]] || items_count=0
    if [[ "$items_count" -ge 1 ]]; then
      note_pass "index.json lists ${items_count} items"
    else
      note_pass "index.json present with empty items (allowed by spec)"
    fi

    # If items exist, validate HEAD for each; else, try a common one
    if [[ "$items_count" -ge 1 ]]; then
      mapfile -t loggers < <(jq -r '(.items // []) | .[].logger | select(. != null)' "$WORKDIR/index.json")
    else
      loggers=(metrics.FileTotal)
    fi

    head_all_ok=1
    for lg in "${loggers[@]}"; do
      mapfile -t head_out < <(curl -sS -I "$BASE_URL/logs/$STEP_ID/$lg" || true)
      status_line=$(printf '%s\n' "${head_out[@]}" | head -n1)
      cl=$(printf '%s\n' "${head_out[@]}" | awk -F': ' 'tolower($1)=="content-length"{print $2}' | tr -d '\r')
      lm=$(printf '%s\n' "${head_out[@]}" | awk -F': ' 'tolower($1)=="last-modified"{print $2}' | tr -d '\r')
      echo "$status_line" | grep -q " 200 " && ok200=1 || ok200=0
      if [[ "$ok200" != 1 || -z "$cl" || -z "$lm" ]]; then head_all_ok=0; fi
    done
    [[ $head_all_ok == 1 ]] && note_pass "HEAD /logs/<stepId>/<logger> returns 200 with size/mtime" || note_fail "HEAD missing 200/headers for some logger(s)"

    # Unknown logger must be 404
    unk_code=$(curl -sS -o /dev/null -w '%{http_code}' -I "$BASE_URL/logs/$STEP_ID/does.not.exist" || echo 000)
    [[ "$unk_code" == 404 ]] && note_pass "HEAD unknown logger returns 404" || note_fail "HEAD unknown logger returned $unk_code"
  else
    note_fail "index.json HTTP $code"
  fi
fi

echo "\n== Summary =="
echo "Pass: ${#PASS[@]}"; printf ' - %s\n' "${PASS[@]}" || true
echo "Fail: ${#FAIL[@]}"; printf ' - %s\n' "${FAIL[@]}" || true

# Ensure background process (if any) is stopped
if [[ -n "${RUN_PID}" ]] && ps -p "$RUN_PID" >/dev/null 2>&1; then
  echo "== Stopping background spt (pid $RUN_PID) =="
  kill -INT "$RUN_PID" >/dev/null 2>&1 || true
  sleep 2
fi

exit $([[ ${#FAIL[@]} -eq 0 ]] && echo 0 || echo 1)
