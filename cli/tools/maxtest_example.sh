#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# maxtest_example.sh – Reproduce an obsperfdb MAX benchmark using the SPT CLI.
#
# Runs 12 sequential steps (6 WRITE + 6 READ) across 6 object sizes,
# matching the workload matrix from the perf team's Mongoose-based MAX test.
#
# Writes use --save-items to persist items.csv; reads consume them via
# --items-file, skipping the seed phase entirely.  Cooldown pauses between
# steps let the storage system stabilize (cache flush, GC, compaction).
#
# Results land in ./results/<label>-<ts>/ and can be post-processed into
# obsperfdb CSV format with cli/mike/planning/SPT_OBSPERFDB_INVESTIGATION.md
# (see the spt_to_obsperfdb.py script in that document).
#
# Usage:
#   ./tools/maxtest_example.sh                                  # defaults from .env
#   ./tools/maxtest_example.sh --cooldown 300                   # 5-min cooldown
#   ./tools/maxtest_example.sh --skip-writes                    # reads only (reuse items)
#   ./tools/maxtest_example.sh --skip-reads                     # writes only
#   HOSTS="node1,node2,node3,node4" ./tools/maxtest_example.sh          # multi-host
# ---------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# ---- Connection defaults (from .env or environment) ------------------------
HOSTS=${HOSTS:-"127.0.0.1"}
S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ENDPOINTS=${S3_ENDPOINTS:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
S3_BUCKET=${S3_BUCKET:-""}

# ---- Test knobs ------------------------------------------------------------
COOLDOWN=${COOLDOWN:-900}          # seconds between steps (default 15 min)
RESULTS_DIR=${RESULTS_DIR:-"./results"}
SKIP_WRITES=${SKIP_WRITES:-false}
SKIP_READS=${SKIP_READS:-false}
FORCE=${FORCE:-true}
VERBOSE=${VERBOSE:-false}
DRY_RUN=${DRY_RUN:-false}
MIN_HOSTS=${MIN_HOSTS:-""}

# ---- Workload matrix -------------------------------------------------------
# Each row: SIZE  WRITE_THREADS  WRITE_DURATION  READ_THREADS  READ_DURATION
#
# Thread counts are per-node (SPT's --threads flag).  With N --test-hosts the
# total concurrency is threads × N, matching the Mongoose model.
STEPS=(
  "10KB   120  30m  320  5m"
  "100KB  200  30m  320  5m"
  "1MB    160  5m   160  5m"
  "10MB   80   5m   80   5m"
  "100MB  40   5m   80   5m"
  "200MB  40   5m   80   5m"
)

# ---- CLI arg parsing -------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --hosts)            HOSTS="${2:-}";        shift 2 ;;
    --s3-endpoints)     S3_ENDPOINTS="${2:-}"; shift 2 ;;
    --s3-endpoint)      S3_ENDPOINT="${2:-}";  shift 2 ;;
    --s3-access-key)    S3_ACCESS_KEY="${2:-}"; shift 2 ;;
    --s3-secret-key)    S3_SECRET_KEY="${2:-}"; shift 2 ;;
    --s3-bucket)        S3_BUCKET="${2:-}";    shift 2 ;;
    --cooldown)         COOLDOWN="${2:-}";     shift 2 ;;
    --results-dir)      RESULTS_DIR="${2:-}";  shift 2 ;;
    --min-hosts)        MIN_HOSTS="${2:-}";    shift 2 ;;
    --skip-writes)      SKIP_WRITES=true;     shift 1 ;;
    --skip-reads)       SKIP_READS=true;      shift 1 ;;
    --force)            FORCE=true;           shift 1 ;;
    --no-force)         FORCE=false;          shift 1 ;;
    --verbose)          VERBOSE=true;         shift 1 ;;
    --dry-run)          DRY_RUN=true;         shift 1 ;;
    -h|--help)
      cat << 'EOF'
Usage: maxtest.sh [options]

Reproduce an obsperfdb-style MAX benchmark (6 sizes × write + read).

Connection:
  --hosts CSV              Comma-separated [user@]host list (env: HOSTS)
  --s3-endpoint URL        Single S3 endpoint (env: S3_ENDPOINT)
  --s3-endpoints CSV       Multiple S3 endpoints (env: S3_ENDPOINTS)
  --s3-access-key KEY      S3 access key (env: S3_ACCESS_KEY)
  --s3-secret-key KEY      S3 secret key (env: S3_SECRET_KEY)
  --s3-bucket NAME         S3 bucket (env: S3_BUCKET)

Test Control:
  --cooldown SECONDS       Pause between steps (default: 900 = 15 min)
  --results-dir DIR        Results output directory (default: ./results)
  --min-hosts N            Minimum hosts required (default: all)
  --skip-writes            Skip write phase (reuse existing items.csv)
  --skip-reads             Skip read phase (writes only)
  --dry-run                Print commands without executing
  --[no-]force             Auto-resolve port conflicts (default: true)
  --verbose                Enable verbose output

Environment:
  All --s3-* flags can be set via S3_* env vars or .env file.
  COOLDOWN, RESULTS_DIR, HOSTS also settable via env.
EOF
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ---- Helpers ---------------------------------------------------------------

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

cooldown() {
  if [[ "$COOLDOWN" -gt 0 ]]; then
    log "Cooldown: sleeping ${COOLDOWN}s..."
    sleep "$COOLDOWN"
  fi
}

# Build the shared connection + control flags as an array.
build_shared_args() {
  local args=(
    --access-key "$S3_ACCESS_KEY"
    --secret-key "$S3_SECRET_KEY"
    --test-hosts "$HOSTS"
    --bucket "$S3_BUCKET"
    --headless
    --results-dir "$RESULTS_DIR"
  )
  if [[ -n "$S3_ENDPOINTS" ]]; then
    args+=(--endpoints "$S3_ENDPOINTS")
  elif [[ -n "$S3_ENDPOINT" ]]; then
    args+=(--endpoint "$S3_ENDPOINT")
  fi
  [[ -n "$MIN_HOSTS" ]] && args+=(--min-hosts "$MIN_HOSTS")
  $FORCE   && args+=(--force)
  $VERBOSE && args+=(--verbose)
  printf '%s\n' "${args[@]}"
}

# Find the items.csv artifact for a given write label.
# Returns the path or exits with an error.
find_items_csv() {
  local label="$1"
  local pattern="${RESULTS_DIR}/${label}-*/*.items.csv"
  local matches=()
  # Use nullglob so the array is empty (not the literal glob) on no match.
  local _old_nullglob
  _old_nullglob="$(shopt -p nullglob || true)"
  shopt -s nullglob
  matches=( $pattern )
  eval "$_old_nullglob"
  if [[ ${#matches[@]} -eq 0 ]]; then
    echo "ERROR: No items.csv found for label '${label}' (glob: ${pattern})" >&2
    return 1
  fi
  # Use the most recent match (last alphabetically by timestamp dir).
  echo "${matches[-1]}"
}

run_cmd() {
  if $DRY_RUN; then
    log "[dry-run] $*"
  else
    "$@"
  fi
}

# ---- Read shared args into array -------------------------------------------
mapfile -t SHARED_ARGS < <(build_shared_args)

# ---- Banner ----------------------------------------------------------------
log "===== MAX Benchmark ====="
log "Hosts:      $HOSTS"
if [[ -n "$S3_ENDPOINTS" ]]; then
  log "Endpoints:  $S3_ENDPOINTS"
else
  log "Endpoint:   $S3_ENDPOINT"
fi
log "Bucket:     $S3_BUCKET"
log "Cooldown:   ${COOLDOWN}s"
log "Results:    $RESULTS_DIR"
log "Steps:      ${#STEPS[@]} sizes × (write + read)"
log ""

# ---- Phase 1: Writes ------------------------------------------------------
if ! $SKIP_WRITES; then
  log "===== Phase 1: WRITE (${#STEPS[@]} sizes) ====="
  first=true
  for step in "${STEPS[@]}"; do
    read -r size w_threads w_duration _r_threads _r_duration <<< "$step"
    label="w-$(echo "$size" | tr '[:upper:]' '[:lower:]')"

    $first || cooldown
    first=false

    log "--- WRITE ${size}: threads=${w_threads}, duration=${w_duration}, label=${label} ---"
    run_cmd "$ROOT_DIR"/spt run write \
      "${SHARED_ARGS[@]}" \
      --threads "$w_threads" \
      --object-size "$size" \
      --duration "$w_duration" \
      --save-items \
      --label "$label"
  done
  log "===== Phase 1 complete ====="
  log ""
fi

# ---- Phase 2: Reads -------------------------------------------------------
if ! $SKIP_READS; then
  log "===== Phase 2: READ (${#STEPS[@]} sizes) ====="
  first=true
  for step in "${STEPS[@]}"; do
    read -r size _w_threads _w_duration r_threads r_duration <<< "$step"
    w_label="w-$(echo "$size" | tr '[:upper:]' '[:lower:]')"
    r_label="r-$(echo "$size" | tr '[:upper:]' '[:lower:]')"

    # Locate the items.csv from the corresponding write step.
    items_file="$(find_items_csv "$w_label")"
    log "Items file: $items_file ($(wc -l < "$items_file") lines)"

    $first || cooldown
    first=false

    log "--- READ ${size}: threads=${r_threads}, duration=${r_duration}, label=${r_label} ---"
    run_cmd "$ROOT_DIR"/spt run read \
      "${SHARED_ARGS[@]}" \
      --threads "$r_threads" \
      --object-size "$size" \
      --duration "$r_duration" \
      --items-file "$items_file" \
      --label "$r_label"
  done
  log "===== Phase 2 complete ====="
  log ""
fi

log "===== MAX Benchmark finished ====="
log "Results in: $RESULTS_DIR"
log "Post-process with: python3 spt_to_obsperfdb.py --results-dir $RESULTS_DIR"
