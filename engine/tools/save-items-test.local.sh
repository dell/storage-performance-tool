#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# save-items-test.local.sh – exercise the save-items / read-from-file feature
#
# Runs a small write-then-read scenario (1000 × 1 KB) via the engine JAR
# to verify items.csv round-tripping:
#   Phase 1  – create 1000 objects, saving items.csv
#   Phase 2  – read back every object using items.csv
#   Phase 3  – (optional) delete all objects using items.csv
#
# Skip cleanup: SKIP_CLEANUP=1 ./save-items-test.local.sh
# Override knobs via env or .env file (same as jartest.local.sh).
#
# Results are preserved under ./results/save-items-<timestamp>/.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"
RUN_SCRIPT="$ENGINE_ROOT/bundle/build/dist/run.sh"

# ---- Snapshot caller-exported overrides before sourcing .env ---------------
declare -A _caller_env
for _v in S3_ENDPOINTS S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_AUTH_VERSION \
          SAVEITEMS_CONCURRENCY SAVEITEMS_OBJECT_SIZE SAVEITEMS_OBJECT_COUNT \
          SAVEITEMS_READ_COUNT SKIP_CLEANUP; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

# ---- Source .env -----------------------------------------------------------
if [[ -f "$GIT_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$GIT_ROOT/.env"
elif [[ -f "$ENGINE_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ENGINE_ROOT/.env"
fi

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

# ---- Defaults --------------------------------------------------------------
: "${S3_ENDPOINTS:=http://127.0.0.1:9000}"
: "${S3_ACCESS_KEY:=AWS_ACCESS_KEY_ID_EXAMPLE}"
: "${S3_SECRET_KEY:=AWS_SECRET_ACCESS_KEY_EXAMPLE}"
: "${S3_BUCKET:=testwrites}"
: "${S3_AUTH_VERSION:=4}"
: "${SAVEITEMS_CONCURRENCY:=4}"
: "${SAVEITEMS_OBJECT_SIZE:=1KB}"
: "${SAVEITEMS_OBJECT_COUNT:=1000}"
: "${SAVEITEMS_READ_COUNT:=1000}"
: "${SKIP_CLEANUP:=}"

NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

# ---- Results directory -----------------------------------------------------
RESULTS_DIR="${SCRIPT_DIR}/results/save-items-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
ITEMS_FILE="${RESULTS_DIR}/items.csv"

# ---- Shared flags ----------------------------------------------------------
SHARED_ARGS=(
  "--storage-driver-type=s3"
  "--storage-net-node-addrs=${NODE_ADDRS}"
  "--storage-auth-version=${S3_AUTH_VERSION}"
  "--storage-auth-uid=${S3_ACCESS_KEY}"
  "--storage-auth-secret=${S3_SECRET_KEY}"
  "--item-output-path=/${S3_BUCKET}"
  "--item-data-size=${SAVEITEMS_OBJECT_SIZE}"
  "--storage-driver-limit-concurrency=${SAVEITEMS_CONCURRENCY}"
)

# ---- Build if needed -------------------------------------------------------
if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$ENGINE_ROOT" && ./gradlew -q :bundle:build)
fi

# ---- Phase 1: Write -------------------------------------------------------
echo "== Phase 1: create ${SAVEITEMS_OBJECT_COUNT} × ${SAVEITEMS_OBJECT_SIZE} objects =="
"$RUN_SCRIPT" \
  "${SHARED_ARGS[@]}" \
  --load-op-type=create \
  --load-op-limit-count="${SAVEITEMS_OBJECT_COUNT}" \
  --item-output-file="${ITEMS_FILE}" \
  --load-step-id=save-items-create \
  "$@"

ITEM_LINES="$(wc -l < "$ITEMS_FILE")"
echo "== Phase 1 complete – items.csv has ${ITEM_LINES} lines =="
if [[ "$ITEM_LINES" -lt 1 ]]; then
  echo "ERROR: items.csv is empty; aborting." >&2
  exit 1
fi

# ---- Phase 2: Read --------------------------------------------------------
echo "== Phase 2: read ${SAVEITEMS_READ_COUNT} items from items.csv =="
"$RUN_SCRIPT" \
  "${SHARED_ARGS[@]}" \
  --load-op-type=read \
  --load-op-limit-count="${SAVEITEMS_READ_COUNT}" \
  --item-input-file="${ITEMS_FILE}" \
  --load-step-id=save-items-read \
  "$@"

echo "== Phase 2 complete =="

# ---- Phase 3: Cleanup (optional) ------------------------------------------
if [[ -z "${SKIP_CLEANUP}" ]]; then
  echo "== Phase 3: delete objects from items.csv =="
  "$RUN_SCRIPT" \
    "${SHARED_ARGS[@]}" \
    --load-op-type=delete \
    --item-input-file="${ITEMS_FILE}" \
    --load-step-id=save-items-delete \
    "$@"
  echo "== Phase 3 complete =="
else
  echo "== Phase 3 skipped (SKIP_CLEANUP=${SKIP_CLEANUP}) =="
fi

echo "== save-items round-trip test finished =="
echo "== Results preserved in: ${RESULTS_DIR} =="
