#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$REPO_ROOT/bundle/build/dist/run.sh"

# Load repo-local environment defaults if present so users can keep
# credentials and tuning knobs out of the script itself.
if [[ -f "$REPO_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
fi

# Allow overrides via environment variables. Keep variable names aligned with
# other helper scripts (e.g., listtest.local.sh) so a single .env works.
: "${S3_ENDPOINTS:=http://127.0.0.1:9000}"
: "${S3_ACCESS_KEY:=AWS_ACCESS_KEY_ID_EXAMPLE}"
: "${S3_SECRET_KEY:=AWS_SECRET_ACCESS_KEY_EXAMPLE}"
: "${S3_BUCKET:=testwrites}"
: "${S3_AUTH_VERSION:=4}"
: "${JARTEST_CONCURRENCY:=4}"
: "${JARTEST_OBJECT_SIZE:=1MB}"
: "${JARTEST_OP_LIMIT_COUNT:=5000}"

# Convert comma-separated endpoints into host:port pairs expected by Spt.
NODE_ADDRS="${S3_ENDPOINTS//http:\/\/}"
NODE_ADDRS="${NODE_ADDRS//https:\/\/}"

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Run script not found at $RUN_SCRIPT; building distribution..." >&2
  (cd "$REPO_ROOT" && ./gradlew -q :bundle:build)
fi

exec "$RUN_SCRIPT" \
  --storage-driver-type=s3 \
  --storage-net-node-addrs="${NODE_ADDRS}" \
  --storage-auth-version="${S3_AUTH_VERSION}" \
  --storage-auth-uid="${S3_ACCESS_KEY}" \
  --storage-auth-secret="${S3_SECRET_KEY}" \
  --item-output-path="/${S3_BUCKET}" \
  --storage-driver-limit-concurrency="${JARTEST_CONCURRENCY}" \
  --item-data-size="${JARTEST_OBJECT_SIZE}" \
  --load-op-limit-count="${JARTEST_OP_LIMIT_COUNT}" \
  "$@"
