#!/usr/bin/env bash
set -euo pipefail

# Remove all locally cached Docker images on configured nodes.
# - Reads configuration from repo-root .env (PRIMARY_HOST, WORKER_HOSTS, HOSTS, SSH_OPTS).
# - Targets BOTH PRIMARY_HOST and WORKER_HOSTS by default (no --all flag).
# - Retries transient SSH/Docker errors a few times.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Build target list: prefer PRIMARY_HOST + WORKER_HOSTS; fall back to HOSTS; else canned defaults.
if [ -z "${HOSTS:-}" ]; then
  _p="${PRIMARY_HOST:-}"
  _w="${WORKER_HOSTS:-}"
  if [ -n "$_p" ] || [ -n "$_w" ]; then
    if [ -n "$_p" ] && [ -n "$_w" ]; then
      HOSTS="${_p},${_w}"
    else
      HOSTS="${_p}${_w}"
    fi
  else
    HOSTS=${HOSTS:-"root@spt-test-1.cec.delllabs.net,root@spt-test-2.cec.delllabs.net,root@spt-test-3.cec.delllabs.net,root@spt-test-4.cec.delllabs.net"}
  fi
fi

SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}
RETRIES=${RETRIES:-3}
RETRY_SLEEP=${RETRY_SLEEP:-1}

# CLI
while [ $# -gt 0 ]; do
  case "$1" in
    --retry|-r)
      RETRIES="${2:-}"; shift 2 ;;
    --help|-h)
      echo "Usage: $(basename "$0") [--retry N]"; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if ! [[ "$RETRIES" =~ ^[0-9]+$ ]] || [ "$RETRIES" -lt 1 ]; then
  echo "ERROR: --retry requires a positive integer (got: $RETRIES)" >&2
  exit 2
fi

IFS=',' read -r -a HOST_ARR <<<"$HOSTS"

echo "Removing all Docker images on ${#HOST_ARR[@]} host(s)..."
failed_nodes=()
for target in "${HOST_ARR[@]}"; do
  t="${target//[$'\n\t\r ']}"; [ -z "$t" ] && continue
  echo "--- [$t] docker rmi -f (all images)"

  attempt=1
  success=false
  while [ "$attempt" -le "$RETRIES" ]; do
    # Send a small bash script over SSH to control quoting and error handling
    if ssh $SSH_OPTS "$t" bash -s <<'EOS'
set -euo pipefail

# Count images before
before_cnt=$(docker images -aq | wc -l | tr -d '[:space:]')
if [ "$before_cnt" = "0" ]; then
  echo "clean"
  exit 0
fi

# Remove all image IDs returned by docker images -aq (if any)
ids=$(docker images -aq || true)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  docker rmi -f $ids >/dev/null 2>&1 || true
fi

# Verify nothing remains
after_cnt=$(docker images -aq | wc -l | tr -d '[:space:]')
if [ "$after_cnt" != "0" ]; then
  # Show a short summary to help triage (likely due to running containers)
  echo "ERROR: $after_cnt images remain" >&2
  docker images -a --format '{{.Repository}}:{{.Tag}} {{.ID}}' | head -n 10 >&2 || true
  exit 3
fi

echo "clean"
EOS
    then
      success=true
      break
    else
      echo "...retry $attempt/$RETRIES failed on $t; retrying in ${RETRY_SLEEP}s" >&2
      sleep "$RETRY_SLEEP"
      attempt=$((attempt+1))
    fi
  done

  if [ "$success" != true ]; then
    failed_nodes+=("$t")
  fi
done

if [ ${#failed_nodes[@]} -gt 0 ]; then
  echo "One or more nodes still have images present: ${failed_nodes[*]}" >&2
  echo "Tip: stop/remove containers first (tools/nodes-down.sh) and re-run." >&2
  exit 2
fi

echo "All targets report no local Docker images."

