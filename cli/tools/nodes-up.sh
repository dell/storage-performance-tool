#!/usr/bin/env bash
set -euo pipefail

# Turnkey: start Spt workers in --run-node mode on remote hosts.
# - Reads config from ./.env (copy .env.example) or environment.
# - Uses host networking to avoid RMI/NAT issues.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# New convention: PRIMARY_HOST + WORKER_HOSTS. Fall back to HOSTS if unset.
PRIMARY_HOST=${PRIMARY_HOST:-}
WORKER_HOSTS=${WORKER_HOSTS:-}
if [ -z "${WORKER_HOSTS}" ]; then
  HOSTS=${HOSTS:-"root@spt-test-1.cec.delllabs.net,root@spt-test-2.cec.delllabs.net,root@spt-test-3.cec.delllabs.net,root@spt-test-4.cec.delllabs.net"}
  WORKER_HOSTS="$HOSTS"
fi
SPT_IMAGE=${SPT_IMAGE:-"ghcr.io/dell/storage-performance-tool"}
RUN_PORT=${RUN_PORT:-9999}
RMI_PORT=${RMI_PORT:-1099}
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}
PULL_ALWAYS=${PULL_ALWAYS:-true}
CONTAINER_NAME=${CONTAINER_NAME:-${WORKER_CONTAINER_NAME:-spt-worker}}
RETRIES=${RETRIES:-3}
RETRY_SLEEP=${RETRY_SLEEP:-1}

# Parse optional args
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

# Safety: ensure PRIMARY_HOST is not in WORKER_HOSTS (by hostname)
if [ -n "$PRIMARY_HOST" ]; then
  primary_no_user="${PRIMARY_HOST#*@}"
  IFS=',' read -r -a _wh <<<"$WORKER_HOSTS"
  for h in "${_wh[@]}"; do
    [ -z "$h" ] && continue
    if [ "${h#*@}" = "$primary_no_user" ]; then
      echo "ERROR: PRIMARY_HOST ($PRIMARY_HOST) must not appear in WORKER_HOSTS ($WORKER_HOSTS)." >&2
      exit 2
    fi
  done
fi

IFS=',' read -r -a HOST_ARR <<<"$WORKER_HOSTS"

echo "Starting Spt workers on ${#HOST_ARR[@]} hosts..."
failed_nodes=()
started_count=0
for target in "${HOST_ARR[@]}"; do
  target_trimmed="${target//[$'\n\t\r ']}"
  [ -z "$target_trimmed" ] && continue

  echo "--- [$target_trimmed] launching worker"
  attempt=1
  success=false
  while [ "$attempt" -le "$RETRIES" ]; do
    # Send script over stdin to avoid complex quoting issues
    if ssh $SSH_OPTS "$target_trimmed" \
      SPT_IMAGE="$SPT_IMAGE" RUN_PORT="$RUN_PORT" RMI_PORT="$RMI_PORT" PULL_ALWAYS="$PULL_ALWAYS" CONTAINER_NAME="$CONTAINER_NAME" \
      'bash -s' <<'EOS'
set -euo pipefail
name="${CONTAINER_NAME:-spt-node}"
img="$SPT_IMAGE"
run_port="$RUN_PORT"
rmi_port="$RMI_PORT"
pull_always="$PULL_ALWAYS"

if [ "${pull_always}" = "true" ]; then
  docker pull "$img" >/dev/null
fi

docker rm -f "$name" >/dev/null 2>&1 || true

# Determine primary IP for RMI hostname binding
# Prefer the default-route source IP to avoid docker0/bridge addresses (e.g., 172.17.0.1)
ip=""
if command -v ip >/dev/null 2>&1; then
  ip=$(ip -o route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if ($i=="src"){print $(i+1); exit}}') || true
fi
if [ -z "$ip" ]; then
  # Fallback: first non-loopback, non-docker interface IPv4
  if command -v ip >/dev/null 2>&1; then
    ip=$(ip -br -4 addr show scope global | awk '$1!~/(docker|br|veth|lo)/{print $3}' | cut -d/ -f1 | head -n1) || true
  fi
fi
if [ -z "$ip" ]; then
  # Last resort: hostname lookups
  ip=$(hostname -I 2>/dev/null | awk '{print $1}') || true
fi
if [ -z "$ip" ]; then
  ip=$(getent hosts "$(hostname -f)" | awk '{print $1}' | head -n1) || true
fi
echo "RMI host IP chosen: ${ip:-<unset>}" >&2

docker run -d --restart unless-stopped \
  --name "$name" \
  --network host \
  -e JAVA_OPTS="-Djava.rmi.server.hostname=${ip}" \
  "$img" \
    --run-node \
    --run-port="${run_port}" \
    --load-step-node-port="${rmi_port}"

# quick readiness probe on REST API
for i in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:${run_port}/run" -I >/dev/null; then
    echo "Worker API ready on ${ip}:${run_port}"
    exit 0
  fi
  sleep 0.5
done
echo "WARN: REST API not reachable locally on port ${run_port} (container may still be initializing)"
exit 2
EOS
    then
      success=true
      started_count=$((started_count+1))
      break
    else
      echo "...retry $attempt/$RETRIES on $target_trimmed in ${RETRY_SLEEP}s" >&2
      sleep "$RETRY_SLEEP"
      attempt=$((attempt+1))
    fi
  done

  if [ "$success" != true ]; then
    failed_nodes+=("$target_trimmed")
  fi
done

if [ ${#failed_nodes[@]} -gt 0 ]; then
  echo "Failed to start/verify on: ${failed_nodes[*]}" >&2
  exit 2
fi

echo "All workers started and verified ($started_count/${#HOST_ARR[@]})."
