#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"

declare -A _caller_env
for _v in HOSTS WORKER_HOSTS SPT_IMAGE RUN_PORT RMI_PORT SSH_OPTS WORKER_CONTAINER_NAME PULL_ALWAYS RETRIES RETRY_SLEEP; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

[ -f "$GIT_ROOT/.env" ] && source "$GIT_ROOT/.env"
[ -f "$ENGINE_ROOT/.env" ] && source "$ENGINE_ROOT/.env"

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

: "${SPT_IMAGE:=ghcr.io/dell/storage-performance-tool}"
: "${RUN_PORT:=9999}"
: "${RMI_PORT:=1099}"
: "${SSH_OPTS:=-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10}"
: "${WORKER_CONTAINER_NAME:=spt-worker}"
: "${PULL_ALWAYS:=true}"
: "${RETRIES:=3}"
: "${RETRY_SLEEP:=1}"

WORKER_HOSTS="${WORKER_HOSTS:-${HOSTS:-}}"
if [ -z "$WORKER_HOSTS" ]; then
  echo "ERROR: WORKER_HOSTS or HOSTS must be set in .env or environment" >&2
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --retry|-r)
      RETRIES="${2:-}"; shift 2 ;;
    --help|-h)
      cat <<'USAGE'
Usage: jar-nodes-up.sh [--retry N]

Starts remote Spt worker nodes in --run-node mode using Docker.
Configuration is loaded from .env (repo/engine) and environment.
USAGE
      exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2 ;;
  esac
done

if ! [[ "$RETRIES" =~ ^[0-9]+$ ]] || [ "$RETRIES" -lt 1 ]; then
  echo "ERROR: --retry requires a positive integer (got: $RETRIES)" >&2
  exit 2
fi

IFS=',' read -r -a HOST_ARR <<<"$WORKER_HOSTS"

echo "Starting JAR worker nodes on ${#HOST_ARR[@]} host(s)..."
echo "Image: $SPT_IMAGE"
echo "Ports: run=$RUN_PORT rmi=$RMI_PORT"

failed_nodes=()
started_count=0

for target in "${HOST_ARR[@]}"; do
  target_trimmed="${target//[$'\n\t\r ']}"
  [ -z "$target_trimmed" ] && continue
  host_no_user="${target_trimmed#*@}"
  echo "--- [$target_trimmed] starting worker container"
  attempt=1
  success=false
  while [ "$attempt" -le "$RETRIES" ]; do
    if ssh $SSH_OPTS "$target_trimmed" \
      SPT_IMAGE="$SPT_IMAGE" \
      RUN_PORT="$RUN_PORT" \
      RMI_PORT="$RMI_PORT" \
      PULL_ALWAYS="$PULL_ALWAYS" \
      CONTAINER_NAME="$WORKER_CONTAINER_NAME" \
      HOST_LABEL="$host_no_user" \
      'bash -s' <<'EOS'
set -euo pipefail
img="$SPT_IMAGE"
run_port="$RUN_PORT"
rmi_port="$RMI_PORT"
pull_always="$PULL_ALWAYS"
name="$CONTAINER_NAME"
host_label="$HOST_LABEL"

if [ "$pull_always" = "true" ]; then
  docker pull "$img" >/dev/null
fi

docker rm -f "$name" >/dev/null 2>&1 || true

ip=""
if command -v ip >/dev/null 2>&1; then
  ip=$(ip -o route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if ($i=="src"){print $(i+1); exit}}') || true
fi
if [ -z "$ip" ] && command -v ip >/dev/null 2>&1; then
  ip=$(ip -br -4 addr show scope global | awk '$1!~/(docker|br|veth|lo)/{print $3}' | cut -d/ -f1 | head -n1) || true
fi
if [ -z "$ip" ]; then
  ip=$(hostname -I 2>/dev/null | awk '{print $1}') || true
fi
if [ -z "$ip" ]; then
  ip=$(getent hosts "$(hostname -f)" | awk '{print $1}' | head -n1) || true
fi

docker run -d --restart unless-stopped \
  --name "$name" \
  --network host \
  --label "spt.managed=true" \
  --label "spt.role=worker" \
  --label "spt.host=${host_label}" \
  -e JAVA_OPTS="-Djava.rmi.server.hostname=${ip}" \
  "$img" \
    --run-node \
    --run-port="${run_port}" \
    --load-step-node-port="${rmi_port}" >/dev/null

for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${run_port}/ready" >/dev/null 2>&1; then
    echo "ready"
    exit 0
  fi
  sleep 0.5
done

echo "worker API not ready on port ${run_port}" >&2
exit 2
EOS
    then
      success=true
      started_count=$((started_count + 1))
      break
    else
      echo "...retry $attempt/$RETRIES on $target_trimmed in ${RETRY_SLEEP}s" >&2
      sleep "$RETRY_SLEEP"
      attempt=$((attempt + 1))
    fi
  done

  if [ "$success" != true ]; then
    failed_nodes+=("$target_trimmed")
  fi
done

if [ ${#failed_nodes[@]} -gt 0 ]; then
  echo "Failed hosts: ${failed_nodes[*]}" >&2
  exit 2
fi

echo "All workers started ($started_count/${#HOST_ARR[@]})."
