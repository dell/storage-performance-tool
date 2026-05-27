#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"

declare -A _caller_env
for _v in HOSTS WORKER_HOSTS SSH_OPTS WORKER_CONTAINER_NAME RETRIES RETRY_SLEEP PURGE_IMAGES SPT_IMAGE; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

[ -f "$GIT_ROOT/.env" ] && source "$GIT_ROOT/.env"
[ -f "$ENGINE_ROOT/.env" ] && source "$ENGINE_ROOT/.env"

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

: "${SSH_OPTS:=-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10}"
: "${WORKER_CONTAINER_NAME:=spt-worker}"
: "${RETRIES:=3}"
: "${RETRY_SLEEP:=1}"
: "${PURGE_IMAGES:=false}"

WORKER_HOSTS="${WORKER_HOSTS:-${HOSTS:-}}"
if [ -z "$WORKER_HOSTS" ]; then
  echo "ERROR: WORKER_HOSTS or HOSTS must be set in .env or environment" >&2
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --purge-images|--prune-images)
      PURGE_IMAGES=true; shift ;;
    --retry|-r)
      RETRIES="${2:-}"; shift 2 ;;
    --help|-h)
      cat <<'USAGE'
Usage: jar-nodes-down.sh [--purge-images] [--retry N]

Stops/removes managed JAR worker containers on remote hosts.
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

failed_nodes=()
for target in "${HOST_ARR[@]}"; do
  target_trimmed="${target//[$'\n\t\r ']}"
  [ -z "$target_trimmed" ] && continue
  host_no_user="${target_trimmed#*@}"
  echo "--- [$target_trimmed] stopping worker containers"
  attempt=1
  success=false
  while [ "$attempt" -le "$RETRIES" ]; do
    if ssh $SSH_OPTS "$target_trimmed" \
      WORKER_CONTAINER_NAME="$WORKER_CONTAINER_NAME" \
      HOST_LABEL="$host_no_user" \
      PURGE_IMAGES="$PURGE_IMAGES" \
      SPT_IMAGE="${SPT_IMAGE:-}" \
      'bash -s' <<'EOS'
set -euo pipefail
name="$WORKER_CONTAINER_NAME"
host_label="$HOST_LABEL"

docker rm -f "$name" >/dev/null 2>&1 || true

ids=$(docker ps -a --filter "label=spt.managed=true" --filter "label=spt.role=worker" --filter "label=spt.host=${host_label}" -q || true)
if [ -n "$ids" ]; then
  docker rm -f $ids >/dev/null 2>&1 || true
fi

if [ "${PURGE_IMAGES:-false}" = "true" ]; then
  if [ -n "${SPT_IMAGE:-}" ]; then
    docker image rm -f "$SPT_IMAGE" >/dev/null 2>&1 || true
  fi
  docker image prune -f >/dev/null 2>&1 || true
fi

left=$(docker ps -a --filter "label=spt.managed=true" --filter "label=spt.role=worker" --filter "label=spt.host=${host_label}" --format '{{.Names}}' || true)
if [ -n "$left" ]; then
  echo "managed worker containers remain: $left" >&2
  exit 3
fi
echo "clean"
EOS
    then
      success=true
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

echo "All worker nodes cleaned."
