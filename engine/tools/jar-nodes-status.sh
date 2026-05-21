#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GIT_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"

declare -A _caller_env
for _v in HOSTS WORKER_HOSTS RUN_PORT SSH_OPTS WORKER_CONTAINER_NAME; do
  [[ -n "${!_v+set}" ]] && _caller_env[$_v]="${!_v}"
done

[ -f "$GIT_ROOT/.env" ] && source "$GIT_ROOT/.env"
[ -f "$ENGINE_ROOT/.env" ] && source "$ENGINE_ROOT/.env"

for _v in "${!_caller_env[@]}"; do
  export "$_v=${_caller_env[$_v]}"
done
unset _caller_env _v

: "${RUN_PORT:=9999}"
: "${SSH_OPTS:=-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10}"
: "${WORKER_CONTAINER_NAME:=spt-worker}"

WORKER_HOSTS="${WORKER_HOSTS:-${HOSTS:-}}"
if [ -z "$WORKER_HOSTS" ]; then
  echo "ERROR: WORKER_HOSTS or HOSTS must be set in .env or environment" >&2
  exit 1
fi

LOOP_SECONDS=""
if [ "${1:-}" = "--loop-seconds" ]; then
  LOOP_SECONDS="${2:-}"
  if ! [[ "$LOOP_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --loop-seconds requires an integer value" >&2
    exit 1
  fi
fi

IFS=',' read -r -a HOST_ARR <<<"$WORKER_HOSTS"
has_jq=0
if command -v jq >/dev/null 2>&1; then has_jq=1; fi

once() {
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "[$ts] Checking ${#HOST_ARR[@]} worker nodes (run-port=$RUN_PORT)"
  for target in "${HOST_ARR[@]}"; do
    t="${target//[$'\n\t\r ']}"
    [ -z "$t" ] && continue
    host="${t#*@}"

    container_state=$(ssh $SSH_OPTS "$t" \
      "docker ps --filter \"label=spt.managed=true\" --filter \"label=spt.role=worker\" --filter \"label=spt.host=$host\" --format '{{.Status}}' | head -n1" || true)
    if [ -z "$container_state" ]; then
      container_state=$(ssh $SSH_OPTS "$t" \
        "docker ps -a --filter name=^/${WORKER_CONTAINER_NAME}$ --format '{{.Status}}' | head -n1" || true)
      [ -z "$container_state" ] && container_state="not-found"
    fi

    ready_code=$(curl -m 2 -s -o /dev/null -w '%{http_code}' "http://${host}:${RUN_PORT}/ready" || echo "000")
    status_code=$(curl -m 2 -s -o /dev/null -w '%{http_code}' "http://${host}:${RUN_PORT}/status" || echo "000")
    metrics_summary=""
    if [ $has_jq -eq 1 ] && [ "$status_code" = "200" ]; then
      metrics_json=$(curl -m 2 -sf "http://${host}:${RUN_PORT}/metrics/json" || true)
      if [ -n "$metrics_json" ]; then
        cp=$(echo "$metrics_json" | jq -r '.. | .completion_percent? // empty' | tail -n1)
        if [ -n "$cp" ]; then
          metrics_summary="completion=${cp}%"
        fi
      fi
    fi

    printf -- "- %-35s container=%-22s ready=%s status=%s %s\n" "$host" "$container_state" "$ready_code" "$status_code" "$metrics_summary"
  done
}

if [ -n "$LOOP_SECONDS" ]; then
  while true; do
    once
    sleep "$LOOP_SECONDS"
  done
else
  once
fi
