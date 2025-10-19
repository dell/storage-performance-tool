#!/usr/bin/env bash
set -euo pipefail

# Check status of remote Spt workers and APIs.
# Default is one-shot; use --loop-seconds N to repeat.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Accept either HOSTS or PRIMARY_HOST + WORKER_HOSTS
if [ -z "${HOSTS:-}" ]; then
  _p="${PRIMARY_HOST:-}"
  _w="${WORKER_HOSTS:-}"
  if [ -n "$_p" ] && [ -n "$_w" ]; then
    HOSTS="${_p},${_w}"
  else
    HOSTS=${HOSTS:-"root@spt-test-1.cec.delllabs.net,root@spt-test-2.cec.delllabs.net,root@spt-test-3.cec.delllabs.net,root@spt-test-4.cec.delllabs.net"}
  fi
fi
RUN_PORT=${RUN_PORT:-9999}
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}
WORKER_CONTAINER_NAME=${WORKER_CONTAINER_NAME:-spt-worker}
ENTRY_CONTAINER_NAME=${ENTRY_CONTAINER_NAME:-spt-primary}

LOOP_SECONDS=""
if [ "${1:-}" = "--loop-seconds" ]; then
  LOOP_SECONDS="${2:-}"
  if ! [[ "$LOOP_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --loop-seconds requires an integer value" >&2
    exit 1
  fi
fi

IFS=',' read -r -a HOST_ARR <<<"$HOSTS"

# Identify primary host (no user prefix) if set
PRIMARY_NAME=""
if [ -n "${PRIMARY_HOST:-}" ]; then
  PRIMARY_NAME="${PRIMARY_HOST#*@}"
fi

has_jq=0
if command -v jq >/dev/null 2>&1; then has_jq=1; fi

function once() {
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "[$ts] Checking ${#HOST_ARR[@]} nodes (run-port=$RUN_PORT)"

  for target in "${HOST_ARR[@]}"; do
    t="${target//[$'\n\t\r ']}"; [ -z "$t" ] && continue
    host="$t"
    if [[ "$host" == *"@"* ]]; then
      host_no_user="${host#*@}"
    else
      host_no_user="$host"
    fi

    # Choose container name per role
    role_container="$WORKER_CONTAINER_NAME"
    is_primary=false
    if [ -n "$PRIMARY_NAME" ] && [ "$host_no_user" = "$PRIMARY_NAME" ]; then
      role_container="$ENTRY_CONTAINER_NAME"
      is_primary=true
    fi

    # Prefer managed label when available
    label_query=$(ssh $SSH_OPTS "$host" "docker ps --filter \"label=spt.managed=true\" --filter \"label=spt.host=$host_no_user\" --format '{{.Status}}|{{.Label \"spt.role\"}}|{{.Names}}|{{.Image}}' | head -n1" || true)
    cont_role=""
    cont_state=""
    cont_image="?"
    if [ -n "$label_query" ]; then
      cont_state="${label_query%%|*}"
      rest="${label_query#*|}"
      cont_role="${rest%%|*}"
      rest="${rest#*|}"
      # we don't currently display the name, but keep parsing to advance
      cont_image="${rest#*|}"
    else
      # Container status via SSH (legacy name-based fallback)
      c_status=$(ssh $SSH_OPTS "$host" "docker ps --filter name=^/${role_container}$ --format '{{.Status}}|{{.Image}}' || true")
      if [ -z "$c_status" ]; then
        c_status=$(ssh $SSH_OPTS "$host" "docker ps -a --filter name=^/${role_container}$ --format '{{.Status}}|{{.Image}}' || true")
        [ -z "$c_status" ] && c_status="not-found|?"
      fi
      cont_state="${c_status%%|*}"
      cont_image="${c_status#*|}"
    fi

    # API check: workers expose /run; entry does not in our workflow
    if [ -z "$cont_state" ]; then
      cont_state="not-found"
    fi

    if [ -n "$cont_role" ]; then
      if [ "$cont_role" = "entry" ]; then
        is_primary=true
      elif [ "$cont_role" = "worker" ] || [ "$cont_role" = "node" ]; then
        is_primary=false
      fi
    fi

    if [ "$is_primary" = true ]; then
      http_code="n/a"
    else
      http_code=$(curl -m 2 -s -o /dev/null -w '%{http_code}' -I "http://${host_no_user}:${RUN_PORT}/run" || echo "000")
    fi

    metrics_summary=""
    if [ $has_jq -eq 1 ] && [ "$is_primary" = false ]; then
      # Try to extract a completion percentage from the latest metrics JSON
      metrics_json=$(curl -m 2 -sf "http://${host_no_user}:${RUN_PORT}/metrics/json" || true)
      if [ -n "$metrics_json" ]; then
        # Search recursively for completion_percent fields; take the last one
        cp=$(echo "$metrics_json" | jq -r '.. | .completion_percent? // empty' | tail -n1)
        if [ -n "$cp" ]; then
          metrics_summary="completion=${cp}%"
        fi
      fi
    fi

    printf -- "- %-35s container=%-22s api=%s %s\n" "$host_no_user" "$cont_state" "$http_code" "$metrics_summary"
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
