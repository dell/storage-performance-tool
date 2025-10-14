#!/usr/bin/env bash
set -euo pipefail

# stopmulti.sh — Stop Spt containers on multiple hosts
#
# Usage:
#   tools/stopmulti.sh --hosts "root@host1,root@host2,127.0.0.1" [--dry-run] [--parallel]
#
# Options:
#   --hosts     Comma-separated list of hosts (may include user@ prefix)
#   --dry-run   Print what would be stopped, but don’t execute
#   --parallel  Run stops in parallel (background SSH), faster on large fleets
#
# Behavior:
# - Identifies containers whose image or name looks Spt-related
#   (image contains "spt" OR name contains "spt" or "spt").
# - For each host:
#     docker ps --format "{{.ID}} {{.Image}} {{.Names}}" | grep -Ei 'spt|spt' | awk '{print $1}' | xargs -r docker rm -f
# - Localhost targets run without SSH; remote hosts run via SSH.
# - Returns non-zero if any host command fails.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

HOSTS=""
DRYRUN=0
PARALLEL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts)
      HOSTS="$2"; shift 2 ;;
    --dry-run|--dryrun)
      DRYRUN=1; shift ;;
    --parallel|-p)
      PARALLEL=1; shift ;;
    -h|--help)
      sed -n '1,40p' "$0" | sed -n '1,20p'
      exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${HOSTS}" ]]; then
  echo "Error: --hosts required (comma-separated)." >&2
  exit 2
fi

IFS=',' read -r -a HOST_ARR <<<"$HOSTS"

stop_cmd='docker ps --format "{{.ID}} {{.Image}} {{.Names}}" | grep -Ei "spt|spt" | awk '{print $1}' | xargs -r docker rm -f'

stop_on_host() {
  local target="$1"
  if [[ "$target" == "127.0.0.1" || "$target" == "localhost" ]]; then
    if (( DRYRUN )); then
      echo "[DRY-RUN] local: $stop_cmd"
    else
      bash -lc "$stop_cmd"
      echo "[OK] local: cleaned up Spt containers"
    fi
  else
    if (( DRYRUN )); then
      echo "[DRY-RUN] $target: $stop_cmd"
    else
      ssh -o BatchMode=yes -o ConnectTimeout=10 "$target" "$stop_cmd"
      echo "[OK] $target: cleaned up Spt containers"
    fi
  fi
}

rc=0
if (( PARALLEL )); then
  pids=()
  for h in "${HOST_ARR[@]}"; do
    stop_on_host "$h" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then rc=1; fi
  done
else
  for h in "${HOST_ARR[@]}"; do
    if ! stop_on_host "$h"; then rc=1; fi
  done
fi

exit "$rc"

