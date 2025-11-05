#!/usr/bin/env bash
set -euo pipefail

# Turnkey: stop/remove SPT worker containers on remote hosts.
# Usage:
#   nodes-down.sh           # stop workers only
#   nodes-down.sh --all     # stop both PRIMARY_HOST and WORKER_HOSTS

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# New convention: PRIMARY_HOST + WORKER_HOSTS. Fall back to HOSTS if unset.
PRIMARY_HOST=${PRIMARY_HOST:-}
WORKER_HOSTS=${WORKER_HOSTS:-}
if [ -z "${WORKER_HOSTS}" ]; then
  HOSTS=${HOSTS:-"root@spt-test-1.cec.delllabs.net,root@spt-test-2.cec.delllabs.net,root@spt-test-3.cec.delllabs.net,root@spt-test-4.cec.delllabs.net"}
  WORKER_HOSTS="$HOSTS"
fi
SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}
CONTAINER_NAME=${CONTAINER_NAME:-spt-node}

# Parse optional args
ALL=false
RETRIES=${RETRIES:-3}
RETRY_SLEEP=${RETRY_SLEEP:-1}
PURGE_IMAGES=${PURGE_IMAGES:-false}

while [ $# -gt 0 ]; do
  case "$1" in
    --all|-a)
      ALL=true; shift ;;
    --purge-images|--prune-images)
      PURGE_IMAGES=true; shift ;;
    --retry|-r)
      RETRIES="${2:-}"; shift 2 ;;
    --help|-h)
      cat <<'USAGE'
Usage: nodes-down.sh [options]
  --all, -a            Include PRIMARY_HOST in addition to WORKER_HOSTS.
  --purge-images       Remove cached SPT images so the next run forces a fresh pull.
  --retry, -r N        Retry failed hosts up to N times (default: 3).
  --help, -h           Show this help message.
Environment overrides:
  PRIMARY_HOST, WORKER_HOSTS, HOSTS, SSH_OPTS, SPT_IMAGE,
  RETRIES, RETRY_SLEEP, PURGE_IMAGES.
USAGE
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Validate retries
if ! [[ "$RETRIES" =~ ^[0-9]+$ ]] || [ "$RETRIES" -lt 1 ]; then
  echo "ERROR: --retry requires a positive integer (got: $RETRIES)" >&2
  exit 2
fi

# Safety: ensure PRIMARY_HOST is not in WORKER_HOSTS
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

targets_csv="$WORKER_HOSTS"
if $ALL; then
  if [ -n "$PRIMARY_HOST" ]; then
    if [ -n "$targets_csv" ]; then
      targets_csv="$PRIMARY_HOST,$targets_csv"
    else
      targets_csv="$PRIMARY_HOST"
    fi
  fi
fi

IFS=',' read -r -a HOST_ARR <<<"$targets_csv"

WORKER_CONTAINER_NAME=${WORKER_CONTAINER_NAME:-spt-worker}
ENTRY_CONTAINER_NAME=${ENTRY_CONTAINER_NAME:-spt-primary}

# Determine which container images should be considered for cleanup.
declare -a _image_candidates=()
if [ -n "${SPT_IMAGE:-}" ]; then
  _image_candidates+=("${SPT_IMAGE}")
fi
SPT_IMAGE_CANDIDATES="${_image_candidates[*]}"

echo "Comprehensive cleanup on ${#HOST_ARR[@]} host(s)${ALL:+ (including primary)}..."
failed_nodes=()
for target in "${HOST_ARR[@]}"; do
  target_trimmed="${target//[$'\n\t\r ']}"
  [ -z "$target_trimmed" ] && continue
  echo "--- [$target_trimmed] removing all spt containers"
  attempt=1
  success=false
  while [ "$attempt" -le "$RETRIES" ]; do
    if ssh $SSH_OPTS "$target_trimmed" \
      SPT_IMAGE="${SPT_IMAGE:-}" \
      SPT_IMAGE_CANDIDATES="${SPT_IMAGE_CANDIDATES:-}" \
      PURGE_IMAGES="${PURGE_IMAGES}" \
      bash -s <<'EOS'
set -euo pipefail
imgs_raw="${SPT_IMAGE_CANDIDATES:-${SPT_IMAGE:-}}"
declare -a imgs=()
if [ -n "$imgs_raw" ]; then
  # shellcheck disable=SC2086 # word-splitting is intentional to separate images
  for candidate in $imgs_raw; do
    [ -n "$candidate" ] && imgs+=("$candidate")
  done
fi
# Remove known names first
for name in spt-worker spt-primary spt-node; do
  docker rm -f "$name" >/dev/null 2>&1 || true
done
# Remove any containers tagged as managed by SPT
ids_by_label=$(docker ps -a --filter "label=spt.managed=true" -q || true)
if [ -n "$ids_by_label" ]; then docker rm -f $ids_by_label >/dev/null 2>&1 || true; fi
# Remove any containers with names starting with "spt" (strict)
ids_by_name=$(docker ps -a --format '{{.ID}} {{.Names}}' | awk '$2 ~ /^spt(-|$)/ {print $1}')
if [ -n "$ids_by_name" ]; then docker rm -f $ids_by_name >/dev/null 2>&1 || true; fi
# Remove any containers created from the Spt image
if [ ${#imgs[@]} -gt 0 ]; then
  for img in "${imgs[@]}"; do
    ids_by_img=$(docker ps -a --filter "ancestor=$img" -q || true)
    if [ -n "$ids_by_img" ]; then docker rm -f $ids_by_img >/dev/null 2>&1 || true; fi
  done
fi
# Verify clean: no spt-named or image-based containers remain
left_names=$(docker ps -a --format '{{.Names}}' | awk '/^spt(-|$)/ {print}')
dangling=()
if [ -n "$left_names" ]; then
  while IFS= read -r name; do
    [ -n "$name" ] && dangling+=("$name")
  done <<<"$left_names"
fi
label_names=$(docker ps -a --filter "label=spt.managed=true" --format '{{.Names}}' || true)
if [ -n "$label_names" ]; then
  while IFS= read -r name; do
    [ -n "$name" ] && dangling+=("$name")
  done <<<"$label_names"
fi
if [ ${#imgs[@]} -gt 0 ]; then
  for img in "${imgs[@]}"; do
    names_by_img=$(docker ps -a --filter "ancestor=$img" --format '{{.Names}}' || true)
    if [ -n "$names_by_img" ]; then
      while IFS= read -r name; do
        [ -n "$name" ] && dangling+=("$name")
      done <<<"$names_by_img"
    fi
  done
fi
if [ ${#dangling[@]} -gt 0 ]; then
  echo "ERROR: dangling spt containers remain: ${dangling[*]}" >&2
  exit 3
fi
purged=false
if [ "${PURGE_IMAGES:-false}" = "true" ]; then
  declared_imgs=("${imgs[@]}")
  if [ ${#declared_imgs[@]} -eq 0 ]; then
    # Fallback: gather unique repositories that contain "spt"
    while IFS= read -r repo; do
      [ -n "$repo" ] && declared_imgs+=("$repo")
    done < <(docker images --format '{{.Repository}}:{{.Tag}}' | grep -E 'spt' || true)
  fi
  if [ ${#declared_imgs[@]} -gt 0 ]; then
    # Deduplicate while preserving order
    declare -A seen=()
    for candidate in "${declared_imgs[@]}"; do
      [ -z "$candidate" ] && continue
      if [ -n "${seen[$candidate]:-}" ]; then
        continue
      fi
      seen[$candidate]=1
      if docker image inspect "$candidate" >/dev/null 2>&1; then
        docker image rm -f "$candidate" >/dev/null 2>&1 || true
        purged=true
      else
        repo="${candidate%%:*}"
        if [ -n "$repo" ]; then
          to_remove=$(docker images "$repo" --format '{{.Repository}}:{{.Tag}}' | tr '\n' ' ' || true)
          if [ -n "$to_remove" ]; then
            # shellcheck disable=SC2086
            docker image rm -f $to_remove >/dev/null 2>&1 || true
            purged=true
          fi
        fi
      fi
    done
  fi
  # Remove dangling layers created during cleanup to reclaim space.
  docker image prune -f >/dev/null 2>&1 || true
fi
if [ "$purged" = "true" ]; then
  echo "clean (images purged)"
else
  echo "clean"
fi
EOS
    then
      success=true
      break
    else
      echo "...retry $attempt/$RETRIES failed on $target_trimmed; retrying in ${RETRY_SLEEP}s" >&2
      sleep "$RETRY_SLEEP"
      attempt=$((attempt+1))
    fi
  done
  if [ "$success" != true ]; then
    failed_nodes+=("$target_trimmed")
  fi
done

if [ ${#failed_nodes[@]} -gt 0 ]; then
  echo "One or more nodes still have dangling containers: ${failed_nodes[*]}" >&2
  exit 2
fi

echo "Cleanup complete on all targets."
