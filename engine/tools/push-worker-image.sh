#!/usr/bin/env bash
set -euo pipefail

# Distribute a local Docker image to all worker nodes without using a registry.
# Uses docker save → SSH transfer → docker load for rapid iteration.
# Reads config from ./.env or environment.

# Script is in engine/tools, .env is in cli directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/cli/.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

# Configuration
IMAGE_TAG=${IMAGE_TAG:-"spt_dev"}
IMAGE_NAME=${IMAGE_NAME:-"ghcr.io/dell/storage-performance-tool"}
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

WORKER_HOSTS=${WORKER_HOSTS:-}
PRIMARY_HOST=${PRIMARY_HOST:-}

# Fall back to HOSTS if WORKER_HOSTS not set
if [ -z "${WORKER_HOSTS}" ]; then
  HOSTS=${HOSTS:-}
  if [ -n "$HOSTS" ]; then
    WORKER_HOSTS="$HOSTS"
  else
    echo "ERROR: Neither WORKER_HOSTS nor HOSTS is set in .env" >&2
    exit 1
  fi
fi

SSH_OPTS=${SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"}

# Parse optional args
while [ $# -gt 0 ]; do
  case "$1" in
    --image|-i)
      FULL_IMAGE="$2"; shift 2 ;;
    --tag|-t)
      IMAGE_TAG="$2"; 
      FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
      shift 2 ;;
    --help|-h)
      echo "Usage: $(basename "$0") [--image IMAGE:TAG] [--tag TAG]"
      echo ""
      echo "Distribute a Docker image to all worker nodes configured in cli/.env"
      echo ""
      echo "Options:"
      echo "  --image IMAGE:TAG  Full image name and tag (default: ghcr.io/dell/storage-performance-tool:spt_dev)"
      echo "  --tag TAG          Image tag only (default: spt_dev)"
      echo "  --help, -h         Show this help message"
      echo ""
      echo "Example:"
      echo "  $(basename "$0") --tag spt_dev"
      exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2 ;;
  esac
done

# Build list of all hosts (primary + workers) for display and distribution
ALL_HOSTS="$WORKER_HOSTS"
if [ -n "$PRIMARY_HOST" ]; then
  # Add primary if not already in the list
  if [[ ! ",$WORKER_HOSTS," == *",$PRIMARY_HOST,"* ]]; then
    ALL_HOSTS="${PRIMARY_HOST},${WORKER_HOSTS}"
  fi
fi

echo "Distributing Docker image: ${FULL_IMAGE}"
echo "To hosts: ${ALL_HOSTS}"
echo ""

# Check if image exists locally
if ! docker image inspect "${FULL_IMAGE}" >/dev/null 2>&1; then
  echo "ERROR: Image ${FULL_IMAGE} not found locally" >&2
  echo "Build it first with: ./gradlew :bundle:dockerBuildImage -PdockerImageTag=${IMAGE_TAG} --no-daemon" >&2
  exit 1
fi

# Get image size for progress reporting
IMAGE_SIZE=$(docker image inspect "${FULL_IMAGE}" --format='{{.Size}}' | awk '{print $1/1024/1024 " MB"}')
echo "Image size: ${IMAGE_SIZE}"
echo ""

# Create temp file for the image tar
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

IMAGE_TAR="${TEMP_DIR}/spt-image.tar"
echo "Saving image to tar file..."
docker save "${FULL_IMAGE}" -o "${IMAGE_TAR}"
TAR_SIZE=$(du -h "${IMAGE_TAR}" | cut -f1)
echo "Image tar created: ${TAR_SIZE}"
echo ""

IFS=',' read -r -a HOST_ARR <<<"$ALL_HOSTS"
failed_nodes=()
success_count=0

for target in "${HOST_ARR[@]}"; do
  target_trimmed="${target//[$'\n\t\r ']}"
  [ -z "$target_trimmed" ] && continue

  echo "--- [$target_trimmed] distributing image"
  
  # Transfer and load the image
  if cat "${IMAGE_TAR}" | ssh $SSH_OPTS "$target_trimmed" 'docker load -q'; then
    echo "✓ Successfully loaded image on $target_trimmed"
    success_count=$((success_count+1))
  else
    echo "✗ Failed to distribute image to $target_trimmed" >&2
    failed_nodes+=("$target_trimmed")
  fi
done

echo ""
echo "Distribution complete: ${success_count}/${#HOST_ARR[@]} nodes successful"

if [ ${#failed_nodes[@]} -gt 0 ]; then
  echo "Failed nodes: ${failed_nodes[*]}" >&2
  exit 1
fi

echo ""
echo "Image ${FULL_IMAGE} is now available on all worker nodes"
