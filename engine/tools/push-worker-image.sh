#!/usr/bin/env bash
set -euo pipefail

# Distribute a local Docker image to all worker nodes without using a registry.
# Uses docker save → SSH transfer → docker load for rapid iteration.
# Reads config from ./.env or environment.

# Script is in engine/tools, .env is in cli directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/cli/.env"
# shellcheck source=/dev/null
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
# SSH_OPTS is a space-separated option list; split it into words once.
read -r -a SSH_OPT_ARR <<<"$SSH_OPTS"

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

# The save-side check reads the tar's manifest.json with python3.
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required to check the saved image" >&2
  exit 1
fi

# Check if image exists locally
if ! docker image inspect "${FULL_IMAGE}" >/dev/null 2>&1; then
  echo "ERROR: Image ${FULL_IMAGE} not found locally" >&2
  echo "Build it first with: ./gradlew :bundle:dockerBuildImage -PdockerImageTag=${IMAGE_TAG} --no-daemon" >&2
  exit 1
fi

# Platform and layer identity of an image, as `docker image inspect` reports it.
# The same format runs locally and on each worker, so the two can be compared.
IDENTITY_FORMAT='{{.Os}}/{{.Architecture}} {{json .RootFS.Layers}}'

# Get image size for progress reporting
IMAGE_SIZE=$(docker image inspect "${FULL_IMAGE}" --format='{{.Size}}' | awk '{print $1/1024/1024 " MiB"}')
LOCAL_IDENTITY=$(docker image inspect "${FULL_IMAGE}" --format="${IDENTITY_FORMAT}")
echo "Image size: ${IMAGE_SIZE}"
echo ""

# check_saved_tar TAR IMAGE: succeed when the tar's manifest.json lists a
# non-empty Layers list for IMAGE and the tar holds its Config and every layer.
# A manifest-only save (no config or layer blobs) loads with exit 0 but cannot
# run, so exit codes alone don't catch it. Prints the reason on failure.
check_saved_tar() {
  local tar_file="$1" image="$2" listing
  if ! listing=$(tar -tf "${tar_file}" 2>&1); then
    echo "cannot list the tar: ${listing}"
    return 1
  fi
  if ! printf '%s\n' "${listing}" | sed 's|^\./||' | grep -qx 'manifest.json'; then
    echo "the tar holds no manifest.json"
    return 1
  fi
  tar -xOf "${tar_file}" manifest.json 2>/dev/null | python3 -c '
import json, sys

image = sys.argv[1]
present = {line[2:] if line.startswith("./") else line for line in sys.argv[2].splitlines()}
try:
    manifest = json.load(sys.stdin)
except ValueError as err:
    sys.exit("manifest.json is not valid JSON: %s" % err)
if not isinstance(manifest, list) or not manifest:
    sys.exit("manifest.json lists no image")
entries = [e for e in manifest if isinstance(e, dict) and image in (e.get("RepoTags") or [])]
if not entries:
    if len(manifest) != 1 or not isinstance(manifest[0], dict):
        sys.exit("manifest.json has no entry for %s" % image)
    entries = manifest
entry = entries[0]
layers = entry.get("Layers") or []
if not layers:
    sys.exit("manifest.json lists no layers")
missing = [p for p in [entry.get("Config") or ""] + layers if p not in present]
if missing:
    sys.exit("%d of the %d config and layer files manifest.json names are not in the tar (first: %s)"
             % (len(missing), len(layers) + 1, missing[0] or "<no Config>"))
' "${image}" "${listing}" 2>&1
}

# Create temp file for the image tar
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

IMAGE_TAR="${TEMP_DIR}/spt-image.tar"
echo "Saving image to tar file..."
docker save "${FULL_IMAGE}" -o "${IMAGE_TAR}"
TAR_SIZE=$(du -h "${IMAGE_TAR}" | cut -f1)
if ! tar_problem=$(check_saved_tar "${IMAGE_TAR}" "${FULL_IMAGE}"); then
  echo "ERROR: The saved image ${FULL_IMAGE} has no layer content: ${tar_problem}" >&2
  echo "Image tar: ${TAR_SIZE}; local image size: ${IMAGE_SIZE}" >&2
  echo "The local image store may be missing content shared with the base image." >&2
  echo "Re-pull the base image or rebuild the image, then run this script again." >&2
  exit 1
fi
echo "Image tar created: ${TAR_SIZE} (local image size: ${IMAGE_SIZE})"
echo ""

IFS=',' read -r -a HOST_ARR <<<"$ALL_HOSTS"
failed_nodes=()
success_count=0

for target in "${HOST_ARR[@]}"; do
  target_trimmed="${target//[$'\n\t\r ']}"
  [ -z "$target_trimmed" ] && continue

  echo "--- [$target_trimmed] distributing image"
  
  # Transfer and load the image, then check that the worker's copy has the
  # local image's platform and layers: a load can exit 0 without them.
  if ! ssh "${SSH_OPT_ARR[@]}" "$target_trimmed" 'docker load -q' <"${IMAGE_TAR}"; then
    echo "✗ Failed to distribute image to $target_trimmed" >&2
    failed_nodes+=("$target_trimmed")
    continue
  fi
  remote_cmd="docker image inspect --format=$(printf '%q' "${IDENTITY_FORMAT}") $(printf '%q' "${FULL_IMAGE}")"
  # remote_cmd is built and quoted here on purpose, for the remote shell.
  # shellcheck disable=SC2029
  if ! remote_identity=$(ssh "${SSH_OPT_ARR[@]}" "$target_trimmed" "${remote_cmd}" </dev/null); then
    echo "✗ Loaded image on $target_trimmed but could not inspect it there" >&2
    failed_nodes+=("$target_trimmed")
  elif [ "${remote_identity}" != "${LOCAL_IDENTITY}" ]; then
    echo "✗ Image loaded on $target_trimmed does not match the local image" >&2
    echo "  local:  ${LOCAL_IDENTITY}" >&2
    echo "  remote: ${remote_identity}" >&2
    failed_nodes+=("$target_trimmed")
  else
    echo "✓ Successfully loaded image on $target_trimmed"
    success_count=$((success_count+1))
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
