#!/usr/bin/env bash
set -euo pipefail

# Starts the engine image of each platform with --version and fails if one doesn't start, or
# reports another version. The release workflows run it on each platform of the pushed image index,
# by digest, so it runs exactly what was pushed. Platforms other than the host's need QEMU.

usage() {
	cat <<'EOF'
Usage: check_image_starts.sh [--expect-version VERSION] --index REPOSITORY@DIGEST [--platforms LIST]
       check_image_starts.sh [--expect-version VERSION] PLATFORM=IMAGE...

--index reads the image index at REPOSITORY@DIGEST and starts the image of each platform in LIST
(comma-separated; default linux/amd64,linux/arm64) by its digest. PLATFORM=IMAGE starts IMAGE as
PLATFORM, for example linux/arm64=ghcr.io/dell/storage-performance-tool@sha256:<digest>.
--expect-version also requires the output line "Engine version: VERSION".
EOF
}

EXPECTED_VERSION=""
EXPECTED_VERSION_GIVEN=false
INDEX_REF=""
PLATFORMS="linux/amd64,linux/arm64"
IMAGES=()

while [[ $# -gt 0 ]]; do
	case "$1" in
		--expect-version|--index|--platforms)
			[[ $# -gt 1 ]] || { echo "error: $1 requires a value" >&2; exit 2; }
			case "$1" in
				--expect-version) EXPECTED_VERSION="$2"; EXPECTED_VERSION_GIVEN=true ;;
				--index) INDEX_REF="$2" ;;
				--platforms) PLATFORMS="$2" ;;
			esac
			shift
			;;
		-h|--help)
			usage
			exit 0
			;;
		*=*)
			IMAGES+=("$1")
			;;
		*)
			echo "error: unknown argument: $1" >&2
			usage >&2
			exit 2
			;;
	esac
	shift
done

if [[ "${EXPECTED_VERSION_GIVEN}" == true && -z "${EXPECTED_VERSION}" ]]; then
	echo "error: --expect-version needs a version" >&2
	exit 2
fi

if [[ -n "${INDEX_REF}" ]]; then
	if [[ "${INDEX_REF}" != *@sha256:* ]]; then
		echo "error: --index needs an image index by digest, REPOSITORY@sha256:...: ${INDEX_REF}" >&2
		exit 2
	fi
	repository="${INDEX_REF%%@*}"
	index_json="$(docker buildx imagetools inspect "${INDEX_REF}" --raw)"
	# One PLATFORM=REPOSITORY@DIGEST line per platform in the list; attestation manifests have no
	# platform of their own and are skipped.
	resolved="$(INDEX_JSON="${index_json}" python3 - "${repository}" "${PLATFORMS}" <<'PY'
import json
import os
import sys

repository, wanted = sys.argv[1], sys.argv[2].split(",")
index = json.loads(os.environ["INDEX_JSON"])
digests = {}
for entry in index.get("manifests", []):
    if (entry.get("annotations") or {}).get("vnd.docker.reference.type") == "attestation-manifest":
        continue
    platform = entry.get("platform") or {}
    digests.setdefault(f"{platform.get('os')}/{platform.get('architecture')}", []).append(entry["digest"])
failed = False
for name in wanted:
    found = digests.get(name, [])
    if len(found) != 1:
        print(f"error: the index has {len(found)} images for {name}", file=sys.stderr)
        failed = True
    else:
        print(f"{name}={repository}@{found[0]}")
sys.exit(1 if failed else 0)
PY
)"
	while IFS= read -r line; do
		IMAGES+=("${line}")
	done <<< "${resolved}"
fi

if [[ ${#IMAGES[@]} -eq 0 ]]; then
	echo "error: give --index or at least one PLATFORM=IMAGE" >&2
	usage >&2
	exit 2
fi

failures=0
for entry in "${IMAGES[@]}"; do
	platform="${entry%%=*}"
	image="${entry#*=}"
	echo "Starting ${image} as ${platform}"
	status=0
	output="$(docker run --rm --platform "${platform}" "${image}" --version 2>&1)" || status=$?
	echo "${output}"
	if [[ ${status} -ne 0 ]]; then
		echo "::error::The ${platform} image doesn't start: --version exited ${status}"
		failures=$((failures + 1))
	elif [[ -n "${EXPECTED_VERSION}" ]] && ! grep -qxF "Engine version: ${EXPECTED_VERSION}" <<< "${output}"; then
		echo "::error::The ${platform} image doesn't report Engine version: ${EXPECTED_VERSION}"
		failures=$((failures + 1))
	else
		echo "The ${platform} image starts"
	fi
done

if [[ ${failures} -ne 0 ]]; then
	exit 1
fi
