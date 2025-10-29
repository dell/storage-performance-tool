#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RELEASE_DIR="${ROOT_DIR}/release"
WORKFLOW="${ROOT_DIR}/.github/workflows/release-artifacts.yaml"
PAYLOAD="${ROOT_DIR}/payloads/tag.json"
ACT_BIN="${ACT_BIN:-${HOME}/bin/act}"
ACT_IMAGE="${ACT_IMAGE:-ghcr.io/catthehacker/ubuntu:act-22.04}"

if [[ ! -x "${ACT_BIN}" ]]; then
	echo "error: act binary not found at ${ACT_BIN}. Set ACT_BIN or install act." >&2
	exit 1
fi

if [[ ! -f "${WORKFLOW}" ]]; then
	echo "error: workflow file not found at ${WORKFLOW}" >&2
	exit 1
fi

if [[ ! -f "${PAYLOAD}" ]]; then
	echo "error: payload file not found at ${PAYLOAD}" >&2
	exit 1
fi

if [[ -d "${RELEASE_DIR}" ]]; then
	echo "Removing existing ${RELEASE_DIR}"
	if ! rm -rf "${RELEASE_DIR}"; then
		echo "rm failed; retrying with sudo..."
		sudo rm -rf "${RELEASE_DIR}"
	fi
fi

mkdir -p "${RELEASE_DIR}"

set -x
"${ACT_BIN}" push --bind \
	-P "ubuntu-latest=${ACT_IMAGE}" \
	-W "${WORKFLOW}" \
	-e "${PAYLOAD}"
set +x
