#!/usr/bin/env bash
# Build a custom act runner image with GitHub CLI and execute the CI workflow locally.

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORKFLOW_PATH="${WORKFLOW_PATH:-.github/workflows/ci.yml}"
SECRETS_FILE="${ACT_SECRETS:-$HOME/.secrets}"
BASE_IMAGE="${ACT_BASE_IMAGE:-ghcr.io/catthehacker/ubuntu:act-latest}"
IMAGE_TAG="${ACT_IMAGE_TAG:-act-gh}" 

if [[ ! -f "${ROOT_DIR}/${WORKFLOW_PATH}" ]]; then
  echo "Workflow file not found: ${ROOT_DIR}/${WORKFLOW_PATH}" >&2
  exit 1
fi

if [[ ! -f "${SECRETS_FILE}" ]]; then
  echo "Secrets file not found: ${SECRETS_FILE}" >&2
  exit 1
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

cat >"${TMP_DIR}/Dockerfile" <<"EOF"
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      gnupg \
      ca-certificates \
      yamllint && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && \
    chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y --no-install-recommends gh && \
    apt-get purge --auto-remove -y gnupg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

EOF

docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t "${IMAGE_TAG}" "${TMP_DIR}"

pushd "${ROOT_DIR}" >/dev/null

act pull_request \
  -P "ubuntu-latest=${IMAGE_TAG}" \
  --pull=false \
  --var CODE_COVERAGE_TARGET=65 \
  --var PACKAGE_SKIP_LIST=github.com/dell/storage-performance-tool/cli/internal/buildinfo,github.com/dell/storage-performance-tool/cli/internal/constants,github.com/dell/storage-performance-tool/cli/tools/coverage/covsum \
  --secret-file "${SECRETS_FILE}" \
  --workflows "${WORKFLOW_PATH}" \
  --rm "$@"

popd >/dev/null
