#!/usr/bin/env bash
set -euo pipefail

# Validate that version metadata is synchronized across the repository.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VERSION_FILE="${ROOT_DIR}/VERSION"

usage() {
	cat <<'EOF'
Usage: check_version_sync.sh [--expected VERSION] [--allow-snapshot]

Ensures VERSION is set and referenced consistently across build tooling.
Fails fast if mismatches are detected.
EOF
}

EXPECTED_VERSION=""
ALLOW_SNAPSHOT="false"

while [[ $# -gt 0 ]]; do
	case "$1" in
		--expected)
			shift
			[[ $# -gt 0 ]] || { echo "error: --expected requires a value" >&2; exit 2; }
			EXPECTED_VERSION="$1"
			;;
		--allow-snapshot)
			ALLOW_SNAPSHOT="true"
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "error: unknown argument: $1" >&2
			usage >&2
			exit 2
			;;
	esac
	shift
done

errors=()

if [[ ! -f "${VERSION_FILE}" ]]; then
	errors+=("VERSION file missing at ${VERSION_FILE}")
else
	REPO_VERSION="$(<"${VERSION_FILE}")"
	REPO_VERSION="${REPO_VERSION//$'\n'/}"
	if [[ -z "${REPO_VERSION}" ]]; then
		errors+=("VERSION file is empty")
	fi
	if [[ "${ALLOW_SNAPSHOT}" != "true" && "${REPO_VERSION}" == *"-SNAPSHOT" ]]; then
		errors+=("VERSION ${REPO_VERSION} still marked as SNAPSHOT; releases must use final SemVer")
	fi
	if [[ -n "${EXPECTED_VERSION}" && "${EXPECTED_VERSION}" != "${REPO_VERSION}" ]]; then
		errors+=("VERSION mismatch: expected '${EXPECTED_VERSION}', found '${REPO_VERSION}'")
	fi
fi

check_contains() {
	local file="$1"
	local pattern="$2"
	if [[ ! -f "${ROOT_DIR}/${file}" ]]; then
		errors+=("Expected file ${file} not found")
		return
	fi
	if ! grep -qF "${pattern}" "${ROOT_DIR}/${file}"; then
		errors+=("Expected to find '${pattern}' in ${file}")
	fi
}

check_contains "cli/Makefile" "VERSION_FILE ?= ../VERSION"
check_contains "engine/Makefile" "VERSION_FILE := ../VERSION"
check_contains "engine/build.gradle" "file('../VERSION').text.trim()"
check_contains "engine/bundle/build.gradle" "rootProject.ext.SPT_VERSION"
check_contains "engine/bundle/Dockerfile" "ARG SPT_VERSION="
check_contains "engine/bundle/Dockerfile" "LABEL version=\"\${SPT_VERSION}\""

# Check that defaults.yaml contains the correct version
# This is critical because spt-base/build.gradle reads version from defaults.yaml
check_version_in_yaml() {
	local file="$1"
	if [[ ! -f "${ROOT_DIR}/${file}" ]]; then
		errors+=("Expected file ${file} not found")
		return
	fi
	local yaml_version
	yaml_version=$(grep -E '^\s*version:\s*' "${ROOT_DIR}/${file}" | tail -1 | sed 's/.*version:\s*//' | tr -d '[:space:]')
	if [[ -n "${REPO_VERSION}" && "${yaml_version}" != "${REPO_VERSION}" ]]; then
		errors+=("Version in ${file} is '${yaml_version}' but VERSION file has '${REPO_VERSION}'")
	fi
}

check_version_in_yaml "engine/core/spt-base/src/main/resources/config/defaults.yaml"

if [[ "${#errors[@]}" -gt 0 ]]; then
	printf 'Version sync check failed:\n' >&2
	for err in "${errors[@]}"; do
		printf '  - %s\n' "${err}" >&2
	done
	exit 1
fi

printf 'Version sync check passed. VERSION=%s\n' "${REPO_VERSION:-unknown}"
