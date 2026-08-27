#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$ENGINE_ROOT/.." && pwd)"
CLI_ROOT="$REPO_ROOT/cli"
JAR_PATH="${SPT_BUILD_INFO_JAR:-$ENGINE_ROOT/bundle/build/dist/spt.jar}"
SKIP_BUILD=false

usage() {
	cat <<'EOF'
Usage: engine/tools/testbuildinfo.sh [--skip-build]

Builds and verifies the packaged engine, starts a local packaged node, and
requires the real Go Engine Build Information collector canary to pass.

Options:
  --skip-build  Reuse SPT_BUILD_INFO_JAR or bundle/build/dist/spt.jar.
  -h, --help    Show this help.

Environment:
  SPT_BUILD_INFO_JAR  Packaged engine JAR path.
  JAVA                Java executable (default: java from PATH).
EOF
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

for arg in "$@"; do
	case "$arg" in
		--skip-build) SKIP_BUILD=true ;;
		-h|--help) usage; exit 0 ;;
		*) die "unknown argument: $arg" ;;
	esac
done

for command_name in curl go python3 unzip; do
	command -v "$command_name" >/dev/null 2>&1 || die "required command not found: $command_name"
done
JAVA_BIN="${JAVA:-java}"
command -v "$JAVA_BIN" >/dev/null 2>&1 || die "Java executable not found: $JAVA_BIN"

if [[ "$SKIP_BUILD" != "true" ]]; then
	(
		cd "$ENGINE_ROOT"
		./gradlew :bundle:verifyEngineBuildInfoCanary --no-daemon
	)
fi

[[ -f "$JAR_PATH" ]] || die "packaged engine JAR not found: $JAR_PATH"
EXPECTED_VERSION="$(unzip -p "$JAR_PATH" META-INF/spt-build-info.properties \
	| awk -F '=' '$1 == "version" {sub(/^[^=]*=/, ""); gsub(/\r/, ""); print; exit}')"
[[ -n "$EXPECTED_VERSION" ]] || die "version is missing from packaged Engine Build Information in $JAR_PATH"

reserve_port() {
	python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

API_PORT="$(reserve_port)"
NODE_PORT="$(reserve_port)"
while [[ "$NODE_PORT" == "$API_PORT" ]]; do
	NODE_PORT="$(reserve_port)"
done

WORK_DIR="$(mktemp -d /tmp/spt-build-info-canary.XXXXXX)"
NODE_STDOUT="$WORK_DIR/node.stdout.log"
NODE_STDERR="$WORK_DIR/node.stderr.log"
NODE_PID=""

cleanup() {
	status=$?
	trap - EXIT INT TERM
	if [[ -n "$NODE_PID" ]] && kill -0 "$NODE_PID" 2>/dev/null; then
		curl --silent --output /dev/null --max-time 2 --request POST \
			"http://127.0.0.1:$API_PORT/shutdown" || true
		for _ in $(seq 1 100); do
			kill -0 "$NODE_PID" 2>/dev/null || break
			sleep 0.1
		done
		if kill -0 "$NODE_PID" 2>/dev/null; then
			kill "$NODE_PID" 2>/dev/null || true
		fi
		wait "$NODE_PID" 2>/dev/null || true
	fi
	if [[ $status -ne 0 ]]; then
		printf '%s\n' '--- packaged engine stdout ---' >&2
		tail -n 80 "$NODE_STDOUT" >&2 || true
		printf '%s\n' '--- packaged engine stderr ---' >&2
		tail -n 80 "$NODE_STDERR" >&2 || true
	fi
	rm -rf "$WORK_DIR"
	exit "$status"
}
trap cleanup EXIT INT TERM

mkdir -p "$WORK_DIR/home" "$WORK_DIR/log"
(
	cd "$(dirname "$JAR_PATH")"
	SPT_HOME="$WORK_DIR/home" SPT_LOG_DIR="$WORK_DIR/log" \
		"$JAVA_BIN" -jar "$JAR_PATH" \
		--run-node \
		--run-port="$API_PORT" \
		--load-step-node-port="$NODE_PORT" \
		--api-linger-sec=0 \
		--storage-driver-type=netty-mock \
		>"$NODE_STDOUT" 2>"$NODE_STDERR"
) &
NODE_PID=$!

BASE_URL="http://127.0.0.1:$API_PORT"
ready=false
for _ in $(seq 1 300); do
	if ! kill -0 "$NODE_PID" 2>/dev/null; then
		die "packaged engine exited before /version became ready"
	fi
	if curl --fail --silent --max-time 1 "$BASE_URL/version" >/dev/null; then
		ready=true
		break
	fi
	sleep 0.1
done
[[ "$ready" == "true" ]] || die "timed out waiting for packaged engine /version"

printf 'Verifying packaged Go collector against engine version %s\n' "$EXPECTED_VERSION"
set +e
CANARY_OUTPUT="$(
	cd "$CLI_ROOT"
	SPT_TEST_PACKAGED_ENGINE_URL="$BASE_URL" \
	SPT_TEST_PACKAGED_ENGINE_VERSION="$EXPECTED_VERSION" \
		go test -json -tags packaged_engine_canary -count=1 \
		-run '^TestPackagedEngineSemanticVersionCanary$' ./internal/engineinfo
)"
CANARY_STATUS=$?
set -e
printf '%s\n' "$CANARY_OUTPUT"
[[ $CANARY_STATUS -eq 0 ]] || die "packaged Go collector canary failed"
awk '/"Action":"pass"/ && /"Test":"TestPackagedEngineSemanticVersionCanary"/ { passed = 1 } END { exit !passed }' \
	<<<"$CANARY_OUTPUT" || die "packaged Go collector canary did not execute to PASS"

SHUTDOWN_STATUS="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 2 \
	--request POST "$BASE_URL/shutdown")"
[[ "$SHUTDOWN_STATUS" == "202" ]] || die "packaged engine shutdown returned HTTP $SHUTDOWN_STATUS"
for _ in $(seq 1 200); do
	if ! kill -0 "$NODE_PID" 2>/dev/null; then
		break
	fi
	sleep 0.1
done
kill -0 "$NODE_PID" 2>/dev/null && die "packaged engine did not stop after shutdown"
set +e
wait "$NODE_PID"
NODE_STATUS=$?
set -e
NODE_PID=""
[[ $NODE_STATUS -eq 0 ]] || die "packaged engine exited with status $NODE_STATUS"

printf 'Packaged Engine Build Information qualification passed.\n'
