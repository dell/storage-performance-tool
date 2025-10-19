#!/usr/bin/env bash
#
# Polls SPT engine metrics endpoints on a loop and writes JSON payloads to a log file.
# Useful for debugging entry-node metric drops during multi-step runs.
#
# Example:
#   tools/monitor-entry-metrics.sh --host 10.247.70.241 --interval 1 \
#       --output /tmp/entry-metrics.log --include-cluster
#
set -euo pipefail

HOST="localhost"
PORT=9999
INTERVAL=1
OUTPUT=""
INCLUDE_CLUSTER=0
INCLUDE_VERBOSE=0
ONESHOT=0

usage() {
	echo "Usage: $(basename "$0") [options]" >&2
	echo "" >&2
	echo "Options:" >&2
	echo "  --host <addr>        Entry node hostname or IP (default: localhost)" >&2
	echo "  --port <port>        Metrics port (default: 9999)" >&2
	echo "  --interval <sec>     Poll interval in seconds (default: 1)" >&2
	echo "  --output <path>      Log file path (required)" >&2
	echo "  --include-cluster    Also capture /metrics/fleet/json" >&2
	echo "  --include-verbose    Capture /metrics/json?verbose=true in addition to base" >&2
	echo "  --once               Perform a single poll then exit" >&2
	echo "  -h, --help           Show this help" >&2
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--host)
			HOST="$2"; shift 2 ;;
		--port)
			PORT="$2"; shift 2 ;;
		--interval)
			INTERVAL="$2"; shift 2 ;;
		--output)
			OUTPUT="$2"; shift 2 ;;
		--include-cluster)
			INCLUDE_CLUSTER=1; shift ;;
		--include-verbose)
			INCLUDE_VERBOSE=1; shift ;;
		--once)
			ONESHOT=1; shift ;;
		-h|--help)
			usage; exit 0 ;;
		*)
			echo "Unknown option: $1" >&2
			usage >&2
			exit 1 ;;
	esac
done

if [[ -z "$OUTPUT" ]]; then
	echo "Error: --output is required" >&2
	usage >&2
	exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
	echo "Error: curl is required" >&2
	exit 1
fi

log_header() {
	local now
	now=$(date --iso-8601=seconds)
	echo "# monitor-entry-metrics.sh" >>"$OUTPUT"
	echo "# started_at=$now" >>"$OUTPUT"
	echo "# host=$HOST port=$PORT interval=$INTERVAL" >>"$OUTPUT"
	echo "# include_cluster=$INCLUDE_CLUSTER include_verbose=$INCLUDE_VERBOSE" >>"$OUTPUT"
	echo "#" >>"$OUTPUT"
}

log_request() {
	local label="$1"
	local url="$2"
	local ts
	local response
	local body
	local http_code

	ts=$(date --iso-8601=seconds)
	response=$(curl -sS --max-time "$((INTERVAL*2))" -w 'HTTP_STATUS:%{http_code}' "$url" 2>&1) || true
	body=${response%HTTP_STATUS:*}
	http_code=${response##*HTTP_STATUS:}

	echo "[$ts] $label status=$http_code" >>"$OUTPUT"
	echo "$body" >>"$OUTPUT"
	echo "" >>"$OUTPUT"

	printf '[%s] %-18s status=%s\n' "$ts" "$label" "$http_code"
}

trap 'echo "Stopping monitor" >&2; exit 0' INT TERM

log_header

while true; do
	log_request "metrics/json" "http://$HOST:$PORT/metrics/json"
	if [[ $INCLUDE_VERBOSE -eq 1 ]]; then
		log_request "metrics/json?verbose" "http://$HOST:$PORT/metrics/json?verbose=true"
	fi
	if [[ $INCLUDE_CLUSTER -eq 1 ]]; then
		log_request "metrics/fleet/json" "http://$HOST:$PORT/metrics/fleet/json"
	fi
	if [[ $ONESHOT -eq 1 ]]; then
		break
	fi
	sleep "$INTERVAL"
done
