#!/usr/bin/env bash
set -euo pipefail

# Gracefully shut down a local Spt node via the REST API.
# Sends POST /shutdown and waits for the node to stop.

PORT="${PORT:-9999}"

log() { printf "[%s] %s\n" "$(date -u +%H:%M:%S)" "$*"; }

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<EOF
Usage: PORT=9999 tools/nodestop.local.sh

Stops the active run on a local node via DELETE /run.
Returns non-zero if no active run is found.
EOF
  exit 0
fi

log "Requesting node shutdown @ http://127.0.0.1:$PORT/shutdown ..."
if ! curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/status" | grep -q '^200$'; then
  log "Node is not responding on /status (is it running?)"
  exit 2
fi

resp=$(curl -sS -D - -o /dev/null -X POST "http://127.0.0.1:$PORT/shutdown") || true
code=$(printf "%s" "$resp" | awk 'NR==1{print $2}')
if [[ "$code" != "202" ]]; then
  log "Unexpected response code: $code"; echo "$resp" | sed 's/^/  /'
  exit 1
fi

log "Shutdown accepted. Waiting for node to stop..."
for i in $(seq 1 30); do
  if ! curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/status" | grep -q '^200$'; then
    log "Node stopped."
    exit 0
  fi
  sleep 1
done
log "WARN: Node still responding after 30s; it may still be lingering (api.linger.sec)."
exit 0
