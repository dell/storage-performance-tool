#!/usr/bin/env bash
set -euo pipefail

# Verify passwordless SSH + Docker availability on remote hosts via spt.
# Reads defaults from repo-root .env (copy .env.example).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
[ -f "$ROOT_DIR/.env" ] && source "$ROOT_DIR/.env"

# Inputs (env or CLI):
#   HOSTS      - CSV of user@host targets
#   MIN_HOSTS  - Minimum number of hosts required to pass

HOSTS=${HOSTS:-"root@spt-test-1.cec.delllabs.net,root@spt-test-2.cec.delllabs.net,root@spt-test-3.cec.delllabs.net,root@spt-test-4.cec.delllabs.net"}
MIN_HOSTS=${MIN_HOSTS:-2}

# Simple arg parsing
while [ $# -gt 0 ]; do
  case "$1" in
    --hosts) HOSTS="${2:-}"; shift 2 ;;
    --min-hosts) MIN_HOSTS="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--hosts user@h1,user@h2,…] [--min-hosts N]"; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

echo "=== Testing Multi-Node Infrastructure Verification ==="
echo "Hosts: $HOSTS"
echo "Min hosts to pass: $MIN_HOSTS"
echo

"$ROOT_DIR"/spt --debug verify \
  --test-hosts "$HOSTS" \
  --min-hosts "$MIN_HOSTS"

echo
echo "=== Verification Complete ==="
echo
