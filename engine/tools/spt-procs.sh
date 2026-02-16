#!/usr/bin/env bash
#
# List or kill leftover SPT java processes.
#
# Usage:
#   ./tools/spt-procs.sh          # list SPT java processes
#   ./tools/spt-procs.sh -k       # kill them (SIGTERM)
#   ./tools/spt-procs.sh -9       # kill them (SIGKILL)
#
set -euo pipefail

SIGNAL=""
case "${1:-}" in
  -k|--kill)  SIGNAL=TERM ;;
  -9|--force) SIGNAL=KILL ;;
  -h|--help)
    echo "Usage: $0 [-k|--kill] [-9|--force]"
    echo "  (no args)   List SPT java processes"
    echo "  -k, --kill  Send SIGTERM"
    echo "  -9, --force Send SIGKILL"
    exit 0
    ;;
  "") ;;
  *)  echo "Unknown option: $1" >&2; exit 1 ;;
esac

found=0
while IFS= read -r pid; do
  cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null) || continue
  # Match java processes running spt.jar
  [[ "$cmdline" == *spt.jar* ]] || continue
  found=1
  if [[ -z "$SIGNAL" ]]; then
    elapsed=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ') || elapsed="?"
    printf "PID %-7s  age %ss  %s\n" "$pid" "$elapsed" "$cmdline"
  else
    echo "Sending SIG${SIGNAL} to PID $pid"
    kill -"$SIGNAL" "$pid" 2>/dev/null || echo "  failed (already gone?)"
  fi
done < <(pgrep -x java 2>/dev/null || true)

if [[ $found -eq 0 ]]; then
  echo "No SPT java processes found."
fi
