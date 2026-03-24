#!/usr/bin/env bash
# Restart moonshot runner if it exits. Run from repo root:
#   chmod +x scripts/run_moonshot_daemon.sh && ./scripts/run_moonshot_daemon.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
while true; do
  echo "$(date -Iseconds) starting live/run_moonshot.py"
  python live/run_moonshot.py || true
  echo "$(date -Iseconds) exited, restarting in 15s"
  sleep 15
done
