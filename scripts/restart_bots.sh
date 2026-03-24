#!/usr/bin/env bash
# Kill moonshot + trend runners, then start live/run_live.py and live/run_moonshot.py.
# Usage from repo root: chmod +x scripts/restart_bots.sh && ./scripts/restart_bots.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -x "${ROOT}/.venv/bin/python" ]]; then
  PY="${ROOT}/.venv/bin/python"
else
  PY="${PYTHON:-python3}"
fi

echo "$(date -Iseconds) stopping existing bot processes..."
pkill -f "live/run_live.py" 2>/dev/null || true
pkill -f "live/run_moonshot.py" 2>/dev/null || true
pkill -f "scripts/run_moonshot_daemon.sh" 2>/dev/null || true
sleep 2

echo "$(date -Iseconds) starting run_live.py -> live.out"
nohup "$PY" live/run_live.py >> live.out 2>&1 &
echo "  pid=$!"

echo "$(date -Iseconds) starting run_moonshot.py -> moonshot.out"
nohup "$PY" live/run_moonshot.py >> moonshot.out 2>&1 &
echo "  pid=$!"

echo "$(date -Iseconds) done. tail: tail -f live.out moonshot.out"
