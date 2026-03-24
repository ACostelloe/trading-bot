from __future__ import annotations

import subprocess
import sys


COMMANDS = [
    [sys.executable, "backtests/run_walk_forward.py"],
    [sys.executable, "backtests/analyze_walk_forward.py"],
    [sys.executable, "backtests/select_approved_parameters.py"],
    [sys.executable, "backtests/run_backtest.py"],
]


def main() -> None:
    for cmd in COMMANDS:
        print(f"\nRunning: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise SystemExit(result.returncode)

    print("\nResearch pipeline complete.")
    print("Approved parameters have been generated and backtest rerun with them.")


if __name__ == "__main__":
    main()
