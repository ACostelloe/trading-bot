"""Backtest helper outputs (no exchange)."""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from backtests.run_backtest import build_monthly_stats, build_symbol_stats


def test_build_symbol_stats_empty() -> None:
    df = pd.DataFrame()
    out = build_symbol_stats(df)
    assert out.empty


def test_build_symbol_stats_groups() -> None:
    df = pd.DataFrame(
        {
            "symbol": ["A", "A", "B"],
            "net_pnl": [10.0, -5.0, 3.0],
        }
    )
    out = build_symbol_stats(df)
    assert len(out) == 2
    a = out[out["symbol"] == "A"].iloc[0]
    assert a["trades"] == 2
    assert a["total_net_pnl"] == pytest.approx(5.0)


def test_build_monthly_stats() -> None:
    df = pd.DataFrame(
        {
            "net_pnl": [1.0, -2.0],
            "exit_time": pd.to_datetime(["2025-01-15", "2025-01-20"], utc=True),
        }
    )
    out = build_monthly_stats(df)
    assert len(out) == 1
    assert out["month"].iloc[0] == "2025-01"
    assert out["trades"].iloc[0] == 2
    assert out["net_pnl"].iloc[0] == pytest.approx(-1.0)
