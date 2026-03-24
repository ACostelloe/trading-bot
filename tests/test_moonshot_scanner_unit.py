from __future__ import annotations

import os
import tempfile

from bot.moonshot_scanner import (
    ScannerConfig,
    MoonshotScanner,
    _spread_pct,
    load_scan_state,
    save_scan_state,
)


def test_spread_pct_mid() -> None:
    assert abs(_spread_pct(100.0, 100.5) - (0.5 / 100.25 * 100.0)) < 1e-6


def test_blacklist_bases() -> None:
    cfg = ScannerConfig(symbol_blacklist_bases=("DOGE",))
    sc = MoonshotScanner(cfg)
    assert sc._blacklisted("DOGEUSDC", "DOGE")
    assert not sc._blacklisted("SHIBUSDC", "SHIB")


def test_persistence_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "state.json")
        save_scan_state(
            path,
            {"version": 1, "ranks": {"AAAUSDC": {"rank": 1, "score": 99.0}}},
        )
        assert load_scan_state(path)["ranks"]["AAAUSDC"]["rank"] == 1


def test_rank_delta_sign() -> None:
    """prior rank 4, now rank 1 -> rank_delta = 3 (moved up the list)."""
    cfg = ScannerConfig(persist_path="", top_n=10)
    sc = MoonshotScanner(cfg)
    top = [{"symbol": "XUSDC", "moonshot_score": 50.0}]
    prior = {"ranks": {"XUSDC": {"rank": 4, "score": 40.0}}}
    sc._merge_prior_ranks(top, prior)
    assert top[0]["rank"] == 1
    assert top[0]["prior_rank"] == 4
    assert top[0]["rank_delta"] == 3
