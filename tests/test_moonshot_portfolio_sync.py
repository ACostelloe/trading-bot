from __future__ import annotations

from bot.moonshot_portfolio_sync import apply_scanner_picks_to_portfolio_doc


def _pick(symbol_rest: str, base: str, quote: str, name: str = "") -> dict:
    return {
        "symbol": symbol_rest,
        "base_asset": base,
        "quote_asset": quote,
        "cg_name": name or base,
        "moonshot_score": 1.0,
    }


def test_managed_slots_updates_scanner_managed_rows() -> None:
    doc = {
        "moonshot": {
            "quote_asset": "USDC",
            "scanner_sync": {"mode": "managed_slots", "default_target_usdc": 7.0},
            "assets": [
                {"name": "Keep", "symbol": "BTC/USDC", "target_usdc": 100, "scanner_managed": False},
                {
                    "name": "Slot A",
                    "symbol": "OLD/USDC",
                    "target_usdc": 5,
                    "enabled": True,
                    "manual_only": False,
                    "scanner_managed": True,
                },
                {
                    "name": "Slot B",
                    "symbol": "ZZZ/USDC",
                    "target_usdc": 5,
                    "enabled": True,
                    "manual_only": False,
                    "scanner_managed": True,
                },
            ],
        }
    }
    picks = [
        _pick("AAAUSDC", "AAA", "USDC", "AAA Coin"),
        _pick("BBBUSDC", "BBB", "USDC", "BBB Coin"),
    ]
    out, w = apply_scanner_picks_to_portfolio_doc(doc, picks)
    assets = out["moonshot"]["assets"]
    assert assets[0]["symbol"] == "BTC/USDC"
    assert assets[1]["symbol"] == "AAA/USDC"
    assert assets[1]["name"] == "AAA Coin"
    assert assets[1]["enabled"] is True
    assert assets[2]["symbol"] == "BBB/USDC"
    assert "no_picks" not in " ".join(w).lower()


def test_managed_slots_disables_extra_slots() -> None:
    doc = {
        "moonshot": {
            "quote_asset": "USDC",
            "scanner_sync": {"mode": "managed_slots"},
            "assets": [
                {
                    "name": "S1",
                    "symbol": "A/USDC",
                    "target_usdc": 1,
                    "scanner_managed": True,
                },
                {
                    "name": "S2",
                    "symbol": "B/USDC",
                    "target_usdc": 1,
                    "scanner_managed": True,
                },
            ],
        }
    }
    picks = [_pick("XXUSDC", "XX", "USDC")]
    out, _ = apply_scanner_picks_to_portfolio_doc(doc, picks)
    assets = out["moonshot"]["assets"]
    assert assets[0]["symbol"] == "XX/USDC"
    assert assets[0]["enabled"] is True
    assert assets[1]["enabled"] is False


def test_append_adds_and_prunes_scanner_rows() -> None:
    doc = {
        "moonshot": {
            "quote_asset": "USDT",
            "scanner_sync": {"mode": "append", "default_target_usdc": 12.0, "max_append": 5},
            "assets": [
                {
                    "name": "Manual",
                    "symbol": "BTC/USDT",
                    "target_usdc": 50,
                    "manual_only": True,
                    "added_by_scanner": False,
                },
                {
                    "name": "Stale",
                    "symbol": "STALE/USDT",
                    "target_usdc": 10,
                    "enabled": True,
                    "manual_only": False,
                    "added_by_scanner": True,
                },
            ],
        }
    }
    picks = [_pick("NEWUSDT", "NEW", "USDT", "New Token")]
    out, _ = apply_scanner_picks_to_portfolio_doc(doc, picks)
    assets = out["moonshot"]["assets"]
    assert len(assets) == 3
    assert assets[1]["symbol"] == "STALE/USDT"
    assert assets[1]["enabled"] is False
    assert assets[2]["symbol"] == "NEW/USDT"
    assert assets[2]["target_usdc"] == 12.0
    assert assets[2].get("added_by_scanner") is True


def test_quote_mismatch_warns_and_skips() -> None:
    doc = {
        "moonshot": {
            "quote_asset": "USDT",
            "scanner_sync": {"mode": "managed_slots"},
            "assets": [
                {"name": "S", "symbol": "X/USDT", "scanner_managed": True},
            ],
        }
    }
    picks = [_pick("AAUSDC", "AA", "USDC")]
    out, w = apply_scanner_picks_to_portfolio_doc(doc, picks)
    assert any("quote_mismatch" in x for x in w)
    assert out["moonshot"]["assets"][0]["symbol"] == "X/USDT"
