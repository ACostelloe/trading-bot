from __future__ import annotations

from bot.moonshot_plans import parse_asset_plans


def test_parse_asset_plans_minimal() -> None:
    moon = {
        "assets": [
            {"name": "X", "symbol": "X/USDT", "target_usdc": 5, "enabled": True, "manual_only": False},
        ]
    }
    plans = parse_asset_plans(moon)
    assert len(plans) == 1
    assert plans[0].symbol == "X/USDT"
    assert plans[0].target_usdc == 5.0
