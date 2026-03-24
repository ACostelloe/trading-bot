from __future__ import annotations

from bot.moonshot_lots import trade_client_order_id
from bot.unified_ledger import (
    SOURCE_TREND,
    UnifiedLedger,
    estimate_fee_quote,
    replay_tagged_slice,
)


def test_apply_buy_sell_realized() -> None:
    led = UnifiedLedger(path="x.json", quote_currency="USDT")
    led.apply_buy("BTC/USDT", SOURCE_TREND, 1.0, 100.0, 0.1)
    chunk = led.apply_sell("BTC/USDT", SOURCE_TREND, 1.0, 110.0, 0.1)
    sl = led.slice("BTC/USDT", SOURCE_TREND)
    assert sl.tracked_qty < 1e-12
    assert chunk > 8.0


def test_estimate_fee_fallback() -> None:
    f = estimate_fee_quote(None, 1.0, 100.0, "USDT", 0.001)
    assert abs(f - 0.1) < 1e-9


def test_replay_tagged_prefix_filters() -> None:
    trades = [
        {
            "side": "buy",
            "amount": 1.0,
            "price": 10.0,
            "fee": {},
            "info": {"clientOrderId": "msbotXXXX"},
        },
        {
            "side": "sell",
            "amount": 0.5,
            "price": 12.0,
            "fee": {},
            "info": {"clientOrderId": "msbotXXXX"},
        },
    ]
    tagged = [t for t in trades if trade_client_order_id(t).startswith("msbot")]
    q, c, r = replay_tagged_slice(tagged, "X", "USDT")
    assert q == 0.5
    assert abs(c - 5.0) < 1e-9
    assert r > 0
