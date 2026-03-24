from __future__ import annotations

from bot.moonshot_lots import apply_trade_to_avg_cost


def test_apply_trade_avg_cost_buy_then_sell() -> None:
    q, c = 0.0, 0.0
    q, c = apply_trade_to_avg_cost(q, c, {"side": "buy", "amount": 2.0, "price": 10.0, "fee": {}}, "X", "USDT")
    assert abs(q - 2.0) < 1e-9 and abs(c - 20.0) < 1e-9
    q, c = apply_trade_to_avg_cost(q, c, {"side": "sell", "amount": 1.0, "price": 15.0, "fee": {}}, "X", "USDT")
    assert abs(q - 1.0) < 1e-9
    assert abs(c - 10.0) < 1e-9
    avg = c / q if q else 0.0
    assert abs(avg - 10.0) < 1e-9
