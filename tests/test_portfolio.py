"""Portfolio cash reservation and PnL accounting."""
from __future__ import annotations

import pytest

from bot.portfolio import Portfolio, Position


def test_open_position_reserves_cash_and_fees() -> None:
    p = Portfolio(cash_usdt=1000.0)
    fee_rate = 0.001
    p.open_position(
        symbol="BTC/USDT",
        qty=0.01,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit=52000.0,
        fee_rate=fee_rate,
        entry_time="2025-01-01T00:00:00+00:00",
    )
    notional = 0.01 * 50000.0
    fee = notional * fee_rate
    assert p.cash_usdt == pytest.approx(1000.0 - notional - fee)
    pos = p.get_position("BTC/USDT")
    assert pos is not None
    assert pos.entry_notional == pytest.approx(notional)
    assert pos.entry_fee == pytest.approx(fee)


def test_open_position_insufficient_cash_raises() -> None:
    p = Portfolio(cash_usdt=10.0)
    with pytest.raises(ValueError, match="Insufficient cash"):
        p.open_position(
            symbol="BTC/USDT",
            qty=1.0,
            entry_price=50000.0,
            stop_loss=49000.0,
            take_profit=52000.0,
            fee_rate=0.001,
        )


def test_close_position_returns_net_pnl_and_restores_cash() -> None:
    p = Portfolio(cash_usdt=1000.0)
    fee_rate = 0.001
    p.open_position(
        "ETH/USDT",
        qty=1.0,
        entry_price=100.0,
        stop_loss=90.0,
        take_profit=120.0,
        fee_rate=fee_rate,
    )
    cash_after_buy = p.cash_usdt
    net = p.close_position("ETH/USDT", 110.0, fee_rate)
    # exit_notional 110, exit_fee 0.11, cost basis 100 + 0.1
    assert net == pytest.approx((110.0 - 0.11) - (100.0 + 0.1))
    assert p.cash_usdt == pytest.approx(cash_after_buy + 110.0 - 110.0 * fee_rate)
    assert not p.has_position("ETH/USDT")
    assert p.realized_pnl == pytest.approx(net)


def test_mark_to_market_with_reservation() -> None:
    p = Portfolio(cash_usdt=1000.0)
    p.open_position("X/Y", qty=2.0, entry_price=50.0, stop_loss=40.0, take_profit=70.0, fee_rate=0.0)
    eq = p.mark_to_market({"X/Y": 60.0})
    # cash after buy: 1000 - 100 = 900; mtm 2*60 = 120 -> 1020
    assert eq == pytest.approx(900.0 + 120.0)


def test_from_dict_legacy_single_position() -> None:
    data = {
        "cash_usdt": 500.0,
        "realized_pnl": 0.0,
        "daily_pnl": 0.0,
        "position": {
            "symbol": "BTC/USDT",
            "qty": 0.001,
            "entry_price": 40000.0,
            "stop_loss": 39000.0,
            "take_profit": 42000.0,
            "entry_time": None,
        },
    }
    p = Portfolio.from_dict(data)
    assert p.has_position("BTC/USDT")
    assert p.cash_usdt == 500.0
