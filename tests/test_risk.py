"""Risk checks and sizing caps."""
from __future__ import annotations

import pytest

from bot.risk import cap_qty_by_cash, check_trade_allowed, calculate_position_size_from_risk


@pytest.fixture
def risk_config() -> dict:
    return {
        "risk": {
            "max_open_positions_total": 2,
            "max_daily_loss": 0.02,
            "account_risk_per_trade": 0.01,
            "min_order_notional": 20.0,
        }
    }


def test_cap_qty_by_cash() -> None:
    q = cap_qty_by_cash(1000.0, entry_price=50.0, fee_rate=0.001)
    # 1000 / (50 * 1.001)
    assert q == pytest.approx(1000.0 / (50.0 * 1.001))


def test_check_trade_allowed_blocks_max_positions(risk_config: dict) -> None:
    d = check_trade_allowed(
        available_cash=10_000.0,
        entry_price=100.0,
        stop_price=90.0,
        config=risk_config,
        total_open_positions=2,
        already_in_symbol=False,
        daily_pnl_fraction=0.0,
        fee_rate=0.001,
    )
    assert not d.allowed
    assert d.reason == "max_open_positions_total_reached"


def test_check_trade_allowed_blocks_already_in_symbol(risk_config: dict) -> None:
    d = check_trade_allowed(
        available_cash=10_000.0,
        entry_price=100.0,
        stop_price=90.0,
        config=risk_config,
        total_open_positions=0,
        already_in_symbol=True,
        daily_pnl_fraction=0.0,
        fee_rate=0.001,
    )
    assert not d.allowed
    assert "symbol" in d.reason


def test_qty_capped_by_cash_smaller_than_risk(risk_config: dict) -> None:
    # Risk-based size > affordable max qty; cash cap wins; notional still above min
    d = check_trade_allowed(
        available_cash=3000.0,
        entry_price=100.0,
        stop_price=99.0,  # tight stop -> large risk-based qty
        config=risk_config,
        total_open_positions=0,
        already_in_symbol=False,
        daily_pnl_fraction=0.0,
        fee_rate=0.001,
    )
    assert d.allowed
    risk_qty = calculate_position_size_from_risk(3000.0, 100.0, 99.0, 0.01)
    cash_qty = cap_qty_by_cash(3000.0, 100.0, 0.001)
    assert risk_qty > cash_qty
    assert d.qty == pytest.approx(min(risk_qty, cash_qty))


def test_min_order_notional_blocks(risk_config: dict) -> None:
    d = check_trade_allowed(
        available_cash=30.0,
        entry_price=10.0,
        stop_price=9.0,
        config=risk_config,
        total_open_positions=0,
        already_in_symbol=False,
        daily_pnl_fraction=0.0,
        fee_rate=0.001,
    )
    assert not d.allowed
    assert "notional" in d.reason
