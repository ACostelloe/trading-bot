"""Narrow tests for quote context, fees, ledger edge cases, moonshot min, kill switch."""

from __future__ import annotations

import pandas as pd

from bot.entry_gates import moonshot_rebalance_skip_reason
from bot.kill_switch import ConsecutiveFailureTracker
from bot.moonshot_lots import trade_client_order_id
from bot.quote_context import build_quote_execution_context
from bot.unified_ledger import (
    SOURCE_MOONSHOT,
    UnifiedLedger,
    estimate_fee_quote,
    replay_tagged_slice,
)


def test_partial_fill_fee_in_quote_from_order() -> None:
    order = {
        "fee": {"cost": 0.25, "currency": "USDT"},
        "filled": 0.4,
        "average": 100.0,
    }
    f = estimate_fee_quote(order, 0.4, 100.0, "USDT", 0.001)
    assert abs(f - 0.25) < 1e-9


def test_restart_after_partial_tp_tagged_replay() -> None:
    """Tagged history: buy 1.0, partial sell 0.3 -> replay leaves 0.7 managed."""
    trades = [
        {
            "side": "buy",
            "amount": 1.0,
            "price": 10.0,
            "fee": {},
            "info": {"clientOrderId": "msbotpartial"},
        },
        {
            "side": "sell",
            "amount": 0.3,
            "price": 20.0,
            "fee": {},
            "info": {"clientOrderId": "msbotpartial"},
        },
    ]
    tagged = [t for t in trades if trade_client_order_id(t).startswith("msbot")]
    q, c, _r = replay_tagged_slice(tagged, "X", "USDT")
    assert abs(q - 0.7) < 1e-9
    assert abs(c - 7.0) < 1e-6


def test_pre_existing_exchange_balance_not_tracked() -> None:
    led = UnifiedLedger(path="x.json", quote_currency="USDT")
    b = led.ensure_symbol("DOGE/USDT")
    b.exchange_total_base = 1000.0
    b.exchange_free_base = 1000.0
    assert b.untracked_base() == 1000.0
    led.apply_buy("DOGE/USDT", SOURCE_MOONSHOT, 100.0, 0.1, 0.01)
    assert abs(b.untracked_base() - 900.0) < 1e-6


def test_usdc_trend_quote_does_not_require_usdt_buffer() -> None:
    cfg = {
        "market": {"symbols": ["ETH/USDC"]},
        "execution": {"stablecoin_cash_buffer_usdt": 100.0, "stablecoin_cash_buffer_usdc": 0.0},
    }
    ctx = build_quote_execution_context(cfg, {"quote_asset": "USDC", "stablecoin_buffer_quote": 0.0})
    assert "USDT" not in ctx.startup_min_free_by_asset or ctx.startup_min_free_by_asset.get("USDT", 0) == 0
    assert ctx.startup_min_free_by_asset.get("USDC", 0) == 0


def test_moonshot_rebalance_below_min_notional() -> None:
    assert (
        moonshot_rebalance_skip_reason(
            needed_notional=50.0,
            spendable_quote=100.0,
            effective_min_notional=75.0,
        )
        == "below_effective_min_notional"
    )
    assert moonshot_rebalance_skip_reason(
        needed_notional=80.0,
        spendable_quote=100.0,
        effective_min_notional=75.0,
    ) is None


def test_kill_switch_trips_on_repeated_failures() -> None:
    kt = ConsecutiveFailureTracker(3)
    assert not kt.record_failure()
    assert not kt.record_failure()
    assert kt.record_failure()


def test_startup_buffer_merges_moonshot_quote_requirement() -> None:
    cfg = {"market": {"symbols": ["BTC/USDT"]}, "execution": {"stablecoin_cash_buffer_usdt": 5.0}}
    moon = {"quote_asset": "USDT", "stablecoin_buffer_quote": 15.0}
    ctx = build_quote_execution_context(cfg, moon)
    assert ctx.startup_min_free_by_asset["USDT"] == 15.0
