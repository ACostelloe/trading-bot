"""Shared trend entry gates for live, paper, and research backtests."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from bot.event_controls import event_controls_for_symbol
from bot.moonshot_guard import evaluate_moonshot_entry
from bot.risk import check_trade_allowed


@dataclass
class TrendBuyGateResult:
    allowed: bool
    reason: str
    qty: float = 0.0
    event_mult: float = 1.0
    meta: dict = field(default_factory=dict)


def effective_min_notional(
    *,
    risk_min: float,
    live_min_notional_check: bool,
    market_min_cost: float | None,
) -> float:
    m = float(risk_min or 0.0)
    if live_min_notional_check and market_min_cost is not None and float(market_min_cost) > 0:
        m = max(m, float(market_min_cost))
    return m


def evaluate_trend_buy_gates(
    *,
    symbol: str,
    signal_price: float,
    signal_stop_loss: float,
    signal_take_profit: float,
    bar_timestamp: pd.Timestamp,
    risk_cfg: dict,
    config: dict,
    available_cash: float,
    open_positions_count: int,
    already_in_symbol: bool,
    daily_pnl: float,
    fee_rate: float,
    starting_balance: float,
    trades_today: int,
    max_trades_per_day: int,
    allow_multiple_positions: bool,
    live_min_notional_check: bool,
    market_min_cost: float | None,
    spendable_cash_after_buffer: float | None = None,
    manual_buy_mode: bool = False,
    manual_buy_notional: float = 0.0,
) -> TrendBuyGateResult:
    """
    Mirrors live trend buy checks (caps, events, risk, notional floors, moonshot).
    When manual_buy_mode is True, qty is derived from manual_buy_notional and spendable_cash_after_buffer.
    """
    exec_cfg = config.get("execution") or {}

    # max_trades_per_day <= 0 means no cap (same as live/paper/backtest after alignment).
    if max_trades_per_day > 0 and trades_today >= max_trades_per_day:
        return TrendBuyGateResult(False, "max_live_trades_per_day_reached", meta={"trades_today": trades_today})

    if (not allow_multiple_positions) and open_positions_count > 0:
        return TrendBuyGateResult(False, "multiple_positions_disabled_existing_open", meta={})

    event_mult, event_block = event_controls_for_symbol(symbol, bar_timestamp, risk_cfg)
    if event_block:
        return TrendBuyGateResult(False, "event_window_blocked", event_mult=event_mult, meta={})

    qty = 0.0
    if manual_buy_mode and manual_buy_notional > 0:
        spendable = float(spendable_cash_after_buffer if spendable_cash_after_buffer is not None else available_cash)
        target_notional = min(float(manual_buy_notional), spendable)
        if target_notional <= 0:
            return TrendBuyGateResult(
                False,
                "manual_buy_no_spendable_after_buffer",
                event_mult=event_mult,
                meta={"available_cash": available_cash, "spendable": spendable},
            )
        qty = (target_notional / float(signal_price)) * event_mult
    else:
        decision = check_trade_allowed(
            available_cash=available_cash,
            entry_price=float(signal_price),
            stop_price=float(signal_stop_loss),
            config=config,
            total_open_positions=open_positions_count,
            already_in_symbol=already_in_symbol,
            daily_pnl_fraction=daily_pnl / max(starting_balance, 1.0),
            fee_rate=fee_rate,
        )
        if not decision.allowed:
            return TrendBuyGateResult(False, decision.reason, event_mult=event_mult, meta={})
        qty = float(decision.qty) * event_mult

    if qty <= 0:
        return TrendBuyGateResult(False, "qty_non_positive_after_events", event_mult=event_mult, meta={"qty": qty})

    notional = qty * float(signal_price)
    eff_min = effective_min_notional(
        risk_min=float(config["risk"]["min_order_notional"]),
        live_min_notional_check=live_min_notional_check,
        market_min_cost=market_min_cost,
    )
    if notional < eff_min:
        return TrendBuyGateResult(
            False,
            "order_notional_below_effective_minimum",
            qty=qty,
            event_mult=event_mult,
            meta={"notional": notional, "effective_min_notional": eff_min},
        )

    if not manual_buy_mode and qty * float(signal_price) > available_cash + 1e-9:
        return TrendBuyGateResult(
            False,
            "order_exceeds_available_cash",
            qty=qty,
            event_mult=event_mult,
            meta={"notional": notional, "available_cash": available_cash},
        )

    return TrendBuyGateResult(
        allowed=True,
        reason="passed_precheck",
        qty=qty,
        event_mult=event_mult,
        meta={"notional": notional, "effective_min_notional": eff_min},
    )


def moonshot_rebalance_skip_reason(
    *,
    needed_notional: float,
    spendable_quote: float,
    effective_min_notional: float,
) -> str | None:
    """Why a moonshot rebalance buy would be skipped (matches run_moonshot logic)."""
    buy_notional = min(max(0.0, float(needed_notional)), max(0.0, float(spendable_quote)))
    if buy_notional < float(effective_min_notional):
        return "below_effective_min_notional"
    return None


def evaluate_moonshot_gate_for_trend_entry(
    *,
    symbol: str,
    entry_notional: float,
    current_equity: float,
    open_positions_count: int,
    config: dict,
    gate_result: TrendBuyGateResult,
) -> TrendBuyGateResult:
    """Apply moonshot checklist after sizing passed other gates."""
    if not gate_result.allowed:
        return gate_result
    moonshot = evaluate_moonshot_entry(
        symbol=symbol,
        entry_notional=float(entry_notional),
        current_equity=float(current_equity),
        open_positions_count=int(open_positions_count),
        config=config,
    )
    if not moonshot.allowed:
        return TrendBuyGateResult(
            False,
            "moonshot_checklist_failed",
            qty=gate_result.qty,
            event_mult=gate_result.event_mult,
            meta={
                "moonshot_score": moonshot.score,
                "moonshot_min": moonshot.min_required,
                "moonshot_reasons": moonshot.reasons,
                **gate_result.meta,
            },
        )
    return gate_result
