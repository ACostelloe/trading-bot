from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RiskDecision:
    allowed: bool
    qty: float = 0.0
    reason: str = ""


def calculate_position_size_from_risk(
    balance_usdt: float,
    entry_price: float,
    stop_price: float,
    risk_fraction: float,
) -> float:
    risk_amount = balance_usdt * risk_fraction
    stop_distance = entry_price - stop_price

    if stop_distance <= 0:
        return 0.0

    return max(risk_amount / stop_distance, 0.0)


def cap_qty_by_cash(
    available_cash: float,
    entry_price: float,
    fee_rate: float,
) -> float:
    unit_cost = entry_price * (1 + fee_rate)
    if unit_cost <= 0:
        return 0.0
    return max(available_cash / unit_cost, 0.0)


def check_trade_allowed(
    available_cash: float,
    entry_price: float,
    stop_price: float,
    config: dict,
    total_open_positions: int,
    already_in_symbol: bool,
    daily_pnl_fraction: float,
    fee_rate: float,
) -> RiskDecision:
    if already_in_symbol:
        return RiskDecision(False, reason="position_already_open_for_symbol")

    if total_open_positions >= config["risk"]["max_open_positions_total"]:
        return RiskDecision(False, reason="max_open_positions_total_reached")

    if daily_pnl_fraction <= -abs(config["risk"]["max_daily_loss"]):
        return RiskDecision(False, reason="max_daily_loss_reached")

    risk_qty = calculate_position_size_from_risk(
        balance_usdt=available_cash,
        entry_price=entry_price,
        stop_price=stop_price,
        risk_fraction=config["risk"]["account_risk_per_trade"],
    )

    cash_qty = cap_qty_by_cash(
        available_cash=available_cash,
        entry_price=entry_price,
        fee_rate=fee_rate,
    )

    qty = min(risk_qty, cash_qty)

    notional = qty * entry_price
    if qty <= 0:
        return RiskDecision(False, reason="invalid_position_size")

    if notional < config["risk"]["min_order_notional"]:
        return RiskDecision(False, reason="order_notional_below_minimum")

    return RiskDecision(True, qty=qty, reason="approved")
