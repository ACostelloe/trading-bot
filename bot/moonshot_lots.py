"""Pure helpers for moonshot managed-lot cost basis (testable without CCXT runner)."""

from __future__ import annotations


def trade_client_order_id(t: dict) -> str:
    inf = t.get("info") or {}
    return str(inf.get("clientOrderId") or t.get("clientOrderId") or "")


def apply_trade_to_avg_cost(
    qty: float,
    cost_basis: float,
    t: dict,
    base_code: str,
    quote_code: str,
) -> tuple[float, float]:
    side = str(t.get("side") or "").lower()
    amount = float(t.get("amount") or 0.0)
    price = float(t.get("price") or 0.0)
    if amount <= 0 or price <= 0:
        return qty, cost_basis

    fee = t.get("fee") or {}
    fee_cost = float(fee.get("cost") or 0.0)
    fee_ccy = str(fee.get("currency") or "")

    if side == "buy":
        base_in = amount
        if fee_ccy == base_code and fee_cost:
            base_in = max(0.0, amount - fee_cost)
        qty += base_in
        cost_basis += amount * price
        if fee_ccy == quote_code and fee_cost:
            cost_basis += fee_cost
        return qty, cost_basis

    if side == "sell":
        sold = min(amount, qty) if qty > 0 else 0.0
        if sold <= 0:
            return qty, cost_basis
        avg = cost_basis / qty if qty > 0 else 0.0
        cost_basis -= sold * avg
        qty -= sold
        if qty <= 1e-12:
            qty = 0.0
            cost_basis = 0.0
        return qty, cost_basis

    return qty, cost_basis
