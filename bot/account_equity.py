"""
Approximate total account value in USDT terms (Binance-style estimated balance).

Spot totals plus optional Binance Funding + Simple Earn (`savings`) wallets, then marks
each asset via */USDT (or 1:1 for stables).
"""

from __future__ import annotations

import logging
from typing import Any

_log = logging.getLogger(__name__)

_STABLES_1_TO_1_USDT = frozenset({"USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USDE"})


def _merge_balance_totals(exchange: Any, log: logging.Logger) -> dict[str, float]:
    """Merge `total` maps from spot and, on Binance, funding + savings (Earn) if available."""
    merged: dict[str, float] = {}

    def add_bal(bal: dict) -> None:
        t = bal.get("total") or {}
        for asset, qty in t.items():
            a = str(asset)
            merged[a] = float(merged.get(a, 0) or 0) + float(qty or 0)

    add_bal(exchange.fetch_balance())
    exid = str(getattr(exchange, "id", "") or "")
    if exid == "binance":
        for wtype in ("funding", "savings"):
            try:
                add_bal(exchange.fetch_balance({"type": wtype}))
            except Exception as exc:
                log.debug("[equity] optional wallet %s omitted: %s", wtype, exc)
    return merged


def estimate_total_account_equity_usdt(exchange: Any, logger: logging.Logger | None = None) -> float:
    """
    Sum positive balances (spot + Binance funding/savings when applicable), valued in USDT.

    Closer to Binance UI “Estimated Balance” than spot-only; futures/margin wallets are
    not included. Small drift vs the app is normal.
    """
    log = logger or _log
    total = _merge_balance_totals(exchange, log)
    usdt_equiv = 0.0
    for asset, qty in total.items():
        q = float(qty or 0.0)
        if q <= 1e-12:
            continue
        a = str(asset).upper()
        if a == "USDT":
            usdt_equiv += q
            continue
        if a in _STABLES_1_TO_1_USDT:
            pair = f"{a}/USDT"
            if pair in exchange.markets:
                try:
                    t = exchange.fetch_ticker(pair)
                    px = float(t.get("last") or t.get("close") or 0.0)
                    usdt_equiv += q * px if px > 0 else q
                except Exception as exc:
                    log.warning("[equity] stable %s mark failed (%s); using 1:1", a, exc)
                    usdt_equiv += q
            else:
                usdt_equiv += q
            continue
        pair = f"{a}/USDT"
        if pair not in exchange.markets:
            log.warning("[equity] skip %s: no market %s", a, pair)
            continue
        try:
            t = exchange.fetch_ticker(pair)
            px = float(t.get("last") or t.get("close") or 0.0)
            if px > 0:
                usdt_equiv += q * px
        except Exception as exc:
            log.warning("[equity] skip %s: %s", a, exc)
    return float(usdt_equiv)
