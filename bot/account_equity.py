"""
Approximate total spot account value in USDT terms (Binance-style estimated balance).

Uses fetch_balance totals and marks each asset via */USDT (or 1:1 for USDT).
"""

from __future__ import annotations

import logging
from typing import Any

_log = logging.getLogger(__name__)

_STABLES_1_TO_1_USDT = frozenset({"USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USDE"})


def estimate_total_account_equity_usdt(exchange: Any, logger: logging.Logger | None = None) -> float:
    """
    Sum all positive `total` balances, valued in USDT (spot).

    Close to Binance UI “Estimated Balance” when markets are liquid; small drift vs
    exchange mark is normal.
    """
    log = logger or _log
    bal = exchange.fetch_balance()
    total = bal.get("total", {}) or {}
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
