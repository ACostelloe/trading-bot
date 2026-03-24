"""
Binance: spot trades only see the spot wallet. Funding (and similar) balances must be
merged for sizing and moved to spot before market sells (e.g. USDC -> USDT).
"""

from __future__ import annotations

import logging
from typing import Any

_log = logging.getLogger(__name__)


def merge_binance_funding_into_free(
    exchange: Any,
    spot_free: dict | None,
    logger: logging.Logger | None = None,
) -> dict[str, float]:
    """Spot `free` plus funding-wallet `free` (Binance only)."""
    log = logger or _log
    out: dict[str, float] = {}
    for k, v in (spot_free or {}).items():
        out[str(k)] = float(v or 0)
    if str(getattr(exchange, "id", "")) != "binance":
        return out
    try:
        fb = exchange.fetch_balance({"type": "funding"})
        for k, v in (fb.get("free") or {}).items():
            a = str(k)
            out[a] = float(out.get(a, 0.0) or 0.0) + float(v or 0)
    except Exception as exc:
        log.warning("[convert] funding wallet merge skipped: %s", exc)
    return out


def ensure_binance_spot_before_stable_sell(
    exchange: Any,
    asset: str,
    sell_qty: float,
    logger: logging.Logger | None = None,
) -> None:
    """Move asset from funding -> spot so a spot market sell can fill."""
    log = logger or _log
    if str(getattr(exchange, "id", "")) != "binance":
        return
    if sell_qty <= 1e-12:
        return
    try:
        spot = exchange.fetch_balance()
        sp = float((spot.get("free") or {}).get(asset, 0) or 0)
        if sp + 1e-12 >= sell_qty:
            return
        fund = exchange.fetch_balance({"type": "funding"})
        fd = float((fund.get("free") or {}).get(asset, 0) or 0)
    except Exception as exc:
        log.warning("[convert] balance fetch for funding->spot: %s", exc)
        return
    need = sell_qty - sp
    move = min(max(0.0, need), fd)
    if move <= 1e-12:
        return
    try:
        exchange.transfer(asset, move, "funding", "spot")
        log.info(
            "BINANCE transfer funding->spot %s qty=%.8f (for spot market sell)",
            asset,
            move,
        )
    except Exception as exc:
        log.warning("[convert] funding->spot transfer failed: %s", exc)
