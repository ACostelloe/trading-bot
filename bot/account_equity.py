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
# USD-pegged; when quote is AUD and no USDT/AUD market exists, cross via BTC (see below).
_STABLES_USD_PEG = frozenset({"USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP", "USDE"})


def _quote_equiv_via_btc_cross(
    exchange: Any, *, asset: str, qty: float, quote_asset: str, log: logging.Logger
) -> float | None:
    """
    Value USD-pegged stables in quote when direct */quote is missing (e.g. no USDT/AUD on Swyftx).
    Uses: qty * (BTC/quote) / (BTC/USDT) — all USD stables use the USDT/BTC leg for the USD peg.
    """
    a = asset.upper()
    q = quote_asset.upper()
    if qty <= 0 or a == q or a not in _STABLES_USD_PEG:
        return None
    if q == "USDT":
        return None
    btc_q = f"BTC/{q}"
    btc_usdt = "BTC/USDT"
    try:
        tq = exchange.fetch_ticker(btc_q)
        tu = exchange.fetch_ticker(btc_usdt)
        pq = float(tq.get("last") or tq.get("close") or 0.0)
        pu = float(tu.get("last") or tu.get("close") or 0.0)
        if pq <= 0 or pu <= 0:
            return None
        # quote per USD-stable ≈ (quote per BTC) / (USDT per BTC)
        return float(qty * (pq / pu))
    except Exception as exc:
        log.debug("[equity] btc cross %s->%s failed: %s", a, q, exc)
        return None


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


def estimate_total_account_equity(
    exchange: Any, *, quote_asset: str = "USDT", logger: logging.Logger | None = None
) -> float:
    """
    Sum positive balances (spot + Binance funding/savings when applicable), valued in quote_asset.

    Closer to exchange UI “Estimated Balance” than spot-only; futures/margin wallets are
    not included. Small drift vs the app is normal.
    """
    log = logger or _log
    total = _merge_balance_totals(exchange, log)
    q = str(quote_asset).upper().strip() or "USDT"
    quote_equiv = 0.0
    for asset, qty in total.items():
        q = float(qty or 0.0)
        if q <= 1e-12:
            continue
        a = str(asset).upper()
        if a == str(quote_asset).upper():
            quote_equiv += q
            continue
        if a in _STABLES_1_TO_1_USDT and str(quote_asset).upper() == "USDT":
            pair = f"{a}/{quote_asset.upper()}"
            if pair in exchange.markets:
                try:
                    t = exchange.fetch_ticker(pair)
                    px = float(t.get("last") or t.get("close") or 0.0)
                    quote_equiv += q * px if px > 0 else q
                except Exception as exc:
                    log.warning("[equity] stable %s mark failed (%s); using 1:1", a, exc)
                    quote_equiv += q
            else:
                quote_equiv += q
            continue
        pair = f"{a}/{str(quote_asset).upper()}"
        if pair not in exchange.markets:
            if a in _STABLES_USD_PEG and str(quote_asset).upper() != "USDT":
                crossed = _quote_equiv_via_btc_cross(
                    exchange, asset=a, qty=q, quote_asset=str(quote_asset), log=log
                )
                if crossed is not None:
                    quote_equiv += crossed
                    continue
            log.warning("[equity] skip %s: no market %s", a, pair)
            continue
        try:
            t = exchange.fetch_ticker(pair)
            px = float(t.get("last") or t.get("close") or 0.0)
            if px > 0:
                quote_equiv += q * px
        except Exception as exc:
            log.warning("[equity] skip %s: %s", a, exc)
    return float(quote_equiv)


def estimate_total_account_equity_usdt(exchange: Any, logger: logging.Logger | None = None) -> float:
    # Backwards-compatible wrapper for existing code paths.
    return estimate_total_account_equity(exchange, quote_asset="USDT", logger=logger)
