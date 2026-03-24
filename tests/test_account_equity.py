from __future__ import annotations

import logging

from bot.account_equity import estimate_total_account_equity_usdt


class _Ex:
    id = "kraken"
    markets = {"BTC/USDT": {}, "USDC/USDT": {}}

    def fetch_balance(self, params=None):
        return {"total": {"USDT": 100.0, "BTC": 0.001}}

    def fetch_ticker(self, sym: str):
        if sym == "BTC/USDT":
            return {"last": 50000.0, "close": 50000.0}
        return {"last": 1.0, "close": 1.0}


def test_estimate_total_positive() -> None:
    eq = estimate_total_account_equity_usdt(_Ex(), logging.getLogger("t"))
    assert eq > 100.0


class _BinanceEx:
    id = "binance"
    markets = {"BTC/USDT": {}}

    def fetch_balance(self, params=None):
        p = params or {}
        w = p.get("type")
        if w == "funding":
            return {"total": {"USDT": 40.0}}
        if w == "savings":
            return {"total": {}}
        return {"total": {"USDT": 60.0}}

    def fetch_ticker(self, sym: str):
        return {"last": 1.0, "close": 1.0}


def test_binance_includes_funding_wallet() -> None:
    eq = estimate_total_account_equity_usdt(_BinanceEx(), logging.getLogger("t"))
    assert abs(eq - 100.0) < 1e-6
