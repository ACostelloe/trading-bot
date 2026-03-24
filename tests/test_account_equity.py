from __future__ import annotations

import logging

from bot.account_equity import estimate_total_account_equity_usdt


class _Ex:
    markets = {"BTC/USDT": {}, "USDC/USDT": {}}

    def fetch_balance(self):
        return {"total": {"USDT": 100.0, "BTC": 0.001}}

    def fetch_ticker(self, sym: str):
        if sym == "BTC/USDT":
            return {"last": 50000.0, "close": 50000.0}
        return {"last": 1.0, "close": 1.0}


def test_estimate_total_positive() -> None:
    eq = estimate_total_account_equity_usdt(_Ex(), logging.getLogger("t"))
    assert eq > 100.0
