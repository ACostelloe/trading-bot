from __future__ import annotations

import logging

from bot.binance_conversion import merge_binance_funding_into_free


class _SpotOnly:
    id = "kraken"

    def fetch_balance(self, params=None):
        raise AssertionError("should not fetch funding on non-binance")


def test_merge_non_binance_unchanged() -> None:
    spot = {"USDT": 10.0, "USDC": 5.0}
    m = merge_binance_funding_into_free(_SpotOnly(), spot, logging.getLogger("t"))
    assert m["USDT"] == 10.0 and m["USDC"] == 5.0


class _Binance:
    id = "binance"

    def fetch_balance(self, params=None):
        p = params or {}
        if p.get("type") == "funding":
            return {"free": {"USDC": 100.0}}
        return {"free": {"USDT": 1.0, "USDC": 2.0}}


def test_merge_binance_adds_funding() -> None:
    spot = {"USDT": 1.0, "USDC": 2.0}
    m = merge_binance_funding_into_free(_Binance(), spot, logging.getLogger("t"))
    assert abs(m["USDC"] - 102.0) < 1e-9
    assert m["USDT"] == 1.0
