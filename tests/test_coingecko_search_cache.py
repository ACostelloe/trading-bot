from __future__ import annotations

import os
import tempfile
import time

from research.cache.coingecko_search_cache import CoinGeckoSearchCache


def test_cache_roundtrip_and_ttl() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "cg.json")
        c = CoinGeckoSearchCache(path, ttl_seconds=3600)
        assert c.get_coin_id("JUP") is None
        c.set_coin_id("JUP", "jupiter-exchange-solana")
        c2 = CoinGeckoSearchCache(path, ttl_seconds=3600)
        assert c2.get_coin_id("jup") == "jupiter-exchange-solana"


def test_cache_expired() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "cg.json")
        c = CoinGeckoSearchCache(path, ttl_seconds=1)
        c.set_coin_id("X", "xid")
        time.sleep(1.1)
        c2 = CoinGeckoSearchCache(path, ttl_seconds=1)
        assert c2.get_coin_id("X") is None
