from __future__ import annotations

import math
import time
from typing import Any

from bot.swyftx_client import SwyftxClient, SwyftxClientConfig


def _tf_to_seconds(tf: str) -> int:
    return {
        "1m": 60,
        "5m": 300,
        "15m": 900,  # Swyftx bars supports 1m,5m,1h,4h,1d; we map 15m to 5m*3 window
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }.get(str(tf), 900)


class SwyftxExchange:
    """
    CCXT-like shim for the subset your bot uses.

    Notes:
    - Swyftx uses JWT auth via /auth/refresh/ (apiKey -> accessToken).
    - Market/order APIs are not CCXT, so we map to compatible shapes.
    - We implement market orders via /swap/ (market-style), because the /orders/
      docs only describe limit/stop semantics.
    """

    id = "swyftx"

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str | None = None,
        user_agent: str = "trading-bot/1.0",
        demo: bool = False,
    ) -> None:
        url = base_url or ("https://api.demo.swyftx.com.au" if demo else "https://api.swyftx.com.au")
        self.client = SwyftxClient(SwyftxClientConfig(api_key=api_key, base_url=url, user_agent=user_agent))
        self.markets: dict[str, dict[str, Any]] = {}
        self._assets_loaded = False

    # ---- Compatibility helpers ----
    def load_markets(self) -> dict[str, dict[str, Any]]:
        assets = self.client.get_market_assets()
        self._assets_loaded = True
        # We do not enumerate all possible pairs; we create markets lazily when asked.
        # Still, expose assets list under a key for debug.
        self._assets = assets
        return self.markets

    def _ensure_assets(self) -> None:
        if not self._assets_loaded:
            self.load_markets()

    def _parse_symbol(self, symbol: str) -> tuple[str, str]:
        if "/" not in symbol:
            raise ValueError(f"Expected symbol like BASE/QUOTE, got {symbol!r}")
        base, quote = symbol.split("/", 1)
        return base.upper(), quote.upper()

    def _ensure_market(self, symbol: str) -> dict[str, Any]:
        self._ensure_assets()
        if symbol in self.markets:
            return self.markets[symbol]
        base, quote = self._parse_symbol(symbol)
        # Swyftx order docs refer to primary (quote) and secondary (base).
        # We treat symbol BASE/QUOTE where QUOTE is primary.
        if self.client.asset_id_for_code(base) is None:
            raise RuntimeError(f"Swyftx unknown base asset code: {base}")
        if self.client.asset_id_for_code(quote) is None:
            raise RuntimeError(f"Swyftx unknown quote asset code: {quote}")
        self.markets[symbol] = {
            "symbol": symbol,
            "base": base,
            "quote": quote,
            "active": True,
            "type": "spot",
            "limits": {},
        }
        return self.markets[symbol]

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        # Swyftx minimum/order increments are per-asset; we keep a conservative 8dp string.
        if amount <= 0:
            return "0"
        return f"{float(amount):.8f}".rstrip("0").rstrip(".")

    # ---- Market data ----
    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        m = self._ensure_market(symbol)
        base = m["base"]
        quote = m["quote"]
        # mid-point: omit amount/limit per docs; response has mid/price strings.
        rows = self.client.get_pair_rates_multi([{"buy": base, "sell": quote}])
        row = rows[0] if rows else {}
        mid = float(row.get("mid") or 0.0)
        px = float(row.get("price") or mid or 0.0)
        return {"symbol": symbol, "last": px, "close": px, "mid": mid}

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 300) -> list[list[float]]:
        m = self._ensure_market(symbol)
        base = m["base"]
        quote = m["quote"]
        tf = str(timeframe)
        # Swyftx supports 1m,5m,1h,4h,1d. Map 15m -> 5m and take longer window.
        resolution = tf if tf in ("1m", "5m", "1h", "4h", "1d") else "5m"
        step = _tf_to_seconds(resolution)
        now_ms = int(time.time() * 1000)
        # request a window large enough; API also has limit cap 20000
        want = int(limit)
        time_start_ms = now_ms - want * step * 1000
        bars = self.client.get_bars(
            base_asset=quote,  # baseAsset in docs is the primary (e.g. AUD)
            secondary_asset=base,
            side="ask",
            resolution=resolution,
            time_start_ms=time_start_ms,
            time_end_ms=now_ms,
            limit=min(want, 20000),
        )
        out: list[list[float]] = []
        for b in bars:
            ts = int(b.get("time") or 0)
            o = float(b.get("open") or 0)
            h = float(b.get("high") or 0)
            l = float(b.get("low") or 0)
            c = float(b.get("close") or 0)
            v = float(b.get("volume") or 0)
            if ts > 0 and c > 0:
                out.append([ts, o, h, l, c, v])
        return out

    # ---- Account ----
    def fetch_balance(self) -> dict[str, Any]:
        self._ensure_assets()
        rows = self.client.get_balances()
        free: dict[str, float] = {}
        total: dict[str, float] = {}
        for r in rows:
            aid = r.get("assetId")
            bal_s = r.get("availableBalance")
            if aid is None or bal_s is None:
                continue
            code = self.client.asset_code_for_id(int(aid))
            if not code:
                continue
            amt = float(bal_s or 0.0)
            free[code] = amt
            total[code] = amt
        return {"free": free, "total": total}

    # ---- Orders (market via swap) ----
    def create_market_buy_order(self, symbol: str, amount_base: float, params: dict | None = None) -> dict[str, Any]:
        """
        Buy BASE using QUOTE.
        We implement this via /swap/ by limiting the QUOTE spent.
        """
        m = self._ensure_market(symbol)
        base = m["base"]
        quote = m["quote"]
        # We only know amount_base; convert to an estimated quote limit using current price.
        t = self.fetch_ticker(symbol)
        px = float(t.get("last") or 0.0)
        if px <= 0:
            raise RuntimeError(f"Cannot price {symbol} for market buy")
        limit_quote = float(amount_base) * px
        limit_qty = self.amount_to_precision(symbol, limit_quote)
        resp = self.client.execute_swap(buy_code=base, sell_code=quote, limit_asset_code=quote, limit_qty=limit_qty)
        return {"info": resp, "symbol": symbol, "side": "buy", "type": "market", "id": _extract_order_uuid(resp)}

    def create_market_sell_order(self, symbol: str, amount_base: float, params: dict | None = None) -> dict[str, Any]:
        """
        Sell BASE for QUOTE via /swap/ by limiting BASE sold.
        """
        m = self._ensure_market(symbol)
        base = m["base"]
        quote = m["quote"]
        limit_qty = self.amount_to_precision(symbol, float(amount_base))
        resp = self.client.execute_swap(buy_code=quote, sell_code=base, limit_asset_code=base, limit_qty=limit_qty)
        return {"info": resp, "symbol": symbol, "side": "sell", "type": "market", "id": _extract_order_uuid(resp)}

    # ---- Optional methods used by reconcile ----
    def fetch_open_orders(self, symbol: str | None = None, since: int | None = None, limit: int | None = None, params=None):
        # Not required for correctness; return empty to avoid rate/compat issues.
        return []

    def fetch_my_trades(self, symbol: str, since: int | None = None, limit: int | None = None, params=None):
        # Swyftx exposes order history, not CCXT trade history. We return [] so ledger doesn't crash.
        return []


def _extract_order_uuid(resp: dict | None) -> str | None:
    r = resp or {}
    # /swap/ returns buyResult.order.orderUuid and sellResult.order.orderUuid (strings)
    for k in ("buyResult", "sellResult"):
        row = r.get(k) or {}
        o = row.get("order") or {}
        ou = o.get("orderUuid")
        if isinstance(ou, str) and ou.strip():
            return ou.strip()
    return None

