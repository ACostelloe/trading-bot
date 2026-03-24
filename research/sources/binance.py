from __future__ import annotations

from typing import Any, Dict, List

from research.sources.http_client import HTTPClient


class BinanceSource:
    def __init__(self, base_url: str, timeout: int, user_agent: str) -> None:
        self.http = HTTPClient(base_url, timeout, user_agent)

    def exchange_info(self) -> Dict[str, Any]:
        return self.http.get("/api/v3/exchangeInfo")

    def tickers_24h(self) -> List[Dict[str, Any]]:
        return self.http.get("/api/v3/ticker/24hr")

    def klines(self, symbol: str, interval: str, limit: int) -> List[List[Any]]:
        return self.http.get(
            "/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )

    def book_ticker(self, symbol: str) -> Dict[str, Any]:
        return self.http.get("/api/v3/ticker/bookTicker", params={"symbol": symbol})
