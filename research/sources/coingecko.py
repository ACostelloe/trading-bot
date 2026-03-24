from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from research.sources.http_client import HTTPClient


@dataclass
class CoinGeckoClientConfig:
    base_url: str = "https://api.coingecko.com/api/v3"
    timeout: int = 15
    user_agent: str = "moonshot-scanner/1.0"
    api_key: Optional[str] = None
    api_key_header: str = "x-cg-demo-api-key"


class CoinGeckoSource:
    def __init__(self, cg: CoinGeckoClientConfig) -> None:
        self._cg = cg
        self.http = HTTPClient(cg.base_url, cg.timeout, cg.user_agent)

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self._cg.api_key:
            headers[self._cg.api_key_header] = self._cg.api_key
        return headers

    def search(self, query: str) -> Dict[str, Any]:
        return self.http.get("/search", params={"query": query}, headers=self._headers())

    def trending(self) -> Dict[str, Any]:
        return self.http.get("/search/trending", headers=self._headers())

    def categories(self) -> List[Dict[str, Any]]:
        return self.http.get("/coins/categories", headers=self._headers())

    def markets(
        self,
        vs_currency: str = "usd",
        page: int = 1,
        per_page: int = 250,
        category: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "vs_currency": vs_currency,
            "order": "volume_desc",
            "page": page,
            "per_page": per_page,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        if category:
            params["category"] = category
        return self.http.get("/coins/markets", params=params, headers=self._headers())

    def coin(self, coin_id: str) -> Dict[str, Any]:
        return self.http.get(
            f"/coins/{coin_id}",
            params={
                "localization": "false",
                "tickers": "false",
                "market_data": "true",
                "community_data": "false",
                "developer_data": "false",
                "sparkline": "false",
            },
            headers=self._headers(),
        )
