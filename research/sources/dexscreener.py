from __future__ import annotations

from typing import Any, Dict, List

from research.sources.http_client import HTTPClient


class DexScreenerSource:
    def __init__(self, base_url: str, timeout: int, user_agent: str) -> None:
        self.http = HTTPClient(base_url, timeout, user_agent)

    def search_pairs(self, query: str) -> Dict[str, Any]:
        return self.http.get("/latest/dex/search", params={"q": query})

    def token_boosts_top(self) -> List[Dict[str, Any]]:
        return self.http.get("/token-boosts/top/v1")

    def token_profiles_latest(self) -> List[Dict[str, Any]]:
        return self.http.get("/token-profiles/latest/v1")
