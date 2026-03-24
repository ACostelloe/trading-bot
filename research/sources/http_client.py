from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class HTTPClient:
    def __init__(self, base_url: str, timeout: int, user_agent: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, headers=headers or {}, timeout=self.timeout)
        if r.status_code in (418, 429):
            raise RuntimeError(f"Rate limited: HTTP {r.status_code} from {self.base_url}")
        r.raise_for_status()
        return r.json()
