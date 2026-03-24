from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional


class CoinGeckoSearchCache:
    """Disk-backed cache for base_asset -> CoinGecko coin_id (search resolution)."""

    def __init__(self, path: str, ttl_seconds: int = 86_400) -> None:
        self.path = path or ""
        self.ttl_seconds = max(0, int(ttl_seconds))
        self._entries: Dict[str, Dict[str, Any]] = {}
        if self.path:
            self._load()

    def _load(self) -> None:
        if not self.path or not os.path.isfile(self.path):
            self._entries = {}
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self._entries = raw if isinstance(raw, dict) else {}
        except (json.JSONDecodeError, OSError):
            self._entries = {}

    def _save(self) -> None:
        if not self.path:
            return
        directory = os.path.dirname(os.path.abspath(self.path))
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._entries, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    def get_coin_id(self, base_asset: str) -> Optional[str]:
        if self.ttl_seconds <= 0 or not self.path:
            return None
        key = base_asset.upper()
        row = self._entries.get(key)
        if not isinstance(row, dict):
            return None
        ts = float(row.get("ts", 0) or 0)
        if time.time() - ts > self.ttl_seconds:
            return None
        cid = row.get("coin_id")
        return str(cid) if cid else None

    def set_coin_id(self, base_asset: str, coin_id: str) -> None:
        if not self.path:
            return
        key = base_asset.upper()
        self._entries[key] = {"coin_id": str(coin_id), "ts": time.time()}
        self._save()
