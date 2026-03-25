from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class SwyftxClientConfig:
    api_key: str
    base_url: str = "https://api.swyftx.com.au"
    user_agent: str = "trading-bot/1.0"
    timeout_seconds: int = 20


class SwyftxClient:
    """
    Minimal Swyftx REST client.

    Auth model (per Swyftx Apiary):
    - POST /auth/refresh/ with {"apiKey": "<apiKey>"} returns {"accessToken": "..."}
    - Use Authorization: Bearer <JWT> + User-Agent on secured endpoints.
    """

    def __init__(self, cfg: SwyftxClientConfig):
        if not cfg.api_key.strip():
            raise RuntimeError("SWYFTX_API_KEY is required")
        self.cfg = cfg
        self._jwt: str | None = None
        self._jwt_set_at: float = 0.0

        self._asset_by_code: dict[str, dict[str, Any]] = {}
        self._asset_by_id: dict[int, dict[str, Any]] = {}

    def _headers(self, *, authed: bool) -> dict[str, str]:
        h = {
            "Content-Type": "application/json",
            "User-Agent": self.cfg.user_agent,
        }
        if authed:
            h["Authorization"] = f"Bearer {self._ensure_jwt()}"
        return h

    def _request(self, method: str, path: str, *, authed: bool, json: Any | None = None, params: dict | None = None):
        url = self.cfg.base_url.rstrip("/") + path
        r = requests.request(
            method=method,
            url=url,
            headers=self._headers(authed=authed),
            json=json,
            params=params,
            timeout=self.cfg.timeout_seconds,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Swyftx HTTP {r.status_code} {path}: {r.text[:500]}")
        return r.json() if r.content else None

    def _ensure_jwt(self) -> str:
        # Tokens last ~1 week, but we refresh proactively every 6 days.
        if self._jwt and (time.time() - self._jwt_set_at) < (6 * 86400):
            return self._jwt
        resp = self._request("POST", "/auth/refresh/", authed=False, json={"apiKey": self.cfg.api_key})
        tok = str((resp or {}).get("accessToken") or "").strip()
        if not tok:
            raise RuntimeError(f"Swyftx auth refresh did not return accessToken: {resp}")
        self._jwt = tok
        self._jwt_set_at = time.time()
        return tok

    # ---- Public endpoints ----
    def get_market_assets(self) -> list[dict[str, Any]]:
        rows = self._request("GET", "/markets/assets/", authed=False)
        if not isinstance(rows, list):
            raise RuntimeError(f"Unexpected /markets/assets/ payload: {type(rows)}")
        # cache lookups
        self._asset_by_code = {str(r.get("code") or "").upper(): r for r in rows if r.get("code")}
        self._asset_by_id = {int(r.get("id")): r for r in rows if r.get("id") is not None}
        return rows

    def asset_code_for_id(self, asset_id: int) -> str | None:
        a = self._asset_by_id.get(int(asset_id))
        return str(a.get("code")).upper() if a and a.get("code") else None

    def asset_id_for_code(self, code: str) -> int | None:
        a = self._asset_by_code.get(str(code).upper())
        return int(a.get("id")) if a and a.get("id") is not None else None

    # ---- Authenticated endpoints ----
    def get_balances(self) -> list[dict[str, Any]]:
        rows = self._request("GET", "/user/balance/", authed=True)
        if not isinstance(rows, list):
            raise RuntimeError(f"Unexpected /user/balance/ payload: {type(rows)}")
        return rows

    def get_pair_rates_multi(self, pairs: list[dict[str, str]]) -> list[dict[str, Any]]:
        # POST /orders/rate/multi/ accepts [{"buy":"BTC","sell":"AUD","amount":"1000","limit":"AUD"}, ...]
        resp = self._request("POST", "/orders/rate/multi/", authed=True, json=pairs)
        if not isinstance(resp, list):
            raise RuntimeError(f"Unexpected /orders/rate/multi/ payload: {type(resp)}")
        return resp

    def get_bars(
        self,
        *,
        base_asset: str,
        secondary_asset: str,
        resolution: str,
        time_start_ms: int,
        time_end_ms: int,
        limit: int,
        side: str = "ask",
    ) -> list[dict[str, Any]]:
        # GET /charts/getBars/baseAsset/secondaryAsset/side/?resolution=...&timeStart=...&timeEnd=...&limit=...
        path = f"/charts/getBars/{base_asset}/{secondary_asset}/{side}/"
        params = {
            "resolution": resolution,
            "timeStart": int(time_start_ms),
            "timeEnd": int(time_end_ms),
            "limit": int(limit),
        }
        rows = self._request("GET", path, authed=False, params=params)
        # Docs show a list of bars, but the live API returns {"candles": [...]}.
        if isinstance(rows, dict):
            err = rows.get("error")
            if isinstance(err, dict):
                raise RuntimeError(f"Swyftx bars error: {err.get('error')} {err.get('message')}".strip())
            candles = rows.get("candles")
            if isinstance(candles, list):
                return candles
        if not isinstance(rows, list):
            # Could also be an upstream HTML (cloudflare) response wrapped by requests.json()
            raise RuntimeError(f"Unexpected bars payload: {type(rows)} keys={list(rows)[:10] if isinstance(rows, dict) else ''}")
        return rows

    def execute_swap(
        self,
        *,
        buy_code: str,
        sell_code: str,
        limit_asset_code: str,
        limit_qty: str,
        intermediate_asset_id: int | None = None,
    ) -> dict[str, Any]:
        """
        Market-style conversion between two assets (Swyftx /swap/).
        Payload uses asset codes (buy/sell) + limitAsset (asset id) + limitQty (string).
        """
        if not self._asset_by_code:
            self.get_market_assets()
        buy_id = self.asset_id_for_code(buy_code)
        sell_id = self.asset_id_for_code(sell_code)
        if buy_id is None:
            raise RuntimeError(f"Unknown buy asset code: {buy_code}")
        if sell_id is None:
            raise RuntimeError(f"Unknown sell asset code: {sell_code}")
        limit_asset_id = self.asset_id_for_code(limit_asset_code)
        if limit_asset_id is None:
            raise RuntimeError(f"Unknown asset code for limitAsset: {limit_asset_code}")
        body: dict[str, Any] = {
            # Swyftx /swap expects asset ids (as strings), not ticker codes.
            "buy": str(int(buy_id)),
            "sell": str(int(sell_id)),
            "limitAsset": int(limit_asset_id),
            "limitQty": str(limit_qty),
        }
        if intermediate_asset_id is not None:
            body["intermediateAssetId"] = int(intermediate_asset_id)
        return self._request("POST", "/swap/", authed=True, json=body)

