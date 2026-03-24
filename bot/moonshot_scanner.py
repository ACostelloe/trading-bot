"""
Binance public Spot market scanner for moonshot-style candidates.

Uses data-api.binance.vision (or configurable base) for exchangeInfo, 24h tickers,
klines, and bookTicker. Optional CoinGecko market-cap enrichment (manual base→id map).

Use as a research / candidate generator only; execution should apply separate gates.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests

_scan_log = logging.getLogger(__name__)


def _spread_pct(bid: float, ask: float) -> float:
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 999.0
    return (ask - bid) / mid * 100.0


@dataclass
class ScannerConfig:
    quote_asset: str = "USDC"
    base_url: str = "https://data-api.binance.vision"
    timeout: int = 15

    min_24h_quote_volume: float = 1_000_000.0
    min_24h_price_change_pct: float = 3.0
    max_24h_price_change_pct: float = 80.0
    min_count: int = 200
    min_listing_days: int = 21

    max_symbols_after_broad_filter: int = 35

    signal_interval: str = "1h"
    signal_lookback: int = 120
    rv_window: int = 20
    breakout_lookback: int = 20
    atr_window: int = 14

    min_rel_volume: float = 1.3
    min_breakout_distance_pct: float = -2.0
    min_atr_pct: float = 2.0

    exclude_stables: bool = True
    exclude_leveraged_tokens: bool = True
    stable_assets: tuple[str, ...] = field(
        default_factory=lambda: (
            "USDT",
            "USDC",
            "FDUSD",
            "TUSD",
            "USDP",
            "DAI",
            "BUSD",
            "EUR",
            "TRY",
            "BRL",
        )
    )
    leveraged_suffixes: tuple[str, ...] = ("UP", "DOWN", "BULL", "BEAR")

    # --- BTC regime (broad risk-off filter) ---
    btc_regime_enabled: bool = True
    # If empty, tries BTC{quote_asset}, then BTCUSDT, then BTCUSDC.
    btc_anchor_symbol: str = ""
    # If Binance 24h % change for anchor is below this, scan returns no candidates.
    btc_min_24h_change_pct: float = -3.0

    # --- Book ticker (spread / slippage proxy) ---
    # 0 = skip book checks; else drop rows where (ask-bid)/mid*100 exceeds this.
    book_ticker_max_spread_pct: float = 0.35

    # --- Blacklist (bases = e.g. "LUNC"; symbols = full "DOGEUSDC") ---
    symbol_blacklist_bases: tuple[str, ...] = field(default_factory=tuple)
    symbol_blacklist_symbols: tuple[str, ...] = field(default_factory=tuple)

    # --- Persistence (rank delta vs last successful save) ---
    persist_path: str = ""

    # --- Optional CoinGecko (public API; rate limits apply) ---
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    # Map Binance base asset -> CoinGecko id, e.g. "RENDER": "render-token"
    coingecko_id_by_base: Dict[str, str] = field(default_factory=dict)
    min_market_cap_usd: Optional[float] = None
    max_market_cap_usd: Optional[float] = None

    top_n: int = 12


class BinanceHTTP:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code in (418, 429):
            raise RuntimeError(f"Rate limited by Binance: HTTP {r.status_code}")
        r.raise_for_status()
        return r.json()


class CoinGeckoHTTP:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code == 429:
            raise RuntimeError("CoinGecko rate limited (429)")
        r.raise_for_status()
        return r.json()


def sma(values: List[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def stddev(values: List[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    chunk = values[-window:]
    mean = sum(chunk) / len(chunk)
    var = sum((x - mean) ** 2 for x in chunk) / len(chunk)
    return math.sqrt(var)


def pct_change(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return ((b - a) / a) * 100.0


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr(highs: List[float], lows: List[float], closes: List[float], window: int) -> Optional[float]:
    if len(closes) < window + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(true_range(highs[i], lows[i], closes[i - 1]))
    if len(trs) < window:
        return None
    return sum(trs[-window:]) / window


def load_scan_state(path: str) -> Dict[str, Any]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_scan_state(path: str, payload: Dict[str, Any]) -> None:
    if not path:
        return
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def fetch_coingecko_market_caps_usd(
    gecko: CoinGeckoHTTP,
    coingecko_ids: List[str],
) -> Dict[str, float]:
    """Return gecko_id -> usd_market_cap (0 if missing)."""
    if not coingecko_ids:
        return {}
    out: Dict[str, float] = {}
    # simple/price allows batch ids
    chunk_size = 40
    for i in range(0, len(coingecko_ids), chunk_size):
        chunk = coingecko_ids[i : i + chunk_size]
        data = gecko.get(
            "/simple/price",
            params={
                "ids": ",".join(chunk),
                "vs_currencies": "usd",
                "include_market_cap": "true",
            },
        )
        for gid, row in (data or {}).items():
            if isinstance(row, dict):
                cap = row.get("usd_market_cap")
                if cap is not None:
                    out[gid] = float(cap)
    return out


class MoonshotScanner:
    def __init__(self, cfg: ScannerConfig) -> None:
        self.cfg = cfg
        self.http = BinanceHTTP(cfg.base_url, cfg.timeout)
        self._coingecko: Optional[CoinGeckoHTTP] = None
        self.last_scan_meta: Dict[str, Any] = {}

    def _gecko(self) -> CoinGeckoHTTP:
        if self._coingecko is None:
            self._coingecko = CoinGeckoHTTP(self.cfg.coingecko_base_url, self.cfg.timeout)
        return self._coingecko

    def get_exchange_info(self) -> Dict[str, Any]:
        return self.http.get("/api/v3/exchangeInfo")

    def get_all_24h_tickers(self) -> List[Dict[str, Any]]:
        return self.http.get("/api/v3/ticker/24hr")

    def get_klines(self, symbol: str, interval: str, limit: int) -> List[List[Any]]:
        return self.http.get(
            "/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )

    def get_book_ticker(self, symbol: str) -> Dict[str, Any]:
        return self.http.get("/api/v3/ticker/bookTicker", params={"symbol": symbol})

    def _resolve_btc_anchor(self, symbol_map: Dict[str, Dict[str, Any]]) -> Optional[str]:
        if self.cfg.btc_anchor_symbol:
            s = self.cfg.btc_anchor_symbol.upper()
            return s if s in symbol_map else None
        for candidate in (
            f"BTC{self.cfg.quote_asset}",
            "BTCUSDT",
            "BTCUSDC",
        ):
            if candidate in symbol_map:
                return candidate
        return None

    def _btc_regime_ok(self, ticker_by_symbol: Dict[str, Dict[str, Any]], symbol_map: Dict[str, Dict[str, Any]]) -> Tuple[bool, str]:
        if not self.cfg.btc_regime_enabled:
            return True, "btc_regime_disabled"
        anchor = self._resolve_btc_anchor(symbol_map)
        if not anchor:
            return True, "btc_anchor_not_found_skipped"
        t = ticker_by_symbol.get(anchor)
        if not t:
            return True, "btc_ticker_missing_skipped"
        try:
            chg = float(t["priceChangePercent"])
        except (KeyError, TypeError, ValueError):
            return True, "btc_change_unreadable_skipped"
        if chg < self.cfg.btc_min_24h_change_pct:
            return (
                False,
                f"btc_risk_off({anchor} 24h={chg:.2f}% < floor {self.cfg.btc_min_24h_change_pct}%)",
            )
        return True, f"btc_ok({anchor} 24h={chg:.2f}%)"

    def _blacklisted(self, symbol: str, base: str) -> bool:
        bases = {b.upper() for b in self.cfg.symbol_blacklist_bases}
        syms = {s.upper() for s in self.cfg.symbol_blacklist_symbols}
        return base.upper() in bases or symbol.upper() in syms

    def _build_symbol_map(self) -> Dict[str, Dict[str, Any]]:
        info = self.get_exchange_info()
        symbols: Dict[str, Dict[str, Any]] = {}
        for s in info["symbols"]:
            if s.get("status") != "TRADING":
                continue
            if not s.get("isSpotTradingAllowed", False):
                continue
            symbols[s["symbol"]] = s
        return symbols

    def _is_excluded_symbol(self, symbol_info: Dict[str, Any]) -> bool:
        base = symbol_info["baseAsset"]
        quote = symbol_info["quoteAsset"]
        symbol = symbol_info["symbol"]

        if self._blacklisted(symbol, base):
            return True

        if quote != self.cfg.quote_asset:
            return True

        if self.cfg.exclude_stables and base in self.cfg.stable_assets:
            return True

        if self.cfg.exclude_leveraged_tokens:
            for suffix in self.cfg.leveraged_suffixes:
                if base.endswith(suffix):
                    return True

        if symbol.endswith("BTC") or symbol.endswith("ETH"):
            return True

        return False

    def _days_since_listing(self, klines: List[List[Any]]) -> float:
        if not klines:
            return 0.0
        first_open_ms = int(klines[0][0])
        now_ms = int(time.time() * 1000)
        return (now_ms - first_open_ms) / (1000 * 60 * 60 * 24)

    def _extract_filters(self, symbol_info: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for f in symbol_info.get("filters", []) or []:
            out[f["filterType"]] = f
        return out

    def _broad_filter(
        self, symbol_map: Dict[str, Dict[str, Any]], tickers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []

        for t in tickers:
            symbol = t["symbol"]
            info = symbol_map.get(symbol)
            if not info:
                continue
            if self._is_excluded_symbol(info):
                continue

            try:
                quote_volume = float(t["quoteVolume"])
                price_change_pct = float(t["priceChangePercent"])
                count = int(t["count"])
                last_price = float(t["lastPrice"])
            except (KeyError, ValueError, TypeError):
                continue

            if quote_volume < self.cfg.min_24h_quote_volume:
                continue
            if count < self.cfg.min_count:
                continue
            if price_change_pct < self.cfg.min_24h_price_change_pct:
                continue
            if price_change_pct > self.cfg.max_24h_price_change_pct:
                continue
            if last_price <= 0:
                continue

            filters = self._extract_filters(info)
            lot_size = filters.get("LOT_SIZE", {})
            min_notional = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})

            candidates.append(
                {
                    "symbol": symbol,
                    "base_asset": info["baseAsset"],
                    "quote_asset": info["quoteAsset"],
                    "last_price": last_price,
                    "price_change_pct_24h": price_change_pct,
                    "quote_volume_24h": quote_volume,
                    "trade_count_24h": count,
                    "min_qty": float(lot_size.get("minQty", 0.0) or 0.0),
                    "step_size": float(lot_size.get("stepSize", 0.0) or 0.0),
                    "min_notional": float(
                        min_notional.get("minNotional")
                        or min_notional.get("notional")
                        or 0.0
                    ),
                }
            )

        candidates.sort(
            key=lambda x: (
                x["price_change_pct_24h"] * 0.55
                + math.log10(max(x["quote_volume_24h"], 1.0)) * 12.0
            ),
            reverse=True,
        )
        return candidates[: self.cfg.max_symbols_after_broad_filter]

    def _deep_metrics(self, symbol: str) -> Optional[Dict[str, Any]]:
        kl = self.get_klines(symbol, self.cfg.signal_interval, self.cfg.signal_lookback)
        if len(kl) < max(
            self.cfg.rv_window + 5,
            self.cfg.breakout_lookback + 5,
            self.cfg.atr_window + 2,
        ):
            return None

        opens = [float(x[1]) for x in kl]
        highs = [float(x[2]) for x in kl]
        lows = [float(x[3]) for x in kl]
        closes = [float(x[4]) for x in kl]
        volumes = [float(x[5]) for x in kl]

        listing_days = self._days_since_listing(kl)
        if listing_days < self.cfg.min_listing_days:
            return None

        close_now = closes[-1]
        close_prev = closes[-2]

        rv_ma = sma(volumes[:-1], self.cfg.rv_window)
        rel_volume = (volumes[-1] / rv_ma) if rv_ma and rv_ma > 0 else 0.0

        breakout_high = max(highs[-(self.cfg.breakout_lookback + 1) : -1])
        breakout_distance_pct = pct_change(breakout_high, close_now)

        atr_value = atr(highs, lows, closes, self.cfg.atr_window)
        atr_pct = (atr_value / close_now * 100.0) if atr_value and close_now > 0 else 0.0

        log_returns: List[float] = []
        for i in range(1, len(closes)):
            if closes[i - 1] > 0 and closes[i] > 0:
                log_returns.append(math.log(closes[i] / closes[i - 1]))
        rv_std = stddev(log_returns, min(24, len(log_returns))) if log_returns else None
        realized_vol_pct = (rv_std * math.sqrt(24) * 100.0) if rv_std is not None else 0.0

        momentum_3 = pct_change(closes[-4], close_now) if len(closes) >= 4 else 0.0
        momentum_6 = pct_change(closes[-7], close_now) if len(closes) >= 7 else 0.0
        candle_body_pct = pct_change(opens[-1], close_now)
        close_vs_prev_pct = pct_change(close_prev, close_now)

        return {
            "listing_days": listing_days,
            "rel_volume": rel_volume,
            "breakout_distance_pct": breakout_distance_pct,
            "atr_pct": atr_pct,
            "realized_vol_pct": realized_vol_pct,
            "momentum_3bars_pct": momentum_3,
            "momentum_6bars_pct": momentum_6,
            "candle_body_pct": candle_body_pct,
            "close_vs_prev_pct": close_vs_prev_pct,
        }

    def _score(self, row: Dict[str, Any]) -> float:
        score = 0.0

        score += min(max(row["price_change_pct_24h"], 0.0), 35.0) * 1.2
        score += min(math.log10(max(row["quote_volume_24h"], 1.0)), 9.0) * 6.0

        score += min(row["rel_volume"], 5.0) * 12.0
        score += min(max(row["momentum_3bars_pct"], -10.0), 20.0) * 1.8
        score += min(max(row["momentum_6bars_pct"], -15.0), 30.0) * 1.2
        score += min(row["atr_pct"], 12.0) * 3.5
        score += min(row["realized_vol_pct"], 40.0) * 0.8

        if row["breakout_distance_pct"] >= 0:
            score += max(0.0, 10.0 - row["breakout_distance_pct"]) * 3.0
        else:
            score -= abs(row["breakout_distance_pct"]) * 1.5

        if 21 <= row["listing_days"] <= 180:
            score += 10.0
        elif row["listing_days"] < 21:
            score -= 20.0

        return round(score, 2)

    def _apply_book_spread(self, row: Dict[str, Any]) -> bool:
        max_sp = self.cfg.book_ticker_max_spread_pct
        if max_sp <= 0:
            return True
        try:
            bt = self.get_book_ticker(row["symbol"])
            bid = float(bt["bidPrice"])
            ask = float(bt["askPrice"])
            sp = _spread_pct(bid, ask)
            row["book_bid"] = bid
            row["book_ask"] = ask
            row["book_spread_pct"] = round(sp, 5)
            return sp <= max_sp
        except Exception:
            row["book_spread_pct"] = None
            return False

    def _enrich_market_caps(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        idmap = self.cfg.coingecko_id_by_base
        if not idmap:
            return results
        base_to_cgid = {k.upper(): v for k, v in idmap.items()}
        needed_ids: List[str] = []
        base_for_id: Dict[str, str] = {}
        for row in results:
            b = str(row.get("base_asset", "")).upper()
            gid = base_to_cgid.get(b)
            if gid and gid not in base_for_id:
                base_for_id[gid] = b
                needed_ids.append(gid)
        if not needed_ids:
            return results
        try:
            caps = fetch_coingecko_market_caps_usd(self._gecko(), needed_ids)
        except Exception as exc:
            self.last_scan_meta["coingecko_error"] = str(exc)
            return results
        for row in results:
            b = str(row.get("base_asset", "")).upper()
            gid = base_to_cgid.get(b)
            if gid and gid in caps:
                row["market_cap_usd"] = caps[gid]
                row["coingecko_id"] = gid
        out: List[Dict[str, Any]] = []
        lo = self.cfg.min_market_cap_usd
        hi = self.cfg.max_market_cap_usd
        cap_bounds = lo is not None or hi is not None
        for row in results:
            cap = row.get("market_cap_usd")
            if cap_bounds and cap is None:
                continue
            if cap is not None:
                if lo is not None and cap < lo:
                    continue
                if hi is not None and cap > hi:
                    continue
            out.append(row)
        return out

    def _merge_prior_ranks(self, top: List[Dict[str, Any]], prior_payload: Dict[str, Any]) -> None:
        prev_ranks = (prior_payload.get("ranks") or {}) if prior_payload else {}
        for i, row in enumerate(top):
            sym = row["symbol"]
            rank_now = i + 1
            row["rank"] = rank_now
            prev = prev_ranks.get(sym)
            if isinstance(prev, dict) and "rank" in prev:
                pr = int(prev["rank"])
                row["prior_rank"] = pr
                row["rank_delta"] = pr - rank_now
                row["prior_score"] = prev.get("score")
            else:
                row["prior_rank"] = None
                row["rank_delta"] = None
                row["is_new_to_list"] = True

    def scan(self) -> List[Dict[str, Any]]:
        self.last_scan_meta = {}
        prior = load_scan_state(self.cfg.persist_path)

        symbol_map = self._build_symbol_map()
        tickers = self.get_all_24h_tickers()
        ticker_by_symbol = {t["symbol"]: t for t in tickers}

        ok, btc_reason = self._btc_regime_ok(ticker_by_symbol, symbol_map)
        self.last_scan_meta["btc_regime"] = btc_reason
        _scan_log.info("[SCAN_BINANCE] btc_gate ok=%s detail=%s", ok, btc_reason)
        if not ok:
            _scan_log.info("[SCAN_BINANCE] abort early_return=len0 reason=btc_regime")
            return []

        broad = self._broad_filter(symbol_map, tickers)
        self.last_scan_meta["binance_broad_count"] = len(broad)
        _scan_log.info(
            "[SCAN_BINANCE] binance_broad_count=%d (cap=%d)",
            len(broad),
            self.cfg.max_symbols_after_broad_filter,
        )

        results: List[Dict[str, Any]] = []
        for row in broad:
            try:
                deep = self._deep_metrics(row["symbol"])
                if not deep:
                    continue
                row = {**row, **deep}

                if row["rel_volume"] < self.cfg.min_rel_volume:
                    continue
                if row["breakout_distance_pct"] < self.cfg.min_breakout_distance_pct:
                    continue
                if row["atr_pct"] < self.cfg.min_atr_pct:
                    continue

                if not self._apply_book_spread(row):
                    continue

                row["moonshot_score"] = self._score(row)
                results.append(row)
            except Exception:
                continue

        results.sort(key=lambda x: x.get("moonshot_score", 0.0), reverse=True)
        results = self._enrich_market_caps(results)
        results.sort(key=lambda x: x.get("moonshot_score", 0.0), reverse=True)

        top = results[: self.cfg.top_n]
        self.last_scan_meta["deep_pass_count"] = len(results)
        self.last_scan_meta["shortlist_count"] = len(top)
        if top:
            self.last_scan_meta["shortlist_head"] = [
                {"symbol": r.get("symbol"), "moonshot_score": r.get("moonshot_score")} for r in top[:5]
            ]
        _scan_log.info(
            "[SCAN_BINANCE] deep_pass=%d shortlist=%d symbols=%s",
            len(results),
            len(top),
            [r.get("symbol") for r in top],
        )
        self._merge_prior_ranks(top, prior)

        if self.cfg.persist_path:
            save_scan_state(
                self.cfg.persist_path,
                {
                    "version": 1,
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "btc_regime": self.last_scan_meta.get("btc_regime"),
                    "ranks": {
                        row["symbol"]: {"rank": row["rank"], "score": row["moonshot_score"]}
                        for row in top
                    },
                },
            )

        return top


def scan_to_ccxt_symbols(picks: List[Dict[str, Any]]) -> List[str]:
    """Convert Binance REST symbols (e.g. BTCUSDC) to CCXT form (BTC/USDC)."""
    out: List[str] = []
    for p in picks:
        sym = p.get("symbol") or ""
        quote = p.get("quote_asset") or ""
        if not sym or not quote or not sym.endswith(quote):
            continue
        base = sym[: -len(quote)]
        out.append(f"{base}/{quote}")
    return out


def load_coingecko_map_yaml(path: str) -> Dict[str, str]:
    """Optional helper: load base_asset -> coingecko_id from YAML { map: { RENDER: render-token } }."""
    try:
        import yaml
    except ImportError:
        return {}
    if not path or not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    m = raw.get("map") or raw.get("coingecko_id_by_base") or {}
    return {str(k): str(v) for k, v in m.items()}


if __name__ == "__main__":
    cfg = ScannerConfig(
        quote_asset="USDC",
        top_n=10,
        persist_path="research/moonshot_scan_state.json",
    )
    scanner = MoonshotScanner(cfg)
    picks = scanner.scan()

    if not picks:
        print("No moonshot candidates found.", scanner.last_scan_meta)
    else:
        for i, p in enumerate(picks, 1):
            pr = p.get("prior_rank")
            d = p.get("rank_delta")
            delta_s = f" Δrank={d:+d}" if d is not None else " new"
            prior_s = f" was#{pr}" if pr is not None else ""
            sp = p.get("book_spread_pct")
            sp_s = f" spread%={sp}" if sp is not None else ""
            mcap = p.get("market_cap_usd")
            mc_s = f" mcap=${mcap:,.0f}" if mcap else ""
            print(
                f"{i:02d}. {p['symbol']:12s} "
                f"score={p['moonshot_score']:6.2f}{delta_s}{prior_s} "
                f"chg24h={p['price_change_pct_24h']:6.2f}% "
                f"qv24h={p['quote_volume_24h']:,.0f} "
                f"rv={p['rel_volume']:.2f} "
                f"atr%={p['atr_pct']:.2f} "
                f"brk%={p['breakout_distance_pct']:.2f} "
                f"age={p['listing_days']:.0f}d "
                f"minNotional={p['min_notional']}"
                f"{sp_s}{mc_s}"
            )
