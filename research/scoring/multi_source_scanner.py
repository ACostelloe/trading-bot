from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from research.cache.coingecko_search_cache import CoinGeckoSearchCache
from research.sources.binance import BinanceSource
from research.sources.coingecko import CoinGeckoClientConfig, CoinGeckoSource
from research.sources.dexscreener import DexScreenerSource

from bot.moonshot_scanner import (
    _spread_pct,
    atr,
    load_scan_state,
    pct_change,
    save_scan_state,
    sma,
    stddev,
)

_log = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    binance_base_url: str = "https://data-api.binance.vision"
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    dexscreener_base_url: str = "https://api.dexscreener.com"

    coingecko_api_key: Optional[str] = None
    coingecko_api_key_header: str = "x-cg-demo-api-key"

    timeout: int = 15
    user_agent: str = "moonshot-scanner/1.0"


@dataclass
class ScannerRules:
    quote_asset: str = "USDC"

    min_24h_quote_volume: float = 1_000_000.0
    min_24h_price_change_pct: float = 3.0
    max_24h_price_change_pct: float = 80.0
    min_24h_trade_count: int = 200

    min_market_cap_usd: float = 10_000_000.0
    max_market_cap_usd: float = 1_500_000_000.0
    min_dex_liquidity_usd: float = 150_000.0
    min_rel_volume: float = 1.25
    min_listing_days_binance: int = 21
    max_candidates_after_binance: int = 40
    top_n: int = 12

    interval: str = "1h"
    lookback: int = 120
    rv_window: int = 20
    breakout_window: int = 20
    atr_window: int = 14

    stable_assets: Tuple[str, ...] = (
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
    leveraged_suffixes: Tuple[str, ...] = ("UP", "DOWN", "BULL", "BEAR")

    category_bonus: Dict[str, float] = field(
        default_factory=lambda: {
            "artificial-intelligence": 10.0,
            "depin": 9.0,
            "real-world-assets-rwa": 7.5,
            "gaming": 5.5,
            "meme-token": 4.0,
            "solana-ecosystem": 4.0,
        }
    )

    # Optional: base ticker -> CoinGecko id (avoids /search; reduces rate limits).
    coingecko_id_by_base: Dict[str, str] = field(default_factory=dict)
    coingecko_cache_path: str = ""
    coingecko_cache_ttl_seconds: int = 86_400

    btc_regime_enabled: bool = True
    btc_anchor_symbol: str = ""
    btc_min_24h_change_pct: float = -3.0

    # 0 = skip book spread filter.
    book_ticker_max_spread_pct: float = 0.35

    persist_path: str = ""


class MultiSourceMoonshotScanner:
    """discover (Binance) -> enrich (CoinGecko, DexScreener) -> score -> filter."""

    def __init__(self, source_cfg: SourceConfig, rules: ScannerRules) -> None:
        self.source_cfg = source_cfg
        self.rules = rules
        self.binance = BinanceSource(
            source_cfg.binance_base_url,
            source_cfg.timeout,
            source_cfg.user_agent,
        )
        cg_cfg = CoinGeckoClientConfig(
            base_url=source_cfg.coingecko_base_url,
            timeout=source_cfg.timeout,
            user_agent=source_cfg.user_agent,
            api_key=source_cfg.coingecko_api_key,
            api_key_header=source_cfg.coingecko_api_key_header,
        )
        self.coingecko = CoinGeckoSource(cg_cfg)
        self.dex = DexScreenerSource(
            source_cfg.dexscreener_base_url,
            source_cfg.timeout,
            source_cfg.user_agent,
        )
        self._cg_cache = CoinGeckoSearchCache(
            rules.coingecko_cache_path,
            rules.coingecko_cache_ttl_seconds,
        )
        self.last_scan_meta: Dict[str, Any] = {}

    def _build_symbol_map(self) -> Dict[str, Dict[str, Any]]:
        info = self.binance.exchange_info()
        out: Dict[str, Dict[str, Any]] = {}
        for s in info["symbols"]:
            if s.get("status") == "TRADING" and s.get("isSpotTradingAllowed", False):
                out[s["symbol"]] = s
        return out

    def _extract_filters(self, symbol_info: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for f in symbol_info.get("filters", []) or []:
            out[f["filterType"]] = f
        return out

    def _exclude_symbol(self, symbol_info: Dict[str, Any]) -> bool:
        base = symbol_info["baseAsset"]
        quote = symbol_info["quoteAsset"]

        if quote != self.rules.quote_asset:
            return True
        if base in self.rules.stable_assets:
            return True
        for suffix in self.rules.leveraged_suffixes:
            if base.endswith(suffix):
                return True
        return False

    def _resolve_btc_anchor(self, symbol_map: Dict[str, Dict[str, Any]]) -> Optional[str]:
        if self.rules.btc_anchor_symbol:
            s = self.rules.btc_anchor_symbol.upper()
            return s if s in symbol_map else None
        for candidate in (f"BTC{self.rules.quote_asset}", "BTCUSDT", "BTCUSDC"):
            if candidate in symbol_map:
                return candidate
        return None

    def _btc_regime_ok(
        self,
        ticker_by_symbol: Dict[str, Dict[str, Any]],
        symbol_map: Dict[str, Dict[str, Any]],
    ) -> Tuple[bool, str]:
        if not self.rules.btc_regime_enabled:
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
        if chg < self.rules.btc_min_24h_change_pct:
            return (
                False,
                f"btc_risk_off({anchor} 24h={chg:.2f}% < floor {self.rules.btc_min_24h_change_pct}%)",
            )
        return True, f"btc_ok({anchor} 24h={chg:.2f}%)"

    def _listing_days(self, klines: List[List[Any]]) -> float:
        if not klines:
            return 0.0
        first_open_ms = int(klines[0][0])
        now_ms = int(time.time() * 1000)
        return (now_ms - first_open_ms) / (1000 * 60 * 60 * 24)

    def _binance_candidates(self) -> List[Dict[str, Any]]:
        symbol_map = self._build_symbol_map()
        tickers = self.binance.tickers_24h()
        candidates: List[Dict[str, Any]] = []

        for t in tickers:
            symbol = t.get("symbol")
            if not symbol or symbol not in symbol_map:
                continue

            info = symbol_map[symbol]
            if self._exclude_symbol(info):
                continue

            try:
                quote_volume = float(t["quoteVolume"])
                price_change_pct = float(t["priceChangePercent"])
                trade_count = int(t["count"])
                last_price = float(t["lastPrice"])
            except Exception:
                continue

            if quote_volume < self.rules.min_24h_quote_volume:
                continue
            if price_change_pct < self.rules.min_24h_price_change_pct:
                continue
            if price_change_pct > self.rules.max_24h_price_change_pct:
                continue
            if trade_count < self.rules.min_24h_trade_count:
                continue
            if last_price <= 0:
                continue

            filters = self._extract_filters(info)
            lot = filters.get("LOT_SIZE", {})
            notional = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})

            candidates.append(
                {
                    "symbol": symbol,
                    "base_asset": info["baseAsset"],
                    "quote_asset": info["quoteAsset"],
                    "last_price": last_price,
                    "price_change_pct_24h": price_change_pct,
                    "quote_volume_24h": quote_volume,
                    "trade_count_24h": trade_count,
                    "min_qty": float(lot.get("minQty", 0.0) or 0.0),
                    "step_size": float(lot.get("stepSize", 0.0) or 0.0),
                    "min_notional": float(
                        notional.get("minNotional") or notional.get("notional") or 0.0
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
        return candidates[: self.rules.max_candidates_after_binance]

    def _binance_momentum_metrics(self, symbol: str) -> Optional[Dict[str, Any]]:
        kl = self.binance.klines(symbol, self.rules.interval, self.rules.lookback)
        if len(kl) < max(
            self.rules.rv_window + 5,
            self.rules.breakout_window + 5,
            self.rules.atr_window + 2,
        ):
            return None

        opens = [float(x[1]) for x in kl]
        highs = [float(x[2]) for x in kl]
        lows = [float(x[3]) for x in kl]
        closes = [float(x[4]) for x in kl]
        volumes = [float(x[5]) for x in kl]

        age_days = self._listing_days(kl)
        if age_days < self.rules.min_listing_days_binance:
            return None

        close_now = closes[-1]
        close_prev = closes[-2]
        breakout_high = max(highs[-(self.rules.breakout_window + 1) : -1])

        rv_ma = sma(volumes[:-1], self.rules.rv_window)
        rel_volume = (volumes[-1] / rv_ma) if rv_ma and rv_ma > 0 else 0.0

        atr_value = atr(highs, lows, closes, self.rules.atr_window)
        atr_pct_val = (atr_value / close_now * 100.0) if atr_value and close_now > 0 else 0.0

        log_returns: List[float] = []
        for i in range(1, len(closes)):
            if closes[i - 1] > 0 and closes[i] > 0:
                log_returns.append(math.log(closes[i] / closes[i - 1]))
        rv_std = stddev(log_returns, min(24, len(log_returns))) if log_returns else None
        realized_vol_pct = (rv_std * math.sqrt(24) * 100.0) if rv_std is not None else 0.0

        return {
            "listing_days_binance": age_days,
            "rel_volume": rel_volume,
            "breakout_distance_pct": pct_change(breakout_high, close_now),
            "atr_pct": atr_pct_val,
            "realized_vol_pct": realized_vol_pct,
            "momentum_3bars_pct": pct_change(closes[-4], close_now) if len(closes) >= 4 else 0.0,
            "momentum_6bars_pct": pct_change(closes[-7], close_now) if len(closes) >= 7 else 0.0,
            "close_vs_prev_pct": pct_change(close_prev, close_now),
            "candle_body_pct": pct_change(opens[-1], close_now),
        }

    def _detail_to_cg_row(self, detail: Dict[str, Any]) -> Dict[str, Any]:
        md = detail.get("market_data", {}) or {}
        categories = detail.get("categories", []) or []
        coin_id = detail.get("id") or ""
        return {
            "coingecko_id": coin_id,
            "cg_name": detail.get("name"),
            "cg_symbol": str(detail.get("symbol", "")).upper(),
            "market_cap_usd": (md.get("market_cap", {}) or {}).get("usd"),
            "fdv_usd": (md.get("fully_diluted_valuation", {}) or {}).get("usd"),
            "total_volume_usd": (md.get("total_volume", {}) or {}).get("usd"),
            "market_cap_rank": detail.get("market_cap_rank"),
            "categories": categories,
        }

    def _cg_match_coin(self, base_asset: str) -> Optional[Dict[str, Any]]:
        idmap = {k.upper(): v for k, v in (self.rules.coingecko_id_by_base or {}).items()}
        mapped = idmap.get(base_asset.upper())

        if mapped:
            try:
                detail = self.coingecko.coin(mapped)
                return self._detail_to_cg_row(detail)
            except Exception:
                return None

        cached_id = self._cg_cache.get_coin_id(base_asset)
        if cached_id:
            try:
                detail = self.coingecko.coin(cached_id)
                return self._detail_to_cg_row(detail)
            except Exception:
                pass

        try:
            res = self.coingecko.search(base_asset)
            coins = res.get("coins", [])
            if not coins:
                return None

            exact = [c for c in coins if str(c.get("symbol", "")).upper() == base_asset.upper()]
            chosen = exact[0] if exact else coins[0]
            coin_id = chosen["id"]
            self._cg_cache.set_coin_id(base_asset, coin_id)

            detail = self.coingecko.coin(coin_id)
            return self._detail_to_cg_row(detail)
        except Exception:
            return None

    def _category_score(self, categories: List[str]) -> float:
        score = 0.0
        normalized = [c.lower().strip().replace(" ", "-") for c in categories]
        for cat, bonus in self.rules.category_bonus.items():
            if cat in normalized:
                score += bonus
        return score

    def _dex_lookup(self, base_asset: str, quote_asset: str) -> Optional[Dict[str, Any]]:
        queries = [
            f"{base_asset}/{quote_asset}",
            f"{base_asset} {quote_asset}",
            base_asset,
        ]
        best_pair = None
        best_score = -1.0

        for q in queries:
            try:
                res = self.dex.search_pairs(q)
                pairs = res.get("pairs", []) or []
            except Exception:
                continue

            for p in pairs:
                base = ((p.get("baseToken") or {}).get("symbol") or "").upper()
                if base != base_asset.upper():
                    continue

                liquidity_usd = ((p.get("liquidity") or {}).get("usd")) or 0.0
                volume_h24 = ((p.get("volume") or {}).get("h24")) or 0.0
                boosts = ((p.get("boosts") or {}).get("active")) or 0
                sc = float(liquidity_usd) + (float(volume_h24) * 0.2) + (float(boosts) * 10_000)

                if sc > best_score:
                    best_score = sc
                    best_pair = p

        if not best_pair:
            return None

        created_ms = best_pair.get("pairCreatedAt")
        pair_age_days = None
        if created_ms:
            pair_age_days = (int(time.time() * 1000) - int(created_ms)) / (1000 * 60 * 60 * 24)

        return {
            "dex_chain_id": best_pair.get("chainId"),
            "dex_id": best_pair.get("dexId"),
            "dex_pair_url": best_pair.get("url"),
            "dex_price_usd": best_pair.get("priceUsd"),
            "dex_liquidity_usd": ((best_pair.get("liquidity") or {}).get("usd")),
            "dex_volume_h24_usd": ((best_pair.get("volume") or {}).get("h24")),
            "dex_price_change_h24_pct": ((best_pair.get("priceChange") or {}).get("h24")),
            "dex_fdv_usd": best_pair.get("fdv"),
            "dex_market_cap_usd": best_pair.get("marketCap"),
            "dex_pair_age_days": pair_age_days,
            "dex_boosts_active": ((best_pair.get("boosts") or {}).get("active")) or 0,
        }

    def _apply_book_spread(self, row: Dict[str, Any]) -> bool:
        max_sp = self.rules.book_ticker_max_spread_pct
        if max_sp <= 0:
            return True
        try:
            bt = self.binance.book_ticker(row["symbol"])
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

    def _score_row(self, row: Dict[str, Any]) -> float:
        score = 0.0

        score += min(max(row.get("price_change_pct_24h", 0.0), 0.0), 35.0) * 1.15
        score += min(math.log10(max(row.get("quote_volume_24h", 1.0), 1.0)), 9.0) * 6.0
        score += min(row.get("rel_volume", 0.0), 5.0) * 12.0
        score += min(row.get("atr_pct", 0.0), 12.0) * 3.0
        score += min(row.get("realized_vol_pct", 0.0), 40.0) * 0.75
        score += min(max(row.get("momentum_3bars_pct", 0.0), -10.0), 20.0) * 1.8
        score += min(max(row.get("momentum_6bars_pct", 0.0), -15.0), 30.0) * 1.2

        breakout_distance = row.get("breakout_distance_pct", -999.0)
        if breakout_distance >= 0:
            score += max(0.0, 10.0 - breakout_distance) * 2.7
        else:
            score -= abs(breakout_distance) * 1.2

        mc = row.get("market_cap_usd")
        if mc:
            if self.rules.min_market_cap_usd <= mc <= self.rules.max_market_cap_usd:
                score += 16.0
            elif mc < self.rules.min_market_cap_usd:
                score -= 8.0
            else:
                score -= 10.0

            try:
                score += max(0.0, 12.0 - math.log10(max(float(mc), 1.0)))
            except Exception:
                pass

        score += row.get("category_score", 0.0)

        dex_liq = row.get("dex_liquidity_usd")
        if dex_liq:
            score += min(math.log10(max(float(dex_liq), 1.0)), 7.0) * 3.5

        dex_boosts = row.get("dex_boosts_active", 0) or 0
        score += min(float(dex_boosts), 10.0) * 1.0

        age = row.get("listing_days_binance")
        if age and 21 <= age <= 180:
            score += 8.0
        elif age and age < 21:
            score -= 20.0

        pair_age = row.get("dex_pair_age_days")
        if pair_age is not None:
            if 2 <= pair_age <= 90:
                score += 6.0
            elif pair_age < 1:
                score -= 8.0

        return round(score, 2)

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
        prior = load_scan_state(self.rules.persist_path) if self.rules.persist_path else {}

        symbol_map = self._build_symbol_map()
        tickers = self.binance.tickers_24h()
        ticker_by_symbol = {t["symbol"]: t for t in tickers}

        ok, btc_reason = self._btc_regime_ok(ticker_by_symbol, symbol_map)
        self.last_scan_meta["btc_regime"] = btc_reason
        _log.info(
            "[SCAN_MULTI] btc_gate ok=%s detail=%s",
            ok,
            btc_reason,
        )
        if not ok:
            _log.info("[SCAN_MULTI] abort early_return=len0 reason=btc_regime")
            return []

        base_candidates = self._binance_candidates()
        self.last_scan_meta["binance_broad_count"] = len(base_candidates)
        _log.info(
            "[SCAN_MULTI] binance_broad_count=%d (max_after_filter=%d)",
            len(base_candidates),
            self.rules.max_candidates_after_binance,
        )
        results: List[Dict[str, Any]] = []

        for row in base_candidates:
            symbol = row["symbol"]
            base_asset = row["base_asset"]

            try:
                momentum = self._binance_momentum_metrics(symbol)
                if not momentum:
                    continue
                row = {**row, **momentum}
            except Exception as e:
                row["error_binance_momentum"] = str(e)
                continue

            if row["rel_volume"] < self.rules.min_rel_volume:
                continue

            cg = self._cg_match_coin(base_asset)
            if cg:
                row.update(cg)
                row["category_score"] = self._category_score(row.get("categories", []))
            else:
                row["category_score"] = 0.0

            market_cap_usd = row.get("market_cap_usd")
            if market_cap_usd is not None:
                if market_cap_usd > self.rules.max_market_cap_usd:
                    continue

            dex = self._dex_lookup(base_asset, self.rules.quote_asset)
            if dex:
                row.update(dex)

            dex_liquidity = row.get("dex_liquidity_usd")
            if dex_liquidity is not None and dex_liquidity < self.rules.min_dex_liquidity_usd:
                continue

            if not self._apply_book_spread(row):
                continue

            row["moonshot_score"] = self._score_row(row)
            results.append(row)

        results.sort(key=lambda x: x.get("moonshot_score", 0.0), reverse=True)
        top = results[: self.rules.top_n]
        self.last_scan_meta["deep_pass_count"] = len(results)
        self.last_scan_meta["shortlist_count"] = len(top)
        if top:
            self.last_scan_meta["shortlist_head"] = [
                {"symbol": r.get("symbol"), "moonshot_score": r.get("moonshot_score")}
                for r in top[:5]
            ]
        _log.info(
            "[SCAN_MULTI] deep_pass=%d shortlist=%d top_symbols=%s",
            len(results),
            len(top),
            [r.get("symbol") for r in top],
        )
        self._merge_prior_ranks(top, prior)

        if self.rules.persist_path:
            save_scan_state(
                self.rules.persist_path,
                {
                    "version": 2,
                    "scanner": "multi_source_moonshot",
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
