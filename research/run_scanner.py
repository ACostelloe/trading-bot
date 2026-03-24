"""
Multi-source moonshot scan: Binance (tradability) + CoinGecko + DexScreener.

Run from repo root:

  python research/run_scanner.py

Optional: copy config/scanner_coingecko_map.example.yaml to config/scanner_coingecko_map.yaml
and set COINGECKO_API_KEY / COINGECKO_API_KEY_HEADER in .env for higher CoinGecko limits.

Candidates are research-only; execution must use separate gates (spread, risk, moonshot_checklist).
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

from bot.moonshot_scanner import load_coingecko_map_yaml

from research.scoring.multi_source_scanner import (
    MultiSourceMoonshotScanner,
    ScannerRules,
    SourceConfig,
    scan_to_ccxt_symbols,
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gecko_path = os.path.join(root, "config", "scanner_coingecko_map.yaml")
    gecko_map = load_coingecko_map_yaml(gecko_path) if os.path.isfile(gecko_path) else {}

    api_key = os.environ.get("COINGECKO_API_KEY") or None
    api_header = os.environ.get("COINGECKO_API_KEY_HEADER") or "x-cg-demo-api-key"

    source_cfg = SourceConfig(
        coingecko_api_key=api_key,
        coingecko_api_key_header=api_header,
    )

    cache_path = os.path.join(root, "research", "cache", "coingecko_search_cache.json")
    persist_path = os.path.join(root, "research", "multi_source_scan_state.json")

    rules = ScannerRules(
        quote_asset="USDC",
        min_24h_quote_volume=1_500_000,
        min_market_cap_usd=15_000_000,
        max_market_cap_usd=1_000_000_000,
        top_n=8,
        coingecko_id_by_base=gecko_map,
        coingecko_cache_path=cache_path,
        coingecko_cache_ttl_seconds=86_400,
        btc_regime_enabled=True,
        btc_min_24h_change_pct=-3.0,
        book_ticker_max_spread_pct=0.35,
        persist_path=persist_path,
    )

    scanner = MultiSourceMoonshotScanner(source_cfg, rules)
    picks = scanner.scan()
    symbols = scan_to_ccxt_symbols(picks)

    print("scan_meta:", scanner.last_scan_meta)
    print("CCXT symbols:", symbols)
    print()
    if not picks:
        print("No multi-source candidates found.")
        return

    for i, p in enumerate(picks, 1):
        d = p.get("rank_delta")
        delta_s = f" Δrank={d:+d}" if d is not None else ""
        sp = p.get("book_spread_pct")
        sp_s = f" spread={sp}%" if sp is not None else ""
        mc = p.get("market_cap_usd")
        mc_s = f" mcap=${mc:,.0f}" if mc else ""
        dex = p.get("dex_liquidity_usd")
        dex_s = f" dexliq=${dex:,.0f}" if dex else ""
        cats = ",".join((p.get("categories") or [])[:2])
        print(
            f"{i:02d}. {p['symbol']:12s} "
            f"score={p.get('moonshot_score', 0):7.2f}{delta_s} "
            f"chg24h={p.get('price_change_pct_24h', 0):6.2f}% "
            f"qv24h={p.get('quote_volume_24h', 0):,.0f} "
            f"rv={p.get('rel_volume', 0):.2f} "
            f"{mc_s}{dex_s} "
            f"cats={cats}"
            f"{sp_s}"
        )


if __name__ == "__main__":
    main()
