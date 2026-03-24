"""
Run the Binance public moonshot scanner from the repo root.

  cd /path/to/trading-bot
  python research/run_moonshot_scan.py

Candidates are for research only; wire into execution only behind your own gates.
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.moonshot_scanner import (
    MoonshotScanner,
    ScannerConfig,
    load_coingecko_map_yaml,
    scan_to_ccxt_symbols,
)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gecko_path = os.path.join(root, "config", "scanner_coingecko_map.yaml")
    gecko_map = load_coingecko_map_yaml(gecko_path) if os.path.isfile(gecko_path) else {}

    cfg = ScannerConfig(
        quote_asset="USDC",
        min_24h_quote_volume=1_500_000,
        min_24h_price_change_pct=4.0,
        max_symbols_after_broad_filter=30,
        top_n=8,
        persist_path=os.path.join(root, "research", "moonshot_scan_state.json"),
        btc_regime_enabled=True,
        btc_min_24h_change_pct=-3.0,
        book_ticker_max_spread_pct=0.35,
        coingecko_id_by_base=gecko_map,
    )
    scanner = MoonshotScanner(cfg)
    picks = scanner.scan()
    symbols = scan_to_ccxt_symbols(picks)

    print("scan_meta:", scanner.last_scan_meta)
    print("CCXT symbols:", symbols)
    print()
    if not picks:
        print("No moonshot candidates found.")
        return
    for i, p in enumerate(picks, 1):
        d = p.get("rank_delta")
        delta_s = f" Δrank={d:+d}" if d is not None else ""
        sp = p.get("book_spread_pct")
        sp_s = f" spread={sp}%" if sp is not None else ""
        mc = p.get("market_cap_usd")
        mc_s = f" mcap=${mc:,.0f}" if mc else ""
        print(
            f"{i:02d}. {p['symbol']:12s} "
            f"score={p['moonshot_score']:6.2f}{delta_s} "
            f"chg24h={p['price_change_pct_24h']:6.2f}% "
            f"qv24h={p['quote_volume_24h']:,.0f} "
            f"rv={p['rel_volume']:.2f} "
            f"atr%={p['atr_pct']:.2f} "
            f"brk%={p['breakout_distance_pct']:.2f} "
            f"age={p['listing_days']:.0f}d "
            f"minNotional={p['min_notional']}"
            f"{sp_s}{mc_s}"
        )


if __name__ == "__main__":
    main()
