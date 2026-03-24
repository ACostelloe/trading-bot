"""
Multi-source moonshot scan: Binance (tradability) + CoinGecko + DexScreener.

Run from repo root:

  python research/run_scanner.py

Uses the same pipeline as the live moonshot daemon: ``run_multi_source_picks`` with
``quote_asset`` and ``scanner_automation.rules`` from ``config/moonshot_portfolio.yaml``.
Unspecified rule fields use ``ScannerRules`` defaults in ``multi_source_scanner.py``.

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

import yaml

from bot.moonshot_automation import run_multi_source_picks, scanner_rules_override_from_moonshot_yaml

from research.scoring.multi_source_scanner import scan_to_ccxt_symbols


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    portfolio_path = os.path.join(root, "config", "moonshot_portfolio.yaml")
    moonshot_root: dict = {}
    if os.path.isfile(portfolio_path):
        with open(portfolio_path, encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
        moonshot_root = doc.get("moonshot") or {}
    else:
        print(f"[WARN] Missing {portfolio_path}; using quote_asset=USDT and empty rules overrides.")

    quote_asset = str(moonshot_root.get("quote_asset") or "USDT").upper()
    rules_ov = scanner_rules_override_from_moonshot_yaml(moonshot_root)

    cfg_line = (
        f"Scanner config: quote_asset={quote_asset} "
        f"rules={rules_ov or '(defaults only)'} "
        f"[{portfolio_path}]"
    )
    print(cfg_line, flush=True)
    print(flush=True)

    picks, scan_meta = run_multi_source_picks(root, quote_asset, rules_ov)
    symbols = scan_to_ccxt_symbols(picks)

    print("scan_meta:", scan_meta)
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
