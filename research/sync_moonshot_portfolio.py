"""
Apply multi-source moonshot scan results to ``config/moonshot_portfolio.yaml``.

Uses ``moonshot.quote_asset`` as the scanner quote (must match picks). Execution still requires
``live/run_moonshot.py`` settings (live mode, gates, checklist); this script only updates targets.

managed_slots (default)
  Mark placeholder rows with ``scanner_managed: true``. Each scan refresh fills symbols in file order.

append
  Appends picks as ``added_by_scanner: true`` (up to ``max_append``). Disables prior scanner rows
  not in the latest scan.

  python research/sync_moonshot_portfolio.py
  python research/sync_moonshot_portfolio.py --dry-run
  python research/sync_moonshot_portfolio.py --from-json research/last_multi_source_scan.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

from bot.moonshot_portfolio_sync import (
    apply_scanner_picks_to_portfolio_doc,
    load_portfolio_yaml,
    write_portfolio_with_backup,
)
from bot.moonshot_scanner import load_coingecko_map_yaml

from research.scoring.multi_source_scanner import (
    MultiSourceMoonshotScanner,
    ScannerRules,
    SourceConfig,
)


def _default_paths(root: str) -> tuple[str, str, str, str]:
    portfolio = os.path.join(root, "config", "moonshot_portfolio.yaml")
    gecko_map = os.path.join(root, "config", "scanner_coingecko_map.yaml")
    cache = os.path.join(root, "research", "cache", "coingecko_search_cache.json")
    persist = os.path.join(root, "research", "multi_source_scan_state.json")
    return portfolio, gecko_map, cache, persist


def _run_scan(root: str, moon_quote: str) -> list[dict]:
    _, gecko_path, cache_path, persist_path = _default_paths(root)
    gecko_map = load_coingecko_map_yaml(gecko_path) if os.path.isfile(gecko_path) else {}

    api_key = os.environ.get("COINGECKO_API_KEY") or None
    api_header = os.environ.get("COINGECKO_API_KEY_HEADER") or "x-cg-demo-api-key"

    source_cfg = SourceConfig(
        coingecko_api_key=api_key,
        coingecko_api_key_header=api_header,
    )
    rules = ScannerRules(
        quote_asset=moon_quote,
        coingecko_id_by_base=gecko_map,
        coingecko_cache_path=cache_path,
        coingecko_cache_ttl_seconds=86_400,
        btc_regime_enabled=True,
        btc_min_24h_change_pct=-3.0,
        book_ticker_max_spread_pct=0.35,
        persist_path=persist_path,
    )
    return MultiSourceMoonshotScanner(source_cfg, rules).scan()


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    portfolio_path, _, _, _ = _default_paths(root)

    parser = argparse.ArgumentParser(description="Sync moonshot portfolio YAML from multi-source scan")
    parser.add_argument("--portfolio", default=portfolio_path, help="Path to moonshot_portfolio.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Print diff summary only; do not write")
    parser.add_argument(
        "--from-json",
        default="",
        help="Load picks from JSON list (scanner rows); skip live scan",
    )
    parser.add_argument("--no-backup", action="store_true", help="Do not write .bak before replace")
    args = parser.parse_args()

    doc = load_portfolio_yaml(args.portfolio)
    moon = doc.get("moonshot") or {}
    moon_quote = str(moon.get("quote_asset") or "USDT").strip().upper()

    if args.from_json:
        with open(args.from_json, "r", encoding="utf-8") as f:
            picks = json.load(f)
        if not isinstance(picks, list):
            raise SystemExit("--from-json must contain a JSON array of pick objects")
    else:
        picks = _run_scan(root, moon_quote)

    updated, warnings = apply_scanner_picks_to_portfolio_doc(doc, picks)
    for w in warnings:
        print("warning:", w)

    if args.dry_run:
        import difflib

        old_t = yaml_dump_for_diff(doc)
        new_t = yaml_dump_for_diff(updated)
        print("--- current")
        print("+++ proposed")
        for line in difflib.unified_diff(
            old_t.splitlines(keepends=True),
            new_t.splitlines(keepends=True),
            lineterm="",
        ):
            print(line, end="")
        print("\ndry-run: no file written")
        return

    write_portfolio_with_backup(args.portfolio, updated, backup=not args.no_backup)
    print("wrote", args.portfolio, "(backup unless --no-backup)")


def yaml_dump_for_diff(d: dict) -> str:
    return yaml.dump(
        d,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )


if __name__ == "__main__":
    main()
