#!/usr/bin/env python3
"""
Run the same full ledger reconcile as run_live / run_moonshot startup:
exchange balances, open orders, tagged trade replay into trend/moonshot slices.

IMPORTANT: Stop run_live and run_moonshot (or any process using unified_ledger.json)
before running, so nothing else reads/writes the ledger while this script saves.

Usage (repo root):

  python scripts/full_reconcile_ledger.py

Options:

  --dry-run     Reconcile in memory only; do not write unified_ledger.json
  --strict      Force strict tagged replay (overwrite slices from clientOrderId tags)
  --no-backup   Skip copying unified_ledger.json to .bak.<timestamp> before save
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time

import yaml


def _root_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Full unified ledger reconcile via Binance API.")
    parser.add_argument(
        "--root",
        default=_root_dir(),
        help="Repository root (default: parent of scripts/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write unified_ledger.json after reconcile",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Force strict_reconcile (overwrite slices from msbot/trbot tagged trades only)",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not copy ledger to .bak.<timestamp> before save",
    )
    parser.add_argument(
        "--deltas-json",
        default="",
        help="If set, write reconciliation delta rows to this path (JSON array)",
    )
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    os.chdir(root)
    sys.path.insert(0, root)

    try:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(root, ".env"))
    except ImportError:
        pass

    from bot.exchange import build_exchange
    from bot.moonshot_plans import parse_asset_plans
    from bot.parameter_manager import apply_approved_parameters
    from bot.unified_ledger import UnifiedLedger, full_reconcile_snapshot, symbols_existing_on_exchange

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("full_reconcile")

    settings_path = os.path.join(root, "config", "settings.yaml")
    portfolio_path = os.path.join(root, "config", "moonshot_portfolio.yaml")
    settings = apply_approved_parameters(_load_yaml(settings_path))
    moon_doc = _load_yaml(portfolio_path)
    moon_root = moon_doc.get("moonshot") or {}

    if bool(settings.get("exchange", {}).get("sandbox", True)):
        logger.error("Refusing: exchange.sandbox is true in config/settings.yaml")
        return 2

    exchange = build_exchange(settings)
    exchange.load_markets()

    ledger_cfg = settings.get("ledger") or {}
    ledger_path = ledger_cfg.get("file", "unified_ledger.json")
    if not os.path.isabs(ledger_path):
        ledger_path = os.path.join(root, ledger_path)

    quote_asset = str(moon_root.get("quote_asset", "USDT")).upper()
    ledger = UnifiedLedger.load(ledger_path, default_quote=quote_asset)
    ledger.path = ledger_path

    trend_symbols = list(settings.get("market", {}).get("symbols", []))
    plans = parse_asset_plans(moon_root)
    moonshot_syms = [p.symbol for p in plans if p.enabled and not p.manual_only]
    moonshot_syms, moon_missing = symbols_existing_on_exchange(exchange, moonshot_syms)
    if moon_missing:
        logger.warning("Symbols not on exchange (omitted): %s", moon_missing)

    trend_prefix = str(ledger_cfg.get("trend_client_order_prefix", "trbot"))
    moonshot_prefix = str(moon_root.get("client_order_id_prefix", "msbot"))
    strict_led = bool(
        ledger_cfg.get(
            "strict_reconcile_tagged_only",
            moon_root.get("reconcile_strict_tagged_only", False),
        )
    )
    if args.strict:
        strict_led = True
        logger.warning("Using --strict: slices overwritten from tagged trades only")

    legacy = str(moon_root.get("state_file", "moonshot_state.json"))
    legacy_path = legacy if os.path.isabs(legacy) else os.path.join(root, legacy)

    lookback = int(
        ledger_cfg.get(
            "reconcile_lookback_days",
            moon_root.get("reconcile_lookback_days", 90),
        )
    )
    max_iter = int(
        ledger_cfg.get(
            "reconcile_max_fetch_iterations",
            moon_root.get("reconcile_max_fetch_iterations", 40),
        )
    )

    logger.info(
        "Reconciling ledger=%s trend_syms=%s moonshot_syms=%s lookback_days=%s strict=%s",
        ledger_path,
        trend_symbols,
        moonshot_syms,
        lookback,
        strict_led,
    )

    deltas: list[dict] = []
    full_reconcile_snapshot(
        exchange,
        ledger,
        trend_symbols=trend_symbols,
        moonshot_symbols=moonshot_syms,
        trend_prefix=trend_prefix,
        moonshot_prefix=moonshot_prefix,
        lookback_days=lookback,
        max_fetch_iterations=max_iter,
        strict_reconcile=strict_led,
        moonshot_legacy_path=legacy_path if os.path.isfile(legacy_path) else None,
        logger=logger,
        reconciliation_deltas=deltas,
    )

    logger.info("Reconcile complete; delta rows=%d", len(deltas))
    if args.deltas_json:
        out_p = args.deltas_json if os.path.isabs(args.deltas_json) else os.path.join(root, args.deltas_json)
        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(deltas, f, indent=2)
        logger.info("Wrote deltas to %s", out_p)

    if args.dry_run:
        logger.info("Dry run: not saving ledger")
        return 0

    if not args.no_backup and os.path.isfile(ledger_path):
        bak = f"{ledger_path}.bak.{int(time.time())}"
        shutil.copy2(ledger_path, bak)
        logger.info("Backup: %s", bak)

    ledger.save()
    logger.info("Saved %s", ledger_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
