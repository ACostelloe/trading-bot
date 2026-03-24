from __future__ import annotations

import os
import time
from dataclasses import replace
from typing import Any, Dict, List, Tuple

from bot.moonshot_plans import AssetPlan, parse_asset_plans
from bot.moonshot_portfolio_sync import (
    apply_scanner_picks_to_portfolio_doc,
    load_portfolio_yaml,
    write_portfolio_with_backup,
)
from bot.moonshot_scanner import MoonshotScanner, ScannerConfig
from bot.unified_ledger import SOURCE_MOONSHOT, UnifiedLedger

# Lazy imports for multi-source (heavy deps / research package).
_MS_SCANNER = None
_MS_RULES = None
_MS_SOURCE = None


def _load_multi_source():
    global _MS_SCANNER, _MS_RULES, _MS_SOURCE
    if _MS_SCANNER is None:
        from research.scoring.multi_source_scanner import (
            MultiSourceMoonshotScanner,
            ScannerRules,
            SourceConfig,
        )

        _MS_SCANNER = MultiSourceMoonshotScanner
        _MS_RULES = ScannerRules
        _MS_SOURCE = SourceConfig
    return _MS_SCANNER, _MS_RULES, _MS_SOURCE


def estimate_equity_quote(
    exchange,
    *,
    quote_asset: str,
    free_bal: dict,
    total_bal: dict,
    valuation_symbols: list[str],
    logger,
) -> float:
    """Quote-value equity: free quote + sum(base_total * last) for listed symbols."""
    eq = float(free_bal.get(quote_asset, 0.0) or 0.0)
    for sym in valuation_symbols:
        if "/" not in sym:
            continue
        try:
            base = sym.split("/")[0]
            qty = float(total_bal.get(base, 0.0) or 0.0)
            if qty <= 0:
                continue
            t = exchange.fetch_ticker(sym)
            px = float(t.get("last") or t.get("close") or 0.0)
            if px <= 0:
                continue
            eq += qty * px
        except Exception as exc:
            logger.warning("[MOONSHOT_RUN] equity_skip symbol=%s err=%s", sym, exc)
    return eq


def moonshot_open_positions_count(ledger: UnifiedLedger, symbols: list[str]) -> int:
    n = 0
    for sym in symbols:
        if ledger.slice(sym, SOURCE_MOONSHOT).tracked_qty > 1e-8:
            n += 1
    return n


def _rules_overrides_from_yaml(ov: Any) -> Dict[str, Any]:
    if not isinstance(ov, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in ov.items():
        if k in ("category_bonus", "stable_assets", "leveraged_suffixes", "coingecko_id_by_base"):
            out[k] = v
        elif isinstance(v, (int, float, str, bool)):
            out[k] = v
    return out


def run_multi_source_picks(
    root: str,
    quote_asset: str,
    rules_override: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    try:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(root, ".env"))
    except ImportError:
        pass

    MultiSourceMoonshotScanner, ScannerRules, SourceConfig = _load_multi_source()
    from bot.moonshot_scanner import load_coingecko_map_yaml

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

    base_rules = ScannerRules(
        quote_asset=quote_asset,
        coingecko_id_by_base=gecko_map,
        coingecko_cache_path=cache_path,
        coingecko_cache_ttl_seconds=86_400,
        persist_path=persist_path,
    )
    allowed = {k: v for k, v in rules_override.items() if k in ScannerRules.__dataclass_fields__}
    rules = replace(base_rules, **allowed)
    scanner = MultiSourceMoonshotScanner(source_cfg, rules)
    picks = scanner.scan()
    return picks, scanner.last_scan_meta


def run_binance_only_picks(
    root: str,
    quote_asset: str,
    cfg_override: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    gecko_path = os.path.join(root, "config", "scanner_coingecko_map.yaml")
    from bot.moonshot_scanner import load_coingecko_map_yaml

    gecko_map = load_coingecko_map_yaml(gecko_path) if os.path.isfile(gecko_path) else {}
    persist = os.path.join(root, "research", "moonshot_scan_state.json")
    base = ScannerConfig(
        quote_asset=quote_asset,
        persist_path=persist,
        coingecko_id_by_base=gecko_map,
    )
    allowed = {k: v for k, v in cfg_override.items() if k in ScannerConfig.__dataclass_fields__}
    cfg = replace(base, **allowed)
    scanner = MoonshotScanner(cfg)
    picks = scanner.scan()
    return picks, scanner.last_scan_meta


def refresh_portfolio_from_scanner(
    *,
    root: str,
    portfolio_path: str,
    moonshot_root: Dict[str, Any],
    settings: Dict[str, Any],
    quote_asset: str,
    logger,
) -> Tuple[Dict[str, Any], list[AssetPlan], list[str], Dict[str, Any]]:
    """
    Run configured scanner engine, merge picks into portfolio YAML, reload plans.

    Returns (moonshot_root, plans, moonshot_syms, meta).
    """
    automation = moonshot_root.get("scanner_automation") or {}
    engine = str(automation.get("engine") or "multi_source").strip().lower()
    rules_ov = _rules_overrides_from_yaml(automation.get("rules") or automation.get("scanner_rules"))

    meta: Dict[str, Any] = {"engine": engine, "warnings": []}

    if engine in ("multi", "multi_source", "multisource"):
        picks, scan_meta = run_multi_source_picks(root, quote_asset, rules_ov)
    elif engine in ("binance", "binance_only", "single"):
        picks, scan_meta = run_binance_only_picks(root, quote_asset, rules_ov)
    else:
        logger.error("[MOONSHOT_RUN] scanner_automation unknown engine=%s", engine)
        meta["error"] = f"unknown_engine({engine})"
        plans = parse_asset_plans(moonshot_root)
        syms = [p.symbol for p in plans if p.enabled and not p.manual_only]
        return moonshot_root, plans, syms, meta

    meta["scan"] = scan_meta
    meta["pick_count"] = len(picks)

    doc = load_portfolio_yaml(portfolio_path)
    updated, warnings = apply_scanner_picks_to_portfolio_doc(doc, picks)
    meta["warnings"].extend(warnings)

    if automation.get("write_portfolio_yaml", True):
        write_portfolio_with_backup(portfolio_path, updated, backup=bool(automation.get("backup_yaml", True)))
        logger.info(
            "[MOONSHOT_RUN] scanner_automation wrote portfolio picks=%d engine=%s",
            len(picks),
            engine,
        )
    else:
        logger.info(
            "[MOONSHOT_RUN] scanner_automation dry_merge picks=%d (write_portfolio_yaml=false)",
            len(picks),
        )

    new_moon = updated.get("moonshot") or {}
    plans = parse_asset_plans(new_moon)
    syms = [p.symbol for p in plans if p.enabled and not p.manual_only]
    return new_moon, plans, syms, meta


def maybe_scanner_refresh(
    *,
    root: str,
    portfolio_path: str,
    moonshot_root: Dict[str, Any],
    plans: list[AssetPlan],
    moonshot_syms: list[str],
    settings: Dict[str, Any],
    quote_asset: str,
    logger,
    state: Dict[str, Any],
) -> Tuple[Dict[str, Any], list[AssetPlan], list[str], Dict[str, Any] | None]:
    """
    If scanner_automation.enabled and interval elapsed, refresh portfolio from scan.

    ``state`` must persist ``last_scan_monotonic`` across loop iterations (mutable dict).
    """
    automation = moonshot_root.get("scanner_automation") or {}
    if not bool(automation.get("enabled", False)):
        return moonshot_root, plans, moonshot_syms, None

    interval = int(automation.get("interval_seconds") or 0)
    if interval <= 0:
        logger.warning("[MOONSHOT_RUN] scanner_automation enabled but interval_seconds<=0; skipping")
        return moonshot_root, plans, moonshot_syms, None

    now = time.monotonic()
    last = float(state.get("last_scan_monotonic") or 0.0)
    if last > 0 and (now - last) < interval:
        return moonshot_root, plans, moonshot_syms, None

    logger.info(
        "[MOONSHOT_RUN] scanner_automation tick interval=%ds engine=%s",
        interval,
        automation.get("engine", "multi_source"),
    )
    try:
        prev_syms = set(moonshot_syms)
        new_moon, new_plans, new_syms, meta = refresh_portfolio_from_scanner(
            root=root,
            portfolio_path=portfolio_path,
            moonshot_root=moonshot_root,
            settings=settings,
            quote_asset=quote_asset,
            logger=logger,
        )
        if set(new_syms) != prev_syms:
            logger.warning(
                "[MOONSHOT_RUN] moonshot symbol set changed old=%s new=%s — restart runner to full_reconcile new symbols",
                sorted(prev_syms),
                sorted(new_syms),
            )
        state["last_scan_monotonic"] = now
        return new_moon, new_plans, new_syms, meta
    except Exception as exc:
        logger.exception("[MOONSHOT_RUN] scanner_automation failed: %s", exc)
        state["last_scan_monotonic"] = now
        return moonshot_root, plans, moonshot_syms, {"error": str(exc)}
