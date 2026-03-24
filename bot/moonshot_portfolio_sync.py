from __future__ import annotations

import copy
import os
import shutil
from typing import Any, Dict, List, Tuple

import yaml

from bot.moonshot_scanner import scan_to_ccxt_symbols


def _moonshot_quote(moonshot_root: Dict[str, Any]) -> str:
    return str(moonshot_root.get("quote_asset") or "USDT").strip().upper()


def _filter_picks_for_quote(
    picks: List[Dict[str, Any]],
    quote_asset: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    q = quote_asset.upper()
    out: List[Dict[str, Any]] = []
    warnings: List[str] = []
    for p in picks:
        pq = str(p.get("quote_asset") or "").strip().upper()
        if pq != q:
            warnings.append(f"skip_pick_quote_mismatch({p.get('symbol')} quote={pq!r} want={q})")
            continue
        out.append(p)
    return out, warnings


def _ccxt_symbol(pick: Dict[str, Any]) -> str | None:
    syms = scan_to_ccxt_symbols([pick])
    return syms[0] if syms else None


def apply_scanner_picks_to_portfolio_doc(
    portfolio_doc: Dict[str, Any],
    picks: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Merge multi-source (or any) scanner picks into config/moonshot_portfolio.yaml structure.

    Configure under ``moonshot.scanner_sync``:

    - ``mode``: ``managed_slots`` (default) or ``append``
    - ``default_target_usdc``: default allocation for new/updated rows (default 10)
    - ``overwrite_target_usdc``: if true, set ``target_usdc`` on managed rows from default (default false)
    - ``max_append``: cap new rows in ``append`` mode (default 12)

    ``managed_slots``: only rows with ``scanner_managed: true`` are updated, in list order.
    Unused slots get ``enabled: false``.

    ``append``: rows with ``added_by_scanner: true`` are disabled if not in this scan; picks not
    already present are appended with ``added_by_scanner: true``. Existing symbols are not duplicated.

    Execution gates (risk, moonshot_checklist) are unchanged — this only edits the portfolio file.
    """
    warnings: List[str] = list()
    out = copy.deepcopy(portfolio_doc)
    moon = out.setdefault("moonshot", {})
    assets: List[Dict[str, Any]] = list(moon.get("assets") or [])
    moon["assets"] = assets

    quote = _moonshot_quote(moon)
    filtered, w = _filter_picks_for_quote(picks, quote)
    warnings.extend(w)

    sync_cfg = moon.get("scanner_sync") or {}
    mode = str(sync_cfg.get("mode") or "managed_slots").strip().lower()
    default_tgt = float(sync_cfg.get("default_target_usdc", 10.0) or 10.0)
    overwrite_target = bool(sync_cfg.get("overwrite_target_usdc", False))
    max_append = int(sync_cfg.get("max_append", 12) or 12)

    if not filtered:
        warnings.append("no_picks_after_quote_filter")
        return out, warnings

    if mode == "managed_slots":
        managed_idx = [i for i, a in enumerate(assets) if bool(a.get("scanner_managed"))]
        if not managed_idx:
            warnings.append(
                "managed_slots_mode_but_no_scanner_managed_assets; add scanner_managed: true to slot rows "
                "or set scanner_sync.mode to append"
            )
            return out, warnings

        for slot, idx in enumerate(managed_idx):
            row = assets[idx]
            if slot < len(filtered):
                pick = filtered[slot]
                sym = _ccxt_symbol(pick)
                if not sym:
                    warnings.append(f"pick_skip_bad_symbol({pick.get('symbol')})")
                    row["enabled"] = False
                    continue
                row["symbol"] = sym
                row["name"] = str(pick.get("cg_name") or pick.get("base_asset") or sym.split("/")[0])
                if overwrite_target or "target_usdc" not in row:
                    row["target_usdc"] = default_tgt
                row["enabled"] = True
            else:
                row["enabled"] = False

    elif mode == "append":
        existing = {str(a.get("symbol", "")).upper() for a in assets}
        new_syms_ordered: List[str] = []
        for pick in filtered:
            sym = _ccxt_symbol(pick)
            if sym:
                new_syms_ordered.append(sym.upper())

        pick_set = set(new_syms_ordered)
        for a in assets:
            if not bool(a.get("added_by_scanner")):
                continue
            su = str(a.get("symbol", "")).upper()
            if su in pick_set:
                a["enabled"] = True
            else:
                a["enabled"] = False

        added = 0
        for pick in filtered:
            if added >= max_append:
                break
            sym = _ccxt_symbol(pick)
            if not sym:
                continue
            if sym.upper() in existing:
                continue
            assets.append(
                {
                    "name": str(pick.get("cg_name") or pick.get("base_asset") or sym.split("/")[0]),
                    "symbol": sym,
                    "target_usdc": default_tgt,
                    "enabled": True,
                    "manual_only": False,
                    "added_by_scanner": True,
                }
            )
            existing.add(sym.upper())
            added += 1

    else:
        warnings.append(f"unknown_scanner_sync_mode({mode!r})")

    return out, warnings


def load_portfolio_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_portfolio_yaml(path: str, doc: Dict[str, Any]) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    text = yaml.dump(
        doc,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def write_portfolio_with_backup(path: str, doc: Dict[str, Any], *, backup: bool = True) -> None:
    if backup and os.path.isfile(path):
        bak = path + ".bak"
        shutil.copy2(path, bak)
    save_portfolio_yaml(path, doc)
