"""Shared risk event windows: size multiplier and hard blocks on new entries."""

from __future__ import annotations

import pandas as pd


def event_controls_for_symbol(
    symbol: str,
    ts: pd.Timestamp,
    risk_cfg: dict,
) -> tuple[float, bool]:
    events_root = (risk_cfg or {}).get("risk_events", {}) or {}
    mult = float(events_root.get("default_size_multiplier", 1.0))
    blocked = False
    for ev in events_root.get("symbols", {}).get(symbol, []) or []:
        try:
            start = pd.Timestamp(ev["start"], tz="UTC")
            end = pd.Timestamp(ev["end"], tz="UTC")
        except Exception:
            continue
        if start <= ts <= end:
            mult = min(mult, float(ev.get("size_multiplier", 1.0)))
            blocked = blocked or bool(ev.get("block_new_entries", False))
    return max(mult, 0.0), blocked
