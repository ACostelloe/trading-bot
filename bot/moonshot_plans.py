from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AssetPlan:
    name: str
    symbol: str
    target_usdc: float
    enabled: bool
    manual_only: bool


def parse_asset_plans(cfg: dict) -> list[AssetPlan]:
    plans: list[AssetPlan] = []
    for row in cfg.get("assets", []) or []:
        plans.append(
            AssetPlan(
                name=str(row.get("name", row.get("symbol", "unknown"))),
                symbol=str(row.get("symbol", "")).strip(),
                target_usdc=float(row.get("target_usdc", 0.0) or 0.0),
                enabled=bool(row.get("enabled", True)),
                manual_only=bool(row.get("manual_only", False)),
            )
        )
    return plans
