from __future__ import annotations

import os
import yaml


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def apply_approved_parameters(config: dict) -> dict:
    selection_cfg = config.get("parameter_selection", {})
    if not selection_cfg.get("enabled", False):
        return config

    source_file = selection_cfg.get("source_file", "approved_strategy_params.yaml")
    if not os.path.exists(source_file):
        return config

    approved = load_yaml(source_file)
    approved_strategy = approved.get("strategy", {})
    if not approved_strategy:
        return config

    merged = yaml.safe_load(yaml.dump(config))
    merged["strategy"].update(approved_strategy)
    return merged


def validate_approved_parameters(config: dict) -> tuple[bool, str]:
    selection_cfg = config.get("parameter_selection", {})
    if not selection_cfg.get("enabled", False):
        return True, "parameter_selection_disabled"

    source_file = selection_cfg.get("source_file", "approved_strategy_params.yaml")
    if not os.path.exists(source_file):
        return False, f"approved parameter file missing: {source_file}"

    approved = load_yaml(source_file)

    if selection_cfg.get("require_stability_threshold", False):
        stability = float(approved.get("stability_score", 0.0))
        combo_win_pct = float(approved.get("combo_win_pct", 0.0))
        min_stability = float(selection_cfg.get("min_stability_score", 0.45))
        min_combo_win_pct = float(selection_cfg.get("min_combo_win_pct", 25.0))

        if stability < min_stability:
            return False, f"stability_score too low: {stability:.3f} < {min_stability:.3f}"

        if combo_win_pct < min_combo_win_pct:
            return False, f"combo_win_pct too low: {combo_win_pct:.2f} < {min_combo_win_pct:.2f}"

    return True, "approved_parameters_valid"
