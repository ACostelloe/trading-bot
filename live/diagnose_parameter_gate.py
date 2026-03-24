"""Print why parameter_selection may block paper/live startup and optional config tweaks."""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import yaml

from bot.parameter_manager import apply_approved_parameters, load_yaml, validate_approved_parameters


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    config_path = os.path.join(root, "config", "settings.yaml")
    raw = load_config(config_path)
    config = apply_approved_parameters(raw)
    sel = raw.get("parameter_selection", {}) or {}

    rel_src = sel.get("source_file", "approved_strategy_params.yaml")
    src_path = rel_src if os.path.isabs(rel_src) else os.path.join(root, rel_src)

    print("=== Parameter gate diagnosis ===\n")
    print(f"settings: {config_path}")
    print(f"parameter_selection.enabled: {sel.get('enabled')}")
    print(f"require_stability_threshold: {sel.get('require_stability_threshold')!r}")
    print(f"min_stability_score: {sel.get('min_stability_score')}")
    print(f"min_combo_win_pct: {sel.get('min_combo_win_pct')}\n")

    if not sel.get("enabled"):
        print("Result: PASS — parameter_selection is disabled.")
        return

    if not os.path.exists(src_path):
        print(f"Result: BLOCK — missing file: {src_path}")
        print("Fix: run backtests/select_approved_parameters.py or create the file.")
        return

    approved = load_yaml(src_path)
    stab = float(approved.get("stability_score", 0.0))
    combo = float(approved.get("combo_win_pct", 0.0))
    print(f"approved: {src_path}")
    print(f"  stability_score: {stab:.6f}")
    print(f"  combo_win_pct: {combo:.4f}%")
    wins = approved.get("combo_wins")
    wins_n = approved.get("combo_windows")
    print(f"  combo_wins / combo_windows: {wins} / {wins_n}\n")

    ok, reason = validate_approved_parameters(config)
    print(f"Result: {'PASS' if ok else 'BLOCK'} - {reason}\n")

    if ok:
        return

    req = sel.get("require_stability_threshold", False)
    if not req:
        print("Note: require_stability_threshold is false; failure is likely a missing-file message above.")
        return

    min_s = float(sel.get("min_stability_score", 0.45))
    min_c = float(sel.get("min_combo_win_pct", 25.0))
    print("Options in config/settings.yaml:\n")
    print("  A) Plumbing only: require_stability_threshold: false")
    print("  B) Match this approved file (lowers the safety bar):")
    print(f"       min_stability_score: {stab:.4f}")
    print(f"       min_combo_win_pct: {combo:.2f}")
    print("  C) Improve research, then re-select: python backtests/run_research_pipeline.py\n")


if __name__ == "__main__":
    main()
