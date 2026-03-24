from __future__ import annotations

import math
import os
import yaml
import pandas as pd


WINDOWS_FILE = "walk_forward_windows.csv"
STABILITY_FILE = "walk_forward_stability_summary.csv"
COMBO_FREQ_FILE = "walk_forward_freq_full_combo.csv"
COMBO_PERF_FILE = "walk_forward_oos_perf_full_combo.csv"
OUTPUT_FILE = "approved_strategy_params.yaml"


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"{path} is empty.")
    return df


def score_combo(row: pd.Series) -> float:
    pf = row.get("avg_test_profit_factor", 0.0)
    if isinstance(pf, float) and math.isinf(pf):
        pf = 5.0

    return (
        float(row.get("avg_test_net_return_pct", 0.0)) * 0.5
        + float(pf) * 10.0
        - float(row.get("avg_test_max_drawdown_pct", 0.0)) * 0.75
        + float(row.get("windows", 0.0)) * 1.5
    )


def main() -> None:
    out_dir = os.path.dirname(os.path.abspath(__file__))
    stability_df = load_csv(os.path.join(out_dir, STABILITY_FILE))
    combo_freq_df = load_csv(os.path.join(out_dir, COMBO_FREQ_FILE))
    combo_perf_df = load_csv(os.path.join(out_dir, COMBO_PERF_FILE))

    full_combo_row = stability_df[stability_df["parameter"] == "full_combo"]
    if full_combo_row.empty:
        raise ValueError("No full_combo row found in stability summary.")
    full_combo_stability = float(full_combo_row.iloc[0]["stability_score_0_to_1"])

    merged = combo_perf_df.merge(
        combo_freq_df,
        on=["best_ema_fast", "best_ema_slow", "best_rsi_entry_min", "best_stop_atr_multiple"],
        how="inner",
    )
    if merged.empty:
        raise ValueError("No overlapping combo frequency/performance data.")

    merged["selection_score"] = merged.apply(score_combo, axis=1)
    merged = merged.sort_values(
        ["selection_score", "wins", "avg_test_net_return_pct"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    best = merged.iloc[0]
    approved = {
        "strategy": {
            "ema_fast": int(best["best_ema_fast"]),
            "ema_slow": int(best["best_ema_slow"]),
            "rsi_entry_min": int(best["best_rsi_entry_min"]),
            "stop_atr_multiple": float(best["best_stop_atr_multiple"]),
        },
        "selection_score": float(best["selection_score"]),
        "combo_win_pct": float(best["win_pct"]),
        "combo_wins": int(best["wins"]),
        "combo_windows": int(best["windows"]),
        "avg_test_net_return_pct": float(best["avg_test_net_return_pct"]),
        "avg_test_profit_factor": float(best["avg_test_profit_factor"]),
        "avg_test_max_drawdown_pct": float(best["avg_test_max_drawdown_pct"]),
        "stability_score": full_combo_stability,
        "source_files": {
            "stability": STABILITY_FILE,
            "combo_frequency": COMBO_FREQ_FILE,
            "combo_performance": COMBO_PERF_FILE,
        },
    }

    output_path = os.path.join(os.path.dirname(out_dir), OUTPUT_FILE)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(approved, f, sort_keys=False)

    print(f"Saved approved parameters to {output_path}")
    print(yaml.safe_dump(approved, sort_keys=False))


if __name__ == "__main__":
    main()
