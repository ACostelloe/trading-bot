from __future__ import annotations

import math
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


WINDOWS_FILE = "walk_forward_windows.csv"
GRID_FILE = "walk_forward_train_grid_scores.csv"


def load_windows(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("walk_forward_windows.csv is empty.")
    return df


def build_value_frequency_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    mappings = {
        "ema_fast": "best_ema_fast",
        "ema_slow": "best_ema_slow",
        "rsi_entry_min": "best_rsi_entry_min",
        "stop_atr_multiple": "best_stop_atr_multiple",
    }

    out: dict[str, pd.DataFrame] = {}
    for label, col in mappings.items():
        freq = (
            df[col]
            .value_counts(dropna=False)
            .rename_axis("value")
            .reset_index(name="wins")
            .sort_values(["wins", "value"], ascending=[False, True])
            .reset_index(drop=True)
        )
        freq["win_pct"] = (freq["wins"] / len(df)) * 100
        out[label] = freq

    return out


def build_combo_frequency_table(df: pd.DataFrame) -> pd.DataFrame:
    combo_cols = [
        "best_ema_fast",
        "best_ema_slow",
        "best_rsi_entry_min",
        "best_stop_atr_multiple",
    ]
    combo_df = (
        df.groupby(combo_cols, as_index=False)
        .size()
        .rename(columns={"size": "wins"})
        .sort_values(
            ["wins", "best_ema_fast", "best_ema_slow", "best_rsi_entry_min", "best_stop_atr_multiple"],
            ascending=[False, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    combo_df["win_pct"] = (combo_df["wins"] / len(df)) * 100
    return combo_df


def build_oos_performance_by_parameter(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    mappings = {
        "ema_fast": "best_ema_fast",
        "ema_slow": "best_ema_slow",
        "rsi_entry_min": "best_rsi_entry_min",
        "stop_atr_multiple": "best_stop_atr_multiple",
    }

    out: dict[str, pd.DataFrame] = {}
    for label, col in mappings.items():
        perf = (
            df.groupby(col, as_index=False)
            .agg(
                windows=("window", "count"),
                avg_test_net_return_pct=("test_net_return_pct", "mean"),
                median_test_net_return_pct=("test_net_return_pct", "median"),
                avg_test_profit_factor=("test_profit_factor", "mean"),
                avg_test_max_drawdown_pct=("test_max_drawdown_pct", "mean"),
                avg_test_total_trades=("test_total_trades", "mean"),
            )
            .sort_values(col)
            .reset_index(drop=True)
        )
        out[label] = perf

    return out


def build_combo_oos_performance(df: pd.DataFrame) -> pd.DataFrame:
    combo_cols = [
        "best_ema_fast",
        "best_ema_slow",
        "best_rsi_entry_min",
        "best_stop_atr_multiple",
    ]
    combo_perf = (
        df.groupby(combo_cols, as_index=False)
        .agg(
            windows=("window", "count"),
            avg_test_net_return_pct=("test_net_return_pct", "mean"),
            median_test_net_return_pct=("test_net_return_pct", "median"),
            avg_test_profit_factor=("test_profit_factor", "mean"),
            avg_test_max_drawdown_pct=("test_max_drawdown_pct", "mean"),
        )
        .sort_values(["windows", "avg_test_net_return_pct"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return combo_perf


def _adjacent_by_grid_step(
    a: tuple,
    b: tuple,
    level_map: dict[str, list],
) -> bool:
    keys = ["best_ema_fast", "best_ema_slow", "best_rsi_entry_min", "best_stop_atr_multiple"]
    diffs = 0
    for i, k in enumerate(keys):
        levels = level_map[k]
        if a[i] == b[i]:
            continue
        try:
            ia = levels.index(a[i])
            ib = levels.index(b[i])
        except ValueError:
            return False
        if abs(ia - ib) == 1:
            diffs += 1
        else:
            return False
    return diffs == 1


def build_neighborhood_robustness(df: pd.DataFrame, combo_perf_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each winning combo, compare OOS return to immediately adjacent grid neighbors.
    Robustness is higher when neighbors behave similarly.
    """
    combo_cols = [
        "best_ema_fast",
        "best_ema_slow",
        "best_rsi_entry_min",
        "best_stop_atr_multiple",
    ]
    level_map = {c: sorted(df[c].dropna().unique().tolist()) for c in combo_cols}

    key_to_ret = {}
    key_to_windows = {}
    for _, row in combo_perf_df.iterrows():
        key = (
            row["best_ema_fast"],
            row["best_ema_slow"],
            row["best_rsi_entry_min"],
            row["best_stop_atr_multiple"],
        )
        key_to_ret[key] = float(row["avg_test_net_return_pct"])
        key_to_windows[key] = int(row["windows"])

    rows = []
    all_keys = list(key_to_ret.keys())
    for key in all_keys:
        neighbors = [k for k in all_keys if k != key and _adjacent_by_grid_step(key, k, level_map)]
        self_ret = key_to_ret[key]
        if neighbors:
            neigh_rets = [key_to_ret[n] for n in neighbors]
            neigh_avg = float(sum(neigh_rets) / len(neigh_rets))
            abs_gap = abs(self_ret - neigh_avg)
            sign_agreement = float(sum((r >= 0) == (self_ret >= 0) for r in neigh_rets) / len(neigh_rets))
            # 0..1, smaller local gaps and more neighbor support => higher
            local_similarity = 1.0 / (1.0 + abs_gap)
            support = min(1.0, len(neighbors) / 4.0)
            robustness = local_similarity * (0.7 + 0.3 * sign_agreement) * support
        else:
            neigh_avg = math.nan
            abs_gap = math.nan
            sign_agreement = math.nan
            robustness = 0.0

        rows.append(
            {
                "best_ema_fast": key[0],
                "best_ema_slow": key[1],
                "best_rsi_entry_min": key[2],
                "best_stop_atr_multiple": key[3],
                "windows": key_to_windows[key],
                "avg_test_net_return_pct": self_ret,
                "neighbor_count": len(neighbors),
                "neighbor_avg_test_net_return_pct": neigh_avg,
                "neighbor_gap_abs_pct": abs_gap,
                "neighbor_sign_agreement_ratio": sign_agreement,
                "neighborhood_robustness_score_0_to_1": robustness,
            }
        )

    out = pd.DataFrame(rows)
    return out.sort_values(
        ["neighborhood_robustness_score_0_to_1", "windows", "avg_test_net_return_pct"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def shannon_entropy_from_counts(counts: pd.Series) -> float:
    total = counts.sum()
    if total <= 0:
        return 0.0
    probs = counts / total
    return float(-(probs * probs.apply(lambda p: math.log(p) if p > 0 else 0.0)).sum())


def normalized_stability_score(counts: pd.Series) -> float:
    """
    1.0 means one value dominates completely.
    0.0 means wins are spread as evenly as possible.
    """
    if counts.empty:
        return 0.0
    if len(counts) == 1:
        return 1.0

    entropy = shannon_entropy_from_counts(counts)
    max_entropy = math.log(len(counts))
    if max_entropy <= 0:
        return 1.0

    normalized_entropy = entropy / max_entropy
    return 1.0 - normalized_entropy


def build_stability_summary(df: pd.DataFrame) -> pd.DataFrame:
    mappings = {
        "ema_fast": "best_ema_fast",
        "ema_slow": "best_ema_slow",
        "rsi_entry_min": "best_rsi_entry_min",
        "stop_atr_multiple": "best_stop_atr_multiple",
    }

    rows = []
    for label, col in mappings.items():
        counts = df[col].value_counts()
        most_common = counts.index[0]
        most_common_wins = int(counts.iloc[0])
        stability_score = normalized_stability_score(counts)

        rows.append(
            {
                "parameter": label,
                "unique_winning_values": int(counts.shape[0]),
                "most_common_value": most_common,
                "most_common_wins": most_common_wins,
                "most_common_win_pct": (most_common_wins / len(df)) * 100,
                "stability_score_0_to_1": stability_score,
            }
        )

    combo_counts = (
        df.groupby(
            [
                "best_ema_fast",
                "best_ema_slow",
                "best_rsi_entry_min",
                "best_stop_atr_multiple",
            ]
        )
        .size()
        .sort_values(ascending=False)
    )

    top_combo = combo_counts.index[0]
    top_combo_wins = int(combo_counts.iloc[0])
    combo_stability = normalized_stability_score(combo_counts)

    rows.append(
        {
            "parameter": "full_combo",
            "unique_winning_values": int(combo_counts.shape[0]),
            "most_common_value": str(top_combo),
            "most_common_wins": top_combo_wins,
            "most_common_win_pct": (top_combo_wins / len(df)) * 100,
            "stability_score_0_to_1": combo_stability,
        }
    )

    return pd.DataFrame(rows)


def plot_parameter_frequencies(freq_tables: dict[str, pd.DataFrame], out_dir: str) -> None:
    for label, df in freq_tables.items():
        if df.empty:
            continue

        plt.figure(figsize=(8, 4.5))
        plt.bar(df["value"].astype(str), df["wins"])
        plt.title(f"Winning Frequency: {label}")
        plt.xlabel(label)
        plt.ylabel("Winning windows")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f"walk_forward_freq_{label}.png"), dpi=150)
        plt.close()


def build_full_grid_local_robustness(train_grid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-window local robustness around the best train-score combo using the full
    tested parameter grid (not only winning combos).
    """
    if train_grid_df.empty:
        return pd.DataFrame()

    keys = ["ema_fast", "ema_slow", "rsi_entry_min", "stop_atr_multiple"]
    level_map = {k: sorted(train_grid_df[k].dropna().unique().tolist()) for k in keys}

    rows = []
    for window, wdf in train_grid_df.groupby("window"):
        if wdf.empty:
            continue
        best_idx = wdf["train_score"].idxmax()
        best = wdf.loc[best_idx]
        best_key = (best["ema_fast"], best["ema_slow"], best["rsi_entry_min"], best["stop_atr_multiple"])

        neighbors = []
        for _, r in wdf.iterrows():
            key = (r["ema_fast"], r["ema_slow"], r["rsi_entry_min"], r["stop_atr_multiple"])
            if key == best_key:
                continue
            if _adjacent_by_grid_step(best_key, key, {
                "best_ema_fast": level_map["ema_fast"],
                "best_ema_slow": level_map["ema_slow"],
                "best_rsi_entry_min": level_map["rsi_entry_min"],
                "best_stop_atr_multiple": level_map["stop_atr_multiple"],
            }):
                neighbors.append(r)

        if neighbors:
            ndf = pd.DataFrame(neighbors)
            neigh_avg = float(ndf["train_score"].mean())
            abs_gap = abs(float(best["train_score"]) - neigh_avg)
            sign_agreement = float(
                ((ndf["train_score"] >= 0) == (float(best["train_score"]) >= 0)).mean()
            )
            local_similarity = 1.0 / (1.0 + abs_gap)
            support = min(1.0, len(ndf) / 6.0)
            robustness = local_similarity * (0.7 + 0.3 * sign_agreement) * support
        else:
            neigh_avg = math.nan
            abs_gap = math.nan
            sign_agreement = math.nan
            robustness = 0.0

        rows.append(
            {
                "window": int(window),
                "ema_fast": best["ema_fast"],
                "ema_slow": best["ema_slow"],
                "rsi_entry_min": best["rsi_entry_min"],
                "stop_atr_multiple": best["stop_atr_multiple"],
                "train_score": float(best["train_score"]),
                "neighbor_count": int(len(neighbors)),
                "neighbor_avg_train_score": neigh_avg,
                "neighbor_gap_abs": abs_gap,
                "neighbor_sign_agreement_ratio": sign_agreement,
                "full_grid_local_robustness_0_to_1": robustness,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("window").reset_index(drop=True)


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _grade_from_score(score_0_to_1: float) -> str:
    if score_0_to_1 >= 0.75:
        return "A"
    if score_0_to_1 >= 0.60:
        return "B"
    if score_0_to_1 >= 0.45:
        return "C"
    return "D"


def build_strategy_stability_grade(
    windows_df: pd.DataFrame,
    full_grid_local_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Overall grade from:
    - median full-grid local robustness
    - OOS hit rate (positive test windows)
    - OOS drawdown consistency
    - combo churn (how often best combo changes window-to-window)
    """
    if windows_df.empty:
        return pd.DataFrame(
            [
                {
                    "strategy_stability_grade": "D",
                    "overall_score_0_to_1": 0.0,
                    "median_full_grid_local_robustness": 0.0,
                    "oos_hit_rate_positive_windows": 0.0,
                    "oos_drawdown_consistency_score": 0.0,
                    "combo_churn_score_0_to_1": 0.0,
                    "windows_evaluated": 0,
                }
            ]
        )

    if not full_grid_local_df.empty and "full_grid_local_robustness_0_to_1" in full_grid_local_df.columns:
        robust_med = float(full_grid_local_df["full_grid_local_robustness_0_to_1"].median())
    else:
        robust_med = 0.0
    robust_med = _clip01(robust_med)

    hit_rate = float((windows_df["test_net_return_pct"] > 0).mean())
    hit_rate = _clip01(hit_rate)

    dd = windows_df["test_max_drawdown_pct"].astype(float)
    dd_mean = float(dd.mean()) if len(dd) else 0.0
    dd_std = float(dd.std(ddof=0)) if len(dd) else 0.0
    # Lower relative variation => more consistent drawdown profile.
    if dd_mean > 0:
        cv = dd_std / dd_mean
        dd_consistency = 1.0 / (1.0 + cv)
    else:
        # No drawdown observed in test windows -> treat as high consistency.
        dd_consistency = 1.0
    dd_consistency = _clip01(dd_consistency)

    combo_cols = ["best_ema_fast", "best_ema_slow", "best_rsi_entry_min", "best_stop_atr_multiple"]
    ordered = windows_df.sort_values("window").reset_index(drop=True)
    combos = ordered[combo_cols].astype(str).agg("|".join, axis=1)
    if len(combos) <= 1:
        churn_rate = 0.0
    else:
        changes = sum(combos.iloc[i] != combos.iloc[i - 1] for i in range(1, len(combos)))
        churn_rate = changes / (len(combos) - 1)
    churn_score = _clip01(1.0 - churn_rate)

    # Weighted blend; prioritize local robustness and OOS hit consistency.
    overall = (
        0.35 * robust_med
        + 0.30 * hit_rate
        + 0.20 * dd_consistency
        + 0.15 * churn_score
    )
    overall = _clip01(overall)
    grade = _grade_from_score(overall)

    return pd.DataFrame(
        [
            {
                "strategy_stability_grade": grade,
                "overall_score_0_to_1": overall,
                "median_full_grid_local_robustness": robust_med,
                "oos_hit_rate_positive_windows": hit_rate,
                "oos_drawdown_consistency_score": dd_consistency,
                "combo_churn_score_0_to_1": churn_score,
                "windows_evaluated": int(len(windows_df)),
            }
        ]
    )


def main() -> None:
    out_dir = os.path.dirname(os.path.abspath(__file__))
    windows_path = os.path.join(out_dir, WINDOWS_FILE)
    grid_path = os.path.join(out_dir, GRID_FILE)

    df = load_windows(windows_path)

    freq_tables = build_value_frequency_tables(df)
    combo_freq_df = build_combo_frequency_table(df)
    perf_tables = build_oos_performance_by_parameter(df)
    combo_perf_df = build_combo_oos_performance(df)
    neighborhood_df = build_neighborhood_robustness(df, combo_perf_df)
    stability_df = build_stability_summary(df)
    if os.path.exists(grid_path):
        train_grid_df = pd.read_csv(grid_path)
        full_grid_local_df = build_full_grid_local_robustness(train_grid_df)
    else:
        train_grid_df = pd.DataFrame()
        full_grid_local_df = pd.DataFrame()
    grade_df = build_strategy_stability_grade(df, full_grid_local_df)

    for label, table in freq_tables.items():
        table.to_csv(os.path.join(out_dir, f"walk_forward_freq_{label}.csv"), index=False)

    combo_freq_df.to_csv(os.path.join(out_dir, "walk_forward_freq_full_combo.csv"), index=False)

    for label, table in perf_tables.items():
        table.to_csv(os.path.join(out_dir, f"walk_forward_oos_perf_{label}.csv"), index=False)

    combo_perf_df.to_csv(os.path.join(out_dir, "walk_forward_oos_perf_full_combo.csv"), index=False)
    neighborhood_df.to_csv(os.path.join(out_dir, "walk_forward_neighborhood_robustness.csv"), index=False)
    if not full_grid_local_df.empty:
        full_grid_local_df.to_csv(os.path.join(out_dir, "walk_forward_full_grid_local_robustness.csv"), index=False)
    grade_df.to_csv(os.path.join(out_dir, "walk_forward_strategy_grade.csv"), index=False)
    stability_df.to_csv(os.path.join(out_dir, "walk_forward_stability_summary.csv"), index=False)

    plot_parameter_frequencies(freq_tables, out_dir)

    print("\n=== PARAMETER STABILITY SUMMARY ===")
    print(stability_df.to_string(index=False))

    print("\n=== MOST COMMON WINNING COMBINATIONS ===")
    print(combo_freq_df.head(10).to_string(index=False))

    print("\n=== OOS PERFORMANCE BY PARAMETER VALUE ===")
    for label, table in perf_tables.items():
        print(f"\n--- {label} ---")
        print(table.to_string(index=False))

    if not neighborhood_df.empty:
        print("\n=== NEIGHBORHOOD ROBUSTNESS (TOP 10) ===")
        print(neighborhood_df.head(10).to_string(index=False))
    if not full_grid_local_df.empty:
        print("\n=== FULL-GRID LOCAL ROBUSTNESS BY WINDOW ===")
        print(full_grid_local_df.to_string(index=False))
    print("\n=== OVERALL STRATEGY STABILITY GRADE ===")
    print(grade_df.to_string(index=False))

    print("\nSaved:")
    print(f"- {os.path.join(out_dir, 'walk_forward_stability_summary.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_ema_fast.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_ema_slow.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_rsi_entry_min.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_stop_atr_multiple.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_full_combo.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_oos_perf_ema_fast.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_oos_perf_ema_slow.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_oos_perf_rsi_entry_min.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_oos_perf_stop_atr_multiple.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_oos_perf_full_combo.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_neighborhood_robustness.csv')}")
    if not full_grid_local_df.empty:
        print(f"- {os.path.join(out_dir, 'walk_forward_full_grid_local_robustness.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_strategy_grade.csv')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_ema_fast.png')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_ema_slow.png')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_rsi_entry_min.png')}")
    print(f"- {os.path.join(out_dir, 'walk_forward_freq_stop_atr_multiple.png')}")


if __name__ == "__main__":
    main()
