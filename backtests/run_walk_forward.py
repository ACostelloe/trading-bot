from __future__ import annotations

import os
import sys
import math
import itertools
import yaml
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bot.exchange import build_exchange
from bot.market_data import fetch_ohlcv_df
from bot.indicators import add_indicators
from bot.strategy import generate_signal
from bot.entry_gates import evaluate_moonshot_gate_for_trend_entry, evaluate_trend_buy_gates
from bot.portfolio import Portfolio
from bot.parameter_manager import apply_approved_parameters


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_yaml_if_exists(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _as_utc_timestamp(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0

    peak = equity_curve[0]
    max_dd = 0.0

    for equity in equity_curve:
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    return max_dd


def summarize_trades(
    trades: list[dict],
    starting_balance: float,
    final_equity: float,
    equity_curve: list[float],
) -> dict:
    total_trades = len(trades)
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] < 0]

    gross_profit = sum(t["net_pnl"] for t in wins)
    gross_loss = abs(sum(t["net_pnl"] for t in losses))

    win_rate = (len(wins) / total_trades) * 100 if total_trades else 0.0
    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (math.inf if gross_profit > 0 else 0.0)
    expectancy = (sum(t["net_pnl"] for t in trades) / total_trades) if total_trades else 0.0
    net_profit = final_equity - starting_balance
    net_return_pct = (net_profit / starting_balance) * 100 if starting_balance > 0 else 0.0
    max_dd_pct = max_drawdown(equity_curve) * 100

    return {
        "starting_balance": starting_balance,
        "final_equity": final_equity,
        "net_profit": net_profit,
        "net_return_pct": net_return_pct,
        "total_trades": total_trades,
        "win_rate_pct": win_rate,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "max_drawdown_pct": max_dd_pct,
    }


def build_override_config(base_config: dict, params: dict) -> dict:
    cfg = yaml.safe_load(yaml.dump(base_config))
    cfg["strategy"]["ema_fast"] = params["ema_fast"]
    cfg["strategy"]["ema_slow"] = params["ema_slow"]
    cfg["strategy"]["rsi_entry_min"] = params["rsi_entry_min"]
    cfg["strategy"]["stop_atr_multiple"] = params["stop_atr_multiple"]
    return cfg


def prepare_symbol_data(
    exchange, symbols: list[str], timeframe: str, limit: int, config: dict
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        df = fetch_ohlcv_df(exchange, symbol, timeframe, limit=limit)
        df = add_indicators(df, config).dropna().reset_index(drop=True)
        if len(df) >= 100:
            out[symbol] = df
    return out


def build_master_timeline(data_by_symbol: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    all_times: set[pd.Timestamp] = set()
    for df in data_by_symbol.values():
        for t in df["timestamp"].tolist():
            all_times.add(pd.Timestamp(t))
    return sorted(all_times)


def subset_symbol_data(
    full_data_by_symbol: dict[str, pd.DataFrame],
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    config: dict,
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for symbol, df in full_data_by_symbol.items():
        sub = df[(df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)].copy()
        if len(sub) >= 30:
            sub = add_indicators(sub[["timestamp", "open", "high", "low", "close", "volume"]].copy(), config)
            sub = sub.dropna().reset_index(drop=True)
            if len(sub) >= 10:
                out[symbol] = sub
    return out


def run_multi_symbol_backtest_on_data(
    data_by_symbol: dict[str, pd.DataFrame],
    config: dict,
    *,
    exchange=None,
    risk_cfg: dict | None = None,
) -> tuple[list[dict], list[float], list[pd.Timestamp], float]:
    fee_rate = float(config.get("backtest", {}).get("fee_rate", 0.001))
    slippage_rate = float(config.get("backtest", {}).get("slippage_rate", 0.0005))
    starting_balance = float(config["risk"]["starting_balance_usdt"])
    risk_cfg = risk_cfg or {}
    exec_cfg = config.get("execution") or {}
    max_trades_per_day = int(exec_cfg.get("max_live_trades_per_day", 0))
    allow_multiple_positions = bool(exec_cfg.get("allow_multiple_positions", False))

    portfolio = Portfolio(cash_usdt=starting_balance)
    trades: list[dict] = []
    equity_curve: list[float] = []
    equity_timestamps: list[pd.Timestamp] = []
    last_known_prices: dict[str, float] = {}
    trade_counts_by_day: dict = {}

    timeline = build_master_timeline(data_by_symbol)

    for current_time in timeline:
        day_key = pd.Timestamp(current_time).date()
        for symbol, df in data_by_symbol.items():
            matching = df[df["timestamp"] == current_time]
            if matching.empty:
                continue

            idx = int(matching.index[-1])
            if idx < 2:
                continue

            hist = df.iloc[: idx + 1].copy()
            row = hist.iloc[-1]

            close_price = float(row["close"])
            high_price = float(row["high"])
            low_price = float(row["low"])
            last_known_prices[symbol] = close_price

            if portfolio.has_position(symbol):
                pos = portfolio.get_position(symbol)
                assert pos is not None

                exit_reason = None
                exit_price = None

                if low_price <= pos.stop_loss:
                    exit_reason = "stop_loss"
                    exit_price = pos.stop_loss * (1 - slippage_rate)
                elif high_price >= pos.take_profit:
                    exit_reason = "take_profit"
                    exit_price = pos.take_profit * (1 - slippage_rate)
                else:
                    signal = generate_signal(hist, config, in_position=True)
                    if signal.action == "sell":
                        exit_reason = signal.reason
                        basis_price = float(signal.price if signal.price is not None else close_price)
                        exit_price = basis_price * (1 - slippage_rate)

                if exit_reason and exit_price is not None:
                    net_pnl = portfolio.close_position(symbol, exit_price, fee_rate)

                    trades.append(
                        {
                            "symbol": symbol,
                            "entry_time": pos.entry_time,
                            "exit_time": current_time,
                            "entry_price": pos.entry_price,
                            "exit_price": exit_price,
                            "qty": pos.qty,
                            "stop_loss": pos.stop_loss,
                            "take_profit": pos.take_profit,
                            "entry_notional": pos.entry_notional,
                            "entry_fee": pos.entry_fee,
                            "exit_fee": pos.qty * exit_price * fee_rate,
                            "net_pnl": net_pnl,
                            "exit_reason": exit_reason,
                        }
                    )

            if not portfolio.has_position(symbol):
                signal = generate_signal(hist, config, in_position=False)

                if signal.action == "buy" and signal.price and signal.stop_loss and signal.take_profit:
                    trades_today = int(trade_counts_by_day.get(day_key, 0))
                    entry_price = float(signal.price) * (1 + slippage_rate)
                    mkt = (exchange.markets.get(symbol) or {}) if exchange is not None else {}
                    mco = float((((mkt.get("limits") or {}).get("cost") or {}).get("min") or 0.0) or 0.0)
                    market_min_cost = mco if mco > 0 else None

                    pre_gate = evaluate_trend_buy_gates(
                        symbol=symbol,
                        signal_price=entry_price,
                        signal_stop_loss=float(signal.stop_loss),
                        signal_take_profit=float(signal.take_profit),
                        bar_timestamp=_as_utc_timestamp(current_time),
                        risk_cfg=risk_cfg,
                        config=config,
                        available_cash=portfolio.available_cash(),
                        open_positions_count=portfolio.open_positions_count(),
                        already_in_symbol=portfolio.has_position(symbol),
                        daily_pnl=portfolio.daily_pnl,
                        fee_rate=fee_rate,
                        starting_balance=starting_balance,
                        trades_today=trades_today,
                        max_trades_per_day=max_trades_per_day,
                        allow_multiple_positions=allow_multiple_positions,
                        live_min_notional_check=bool(exec_cfg.get("live_min_notional_check", True)),
                        market_min_cost=market_min_cost,
                        spendable_cash_after_buffer=None,
                        manual_buy_mode=False,
                        manual_buy_notional=0.0,
                    )
                    if not pre_gate.allowed:
                        continue

                    notional_pre = pre_gate.qty * entry_price
                    current_equity = portfolio.mark_to_market(last_known_prices)
                    moon_gate = evaluate_moonshot_gate_for_trend_entry(
                        symbol=symbol,
                        entry_notional=notional_pre,
                        current_equity=current_equity,
                        open_positions_count=portfolio.open_positions_count(),
                        config=config,
                        gate_result=pre_gate,
                    )
                    if not moon_gate.allowed:
                        continue

                    try:
                        qty = moon_gate.qty
                        if qty <= 0:
                            continue
                        portfolio.open_position(
                            symbol=symbol,
                            qty=qty,
                            entry_price=entry_price,
                            stop_loss=float(signal.stop_loss),
                            take_profit=float(signal.take_profit),
                            fee_rate=fee_rate,
                            entry_time=str(current_time),
                        )
                        trade_counts_by_day[day_key] = trades_today + 1
                    except ValueError:
                        pass

        equity_curve.append(portfolio.mark_to_market(last_known_prices))
        equity_timestamps.append(current_time)

    for symbol in list(portfolio.positions.keys()):
        pos = portfolio.get_position(symbol)
        assert pos is not None
        last_price = last_known_prices.get(symbol, pos.entry_price) * (1 - slippage_rate)
        net_pnl = portfolio.close_position(symbol, last_price, fee_rate)

        trades.append(
            {
                "symbol": symbol,
                "entry_time": pos.entry_time,
                "exit_time": str(timeline[-1]) if timeline else "",
                "entry_price": pos.entry_price,
                "exit_price": last_price,
                "qty": pos.qty,
                "stop_loss": pos.stop_loss,
                "take_profit": pos.take_profit,
                "entry_notional": pos.entry_notional,
                "entry_fee": pos.entry_fee,
                "exit_fee": pos.qty * last_price * fee_rate,
                "net_pnl": net_pnl,
                "exit_reason": "forced_final_bar_exit",
            }
        )

    return trades, equity_curve, equity_timestamps, portfolio.cash_usdt


def score_summary(summary: dict) -> float:
    """
    Balanced but simple ranking score for in-sample optimization.
    Higher is better.
    """
    profit_factor = summary["profit_factor"] if math.isfinite(summary["profit_factor"]) else 5.0
    return (
        summary["net_return_pct"] * 0.5
        + profit_factor * 10.0
        + summary["expectancy"] * 2.0
        - summary["max_drawdown_pct"] * 0.75
    )


def generate_parameter_grid() -> list[dict]:
    ema_fast_values = [10, 20]
    ema_slow_values = [30, 50]
    rsi_entry_values = [50, 55, 60]
    stop_atr_values = [1.0, 1.5, 2.0]

    grid = []
    for ema_fast, ema_slow, rsi_entry, stop_atr in itertools.product(
        ema_fast_values,
        ema_slow_values,
        rsi_entry_values,
        stop_atr_values,
    ):
        if ema_fast >= ema_slow:
            continue
        grid.append(
            {
                "ema_fast": ema_fast,
                "ema_slow": ema_slow,
                "rsi_entry_min": rsi_entry,
                "stop_atr_multiple": stop_atr,
            }
        )
    return grid


def plot_walk_forward_equity(timestamps: list[pd.Timestamp], equity_curve: list[float], output_path: str) -> None:
    if not timestamps or not equity_curve:
        return

    plot_df = pd.DataFrame({"timestamp": timestamps, "equity": equity_curve}).dropna()
    if plot_df.empty:
        return

    plt.figure(figsize=(12, 6))
    plt.plot(plot_df["timestamp"], plot_df["equity"])
    plt.title("Walk-Forward Out-of-Sample Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity (USDT)")
    plt.xticks(rotation=30)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def main() -> None:
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(_root)

    config = apply_approved_parameters(load_config("config/settings.yaml"))
    risk_cfg = load_yaml_if_exists("config/risk_events.yaml")

    exchange = build_exchange(config)
    exchange.load_markets()
    symbols = config["market"]["symbols"]
    timeframe = config["market"]["timeframe"]

    wf = config.get("walk_forward", {})
    train_bars = int(wf.get("train_bars", 180))
    test_bars = int(wf.get("test_bars", 60))
    step_bars = int(wf.get("step_bars", 60))

    min_bars_needed = train_bars + test_bars + step_bars * 8 + 50
    limit = max(int(config["market"].get("limit", 300)), min_bars_needed)

    base_data_by_symbol = prepare_symbol_data(exchange, symbols, timeframe, limit, config)
    if not base_data_by_symbol:
        raise ValueError("No symbols returned enough data for walk-forward testing.")

    master_timeline = build_master_timeline(base_data_by_symbol)
    if len(master_timeline) < (train_bars + test_bars + 10):
        raise ValueError("Not enough total bars for the requested walk-forward windows.")

    param_grid = generate_parameter_grid()

    out_dir = os.path.dirname(os.path.abspath(__file__))
    wf_csv = os.path.join(out_dir, "walk_forward_windows.csv")
    train_grid_csv = os.path.join(out_dir, "walk_forward_train_grid_scores.csv")
    oos_trades_csv = os.path.join(out_dir, "walk_forward_oos_trades.csv")
    oos_summary_csv = os.path.join(out_dir, "walk_forward_oos_summary.csv")
    oos_equity_png = os.path.join(out_dir, "walk_forward_oos_equity_curve.png")

    wf_rows: list[dict] = []
    train_grid_rows: list[dict] = []
    oos_trades_all: list[dict] = []
    combined_oos_equity_curve: list[float] = []
    combined_oos_timestamps: list[pd.Timestamp] = []

    start_idx = 0
    window_num = 1

    while start_idx + train_bars + test_bars <= len(master_timeline):
        train_start = master_timeline[start_idx]
        train_end = master_timeline[start_idx + train_bars - 1]
        test_start = master_timeline[start_idx + train_bars]
        test_end = master_timeline[start_idx + train_bars + test_bars - 1]

        best_params = None
        best_train_summary = None
        best_score = -float("inf")

        for params in param_grid:
            train_cfg = build_override_config(config, params)
            train_data = subset_symbol_data(base_data_by_symbol, train_start, train_end, train_cfg)
            if not train_data:
                continue

            trades, equity_curve, _, final_equity = run_multi_symbol_backtest_on_data(
                train_data,
                train_cfg,
                exchange=exchange,
                risk_cfg=risk_cfg,
            )
            summary = summarize_trades(
                trades=trades,
                starting_balance=float(train_cfg["risk"]["starting_balance_usdt"]),
                final_equity=final_equity,
                equity_curve=equity_curve,
            )
            score = score_summary(summary)
            train_grid_rows.append(
                {
                    "window": window_num,
                    "train_start": str(train_start),
                    "train_end": str(train_end),
                    "ema_fast": params["ema_fast"],
                    "ema_slow": params["ema_slow"],
                    "rsi_entry_min": params["rsi_entry_min"],
                    "stop_atr_multiple": params["stop_atr_multiple"],
                    "train_score": score,
                    "train_net_return_pct": summary["net_return_pct"],
                    "train_profit_factor": summary["profit_factor"],
                    "train_max_drawdown_pct": summary["max_drawdown_pct"],
                    "train_total_trades": summary["total_trades"],
                }
            )

            if summary["total_trades"] == 0:
                continue

            if score > best_score:
                best_score = score
                best_params = params
                best_train_summary = summary

        if best_params is None or best_train_summary is None:
            start_idx += step_bars
            window_num += 1
            continue

        test_cfg = build_override_config(config, best_params)
        test_data = subset_symbol_data(base_data_by_symbol, test_start, test_end, test_cfg)
        if not test_data:
            start_idx += step_bars
            window_num += 1
            continue

        test_trades, test_equity_curve, test_timestamps, test_final_equity = run_multi_symbol_backtest_on_data(
            test_data,
            test_cfg,
            exchange=exchange,
            risk_cfg=risk_cfg,
        )
        test_summary = summarize_trades(
            trades=test_trades,
            starting_balance=float(test_cfg["risk"]["starting_balance_usdt"]),
            final_equity=test_final_equity,
            equity_curve=test_equity_curve,
        )

        wf_rows.append(
            {
                "window": window_num,
                "train_start": str(train_start),
                "train_end": str(train_end),
                "test_start": str(test_start),
                "test_end": str(test_end),
                "best_ema_fast": best_params["ema_fast"],
                "best_ema_slow": best_params["ema_slow"],
                "best_rsi_entry_min": best_params["rsi_entry_min"],
                "best_stop_atr_multiple": best_params["stop_atr_multiple"],
                "train_net_return_pct": best_train_summary["net_return_pct"],
                "train_profit_factor": best_train_summary["profit_factor"],
                "train_max_drawdown_pct": best_train_summary["max_drawdown_pct"],
                "train_total_trades": best_train_summary["total_trades"],
                "test_net_return_pct": test_summary["net_return_pct"],
                "test_profit_factor": test_summary["profit_factor"],
                "test_max_drawdown_pct": test_summary["max_drawdown_pct"],
                "test_total_trades": test_summary["total_trades"],
            }
        )

        oos_trades_all.extend(test_trades)

        if test_equity_curve and test_timestamps:
            if not combined_oos_equity_curve:
                combined_oos_equity_curve.extend(test_equity_curve)
                combined_oos_timestamps.extend(test_timestamps)
            else:
                last_equity = combined_oos_equity_curve[-1]
                base_equity = test_equity_curve[0]
                adjusted_curve = [last_equity + (eq - base_equity) for eq in test_equity_curve]
                combined_oos_equity_curve.extend(adjusted_curve)
                combined_oos_timestamps.extend(test_timestamps)

        start_idx += step_bars
        window_num += 1

    wf_df = pd.DataFrame(wf_rows)
    train_grid_df = pd.DataFrame(train_grid_rows)
    oos_trades_df = pd.DataFrame(oos_trades_all)

    if not wf_df.empty:
        wf_df.to_csv(wf_csv, index=False)
    if not train_grid_df.empty:
        train_grid_df.to_csv(train_grid_csv, index=False)

    if not oos_trades_df.empty:
        oos_trades_df.to_csv(oos_trades_csv, index=False)

    if combined_oos_equity_curve:
        plot_walk_forward_equity(combined_oos_timestamps, combined_oos_equity_curve, oos_equity_png)

    if combined_oos_equity_curve:
        overall_start = combined_oos_equity_curve[0]
        overall_end = combined_oos_equity_curve[-1]
    else:
        overall_start = float(config["risk"]["starting_balance_usdt"])
        overall_end = overall_start

    oos_summary = summarize_trades(
        trades=oos_trades_all,
        starting_balance=overall_start,
        final_equity=overall_end,
        equity_curve=combined_oos_equity_curve if combined_oos_equity_curve else [overall_start],
    )
    pd.DataFrame([oos_summary]).to_csv(oos_summary_csv, index=False)

    print("\n=== WALK-FORWARD OUT-OF-SAMPLE SUMMARY ===")
    for key, value in oos_summary.items():
        if isinstance(value, float):
            if "pct" in key:
                print(f"{key}: {value:.2f}%")
            elif math.isinf(value):
                print(f"{key}: inf")
            else:
                print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")

    if not wf_df.empty:
        print("\n=== WALK-FORWARD WINDOWS ===")
        print(wf_df.to_string(index=False))

    print("\nSaved:")
    print(f"- {wf_csv}")
    print(f"- {train_grid_csv}")
    print(f"- {oos_trades_csv}")
    print(f"- {oos_summary_csv}")
    print(f"- {oos_equity_png}")


if __name__ == "__main__":
    main()
