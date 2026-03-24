from __future__ import annotations

import os
import sys
import math
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
from bot.risk import check_trade_allowed
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


def event_controls_for_symbol(symbol: str, ts: pd.Timestamp, risk_cfg: dict) -> tuple[float, bool]:
    events_root = risk_cfg.get("risk_events", {})
    mult = float(events_root.get("default_size_multiplier", 1.0))
    blocked = False
    for ev in events_root.get("symbols", {}).get(symbol, []):
        try:
            start = pd.Timestamp(ev["start"], tz="UTC")
            end = pd.Timestamp(ev["end"], tz="UTC")
        except Exception:
            continue
        if start <= ts <= end:
            mult = min(mult, float(ev.get("size_multiplier", 1.0)))
            blocked = blocked or bool(ev.get("block_new_entries", False))
    return max(mult, 0.0), blocked


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


def prepare_symbol_data(
    exchange, symbols: list[str], timeframe: str, limit: int, config: dict
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        df = fetch_ohlcv_df(exchange, symbol, timeframe, limit=limit)
        df = add_indicators(df, config).dropna().reset_index(drop=True)
        if len(df) < 100:
            continue
        out[symbol] = df
    return out


def build_master_timeline(data_by_symbol: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    all_times: set[pd.Timestamp] = set()
    for df in data_by_symbol.values():
        for t in df["timestamp"].tolist():
            all_times.add(pd.Timestamp(t))
    return sorted(all_times)


def build_symbol_stats(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "trades",
                "wins",
                "losses",
                "win_rate_pct",
                "total_net_pnl",
                "avg_net_pnl",
                "avg_win",
                "avg_loss",
                "profit_factor",
            ]
        )

    rows = []
    for symbol, group in trades_df.groupby("symbol"):
        wins = group[group["net_pnl"] > 0]
        losses = group[group["net_pnl"] < 0]

        gross_profit = wins["net_pnl"].sum()
        gross_loss = abs(losses["net_pnl"].sum())

        trades = len(group)
        win_count = len(wins)
        loss_count = len(losses)
        win_rate = (win_count / trades) * 100 if trades else 0.0
        avg_win = wins["net_pnl"].mean() if not wins.empty else 0.0
        avg_loss = losses["net_pnl"].mean() if not losses.empty else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (math.inf if gross_profit > 0 else 0.0)

        rows.append(
            {
                "symbol": symbol,
                "trades": trades,
                "wins": win_count,
                "losses": loss_count,
                "win_rate_pct": win_rate,
                "total_net_pnl": group["net_pnl"].sum(),
                "avg_net_pnl": group["net_pnl"].mean(),
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "profit_factor": profit_factor,
            }
        )

    return pd.DataFrame(rows).sort_values("total_net_pnl", ascending=False).reset_index(drop=True)


def build_monthly_stats(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(columns=["month", "trades", "net_pnl"])

    monthly = trades_df.copy()
    monthly["exit_time"] = pd.to_datetime(monthly["exit_time"], utc=True, errors="coerce")
    # YYYY-MM bucket without Period (avoids tz warning on PeriodIndex)
    monthly["month"] = monthly["exit_time"].dt.strftime("%Y-%m")

    out = (
        monthly.groupby("month", as_index=False)
        .agg(
            trades=("net_pnl", "count"),
            net_pnl=("net_pnl", "sum"),
            avg_trade_pnl=("net_pnl", "mean"),
        )
        .sort_values("month")
        .reset_index(drop=True)
    )
    return out


def plot_equity_curve(equity_timestamps: list[pd.Timestamp], equity_curve: list[float], output_path: str) -> None:
    if not equity_curve or not equity_timestamps:
        return

    plot_df = pd.DataFrame(
        {
            "timestamp": equity_timestamps,
            "equity": equity_curve,
        }
    ).dropna()

    if plot_df.empty:
        return

    plt.figure(figsize=(12, 6))
    plt.plot(plot_df["timestamp"], plot_df["equity"])
    plt.title("Portfolio Equity Curve")
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
    symbols = config["market"]["symbols"]
    timeframe = config["market"]["timeframe"]
    limit = config["market"].get("limit", 1000)

    fee_rate = float(config.get("backtest", {}).get("fee_rate", 0.001))
    slippage_rate = float(config.get("backtest", {}).get("slippage_rate", 0.0005))

    data_by_symbol = prepare_symbol_data(exchange, symbols, timeframe, limit, config)
    if not data_by_symbol:
        raise ValueError("No symbols returned enough data for backtesting.")

    timeline = build_master_timeline(data_by_symbol)

    starting_balance = float(config["risk"]["starting_balance_usdt"])
    portfolio = Portfolio(cash_usdt=starting_balance)

    trades: list[dict] = []
    equity_curve: list[float] = []
    equity_timestamps: list[pd.Timestamp] = []

    last_known_prices: dict[str, float] = {}

    for current_time in timeline:
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
                    entry_price = float(signal.price) * (1 + slippage_rate)
                    event_mult, event_block = event_controls_for_symbol(symbol, _as_utc_timestamp(current_time), risk_cfg)
                    if event_block:
                        continue

                    decision = check_trade_allowed(
                        available_cash=portfolio.available_cash(),
                        entry_price=entry_price,
                        stop_price=float(signal.stop_loss),
                        config=config,
                        total_open_positions=portfolio.open_positions_count(),
                        already_in_symbol=portfolio.has_position(symbol),
                        daily_pnl_fraction=portfolio.daily_pnl / max(starting_balance, 1),
                        fee_rate=fee_rate,
                    )

                    if decision.allowed:
                        try:
                            qty = decision.qty * event_mult
                            if qty <= 0 or (qty * entry_price) < config["risk"]["min_order_notional"]:
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

    final_equity = portfolio.cash_usdt
    summary = summarize_trades(
        trades=trades,
        starting_balance=starting_balance,
        final_equity=final_equity,
        equity_curve=equity_curve,
    )

    trades_df = pd.DataFrame(trades)
    summary_df = pd.DataFrame([summary])
    symbol_stats_df = build_symbol_stats(trades_df)
    monthly_stats_df = build_monthly_stats(trades_df)

    out_dir = os.path.dirname(os.path.abspath(__file__))
    trades_csv = os.path.join(out_dir, "backtest_trades.csv")
    summary_csv = os.path.join(out_dir, "backtest_summary.csv")
    symbol_stats_csv = os.path.join(out_dir, "backtest_symbol_stats.csv")
    monthly_stats_csv = os.path.join(out_dir, "backtest_monthly_stats.csv")
    equity_png = os.path.join(out_dir, "backtest_equity_curve.png")

    if not trades_df.empty:
        trades_df.to_csv(trades_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    symbol_stats_df.to_csv(symbol_stats_csv, index=False)
    monthly_stats_df.to_csv(monthly_stats_csv, index=False)

    plot_equity_curve(equity_timestamps, equity_curve, equity_png)

    print("\n=== MULTI-SYMBOL BACKTEST SUMMARY ===")
    for key, value in summary.items():
        if isinstance(value, float):
            if "pct" in key:
                print(f"{key}: {value:.2f}%")
            elif math.isinf(value):
                print(f"{key}: inf")
            else:
                print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")

    if not symbol_stats_df.empty:
        print("\n=== PER-SYMBOL STATS ===")
        print(symbol_stats_df.to_string(index=False))

    if not monthly_stats_df.empty:
        print("\n=== MONTHLY STATS ===")
        print(monthly_stats_df.to_string(index=False))

    print("\nSaved:")
    print(f"- {trades_csv}")
    print(f"- {summary_csv}")
    print(f"- {symbol_stats_csv}")
    print(f"- {monthly_stats_csv}")
    print(f"- {equity_png}")


if __name__ == "__main__":
    main()
