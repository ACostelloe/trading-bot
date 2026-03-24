from __future__ import annotations

import os
import sys
import time
import yaml
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bot.exchange import build_exchange
from bot.market_data import fetch_ohlcv_df, is_data_fresh
from bot.indicators import add_indicators
from bot.strategy import generate_signal
from bot.risk import check_trade_allowed
from bot.execution import handle_paper_buy, handle_paper_sell, check_stop_or_take_profit
from bot.state import load_portfolio, save_portfolio
from bot.logger import get_logger
from bot.alerts import send_telegram
from bot.parameter_manager import apply_approved_parameters, validate_approved_parameters


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _entry_time_str(ts) -> str:
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


def load_yaml_if_exists(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_symbols(base_config: dict, risk_cfg: dict, logger) -> list[str]:
    market_symbols = list(base_config["market"]["symbols"])
    rot = risk_cfg.get("rotation", {})
    if not rot.get("enabled", False):
        return market_symbols

    base_symbols = rot.get("base_symbols", []) or market_symbols[:3]
    candidates = rot.get("candidate_symbols", []) or [s for s in market_symbols if s not in base_symbols]
    top_n = int(rot.get("top_n_candidates", 1))
    ranking_file = rot.get("ranking_file", "backtests/backtest_symbol_stats.csv")
    metric = rot.get("ranking_metric", "total_net_pnl")

    try:
        df = pd.read_csv(ranking_file)
        if "symbol" not in df.columns or metric not in df.columns:
            raise ValueError(f"ranking file missing columns: symbol/{metric}")
        ranked = (
            df[df["symbol"].isin(candidates)]
            .sort_values(metric, ascending=False)["symbol"]
            .drop_duplicates()
            .tolist()
        )
        selected = ranked[:top_n]
        if not selected:
            selected = candidates[:top_n]
        symbols = list(dict.fromkeys(base_symbols + selected))
        logger.info("Rotation enabled: selected symbols=%s", symbols)
        return symbols
    except Exception as exc:
        logger.warning("Rotation fallback to market.symbols due to: %s", exc)
        return market_symbols


def event_controls_for_symbol(
    symbol: str,
    ts: pd.Timestamp,
    risk_cfg: dict,
) -> tuple[float, bool]:
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


def _as_utc_timestamp(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def main() -> None:
    raw_config = load_config("config/settings.yaml")
    config = apply_approved_parameters(raw_config)
    logger = get_logger(config["logging"]["file"])

    is_valid, reason = validate_approved_parameters(config)
    if not is_valid:
        logger.error("Bot startup blocked: %s", reason)
        raise RuntimeError(f"Bot startup blocked: {reason}")

    logger.info(
        "Using strategy parameters: ema_fast=%s ema_slow=%s rsi_entry_min=%s stop_atr_multiple=%s",
        config["strategy"]["ema_fast"],
        config["strategy"]["ema_slow"],
        config["strategy"]["rsi_entry_min"],
        config["strategy"]["stop_atr_multiple"],
    )
    risk_cfg = load_yaml_if_exists("config/risk_events.yaml")

    exchange = build_exchange(config)
    symbols = resolve_symbols(config, risk_cfg, logger)
    timeframe = config["market"]["timeframe"]
    limit = config["market"]["limit"]
    poll_seconds = config["market"]["poll_seconds"]
    state_file = config["state"]["file"]
    fee_rate = float(config.get("backtest", {}).get("fee_rate", 0.001))

    portfolio = load_portfolio(
        state_file=state_file,
        starting_balance_usdt=config["risk"]["starting_balance_usdt"],
    )

    logger.info("Multi-symbol paper bot started for %s on %s", ", ".join(symbols), timeframe)

    while True:
        try:
            latest_prices: dict[str, float] = {}

            for symbol in symbols:
                try:
                    df = fetch_ohlcv_df(exchange, symbol, timeframe, limit)
                    if not is_data_fresh(df, timeframe):
                        logger.warning("Market data stale for %s. Skipping symbol.", symbol)
                        continue

                    df = add_indicators(df, config)
                    last_row = df.iloc[-1]
                    last_price = float(last_row["close"])
                    latest_prices[symbol] = last_price

                    if portfolio.has_position(symbol):
                        trigger = check_stop_or_take_profit(portfolio, symbol, last_price)
                        if trigger:
                            result = handle_paper_sell(portfolio, symbol, last_price, fee_rate)
                            msg = f"{trigger.upper()} SELL {symbol} @ {last_price:.2f} | pnl={result['pnl']:.2f}"
                            logger.info(msg)
                            send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                            save_portfolio(state_file, portfolio)
                            continue

                    signal = generate_signal(df, config, in_position=portfolio.has_position(symbol))
                    logger.info(
                        "Symbol=%s Signal=%s reason=%s price=%s",
                        symbol,
                        signal.action,
                        signal.reason,
                        signal.price,
                    )

                    if signal.action == "buy" and signal.price and signal.stop_loss and signal.take_profit:
                        event_mult, event_block = event_controls_for_symbol(
                            symbol,
                            _as_utc_timestamp(last_row["timestamp"]),
                            risk_cfg,
                        )
                        if event_block:
                            logger.info("Buy blocked for %s: event_window_blocked", symbol)
                            continue

                        decision = check_trade_allowed(
                            available_cash=portfolio.available_cash(),
                            entry_price=signal.price,
                            stop_price=signal.stop_loss,
                            config=config,
                            total_open_positions=portfolio.open_positions_count(),
                            already_in_symbol=portfolio.has_position(symbol),
                            daily_pnl_fraction=portfolio.daily_pnl
                            / max(config["risk"]["starting_balance_usdt"], 1),
                            fee_rate=fee_rate,
                        )

                        if decision.allowed:
                            try:
                                qty = decision.qty * event_mult
                                if qty <= 0 or (qty * float(signal.price)) < config["risk"]["min_order_notional"]:
                                    logger.info(
                                        "Buy blocked for %s: event_size_multiplier_too_small (mult=%.3f)",
                                        symbol,
                                        event_mult,
                                    )
                                    continue
                                result = handle_paper_buy(
                                    portfolio=portfolio,
                                    symbol=symbol,
                                    qty=qty,
                                    entry_price=signal.price,
                                    stop_loss=signal.stop_loss,
                                    take_profit=signal.take_profit,
                                    fee_rate=fee_rate,
                                    entry_time=_entry_time_str(last_row["timestamp"]),
                                )
                                msg = (
                                    f"BUY {symbol} qty={result['qty']:.6f} @ {result['price']:.2f} "
                                    f"SL={result['stop_loss']:.2f} TP={result['take_profit']:.2f}"
                                )
                                logger.info(msg)
                                send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                                save_portfolio(state_file, portfolio)
                            except ValueError as exc:
                                logger.warning("Buy failed for %s: %s", symbol, exc)
                        else:
                            logger.info("Buy blocked for %s: %s", symbol, decision.reason)

                    elif signal.action == "sell" and portfolio.has_position(symbol):
                        result = handle_paper_sell(portfolio, symbol, signal.price or last_price, fee_rate)
                        msg = f"SELL {symbol} @ {result['price']:.2f} | pnl={result['pnl']:.2f}"
                        logger.info(msg)
                        send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                        save_portfolio(state_file, portfolio)

                except Exception as symbol_exc:
                    logger.exception("Symbol cycle failed for %s: %s", symbol, symbol_exc)
                    send_telegram(
                        f"Bot error for {symbol}: {symbol_exc}",
                        enabled=config["alerts"]["telegram_enabled"],
                    )

            equity = portfolio.mark_to_market(latest_prices)
            logger.info(
                "Portfolio Equity=%.2f Cash=%.2f RealizedPnL=%.2f OpenPositions=%d",
                equity,
                portfolio.cash_usdt,
                portfolio.realized_pnl,
                portfolio.open_positions_count(),
            )

        except KeyboardInterrupt:
            logger.info("Bot stopped manually.")
            break
        except Exception as exc:
            logger.exception("Main cycle failed: %s", exc)
            send_telegram(f"Bot error: {exc}", enabled=config["alerts"]["telegram_enabled"])

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
