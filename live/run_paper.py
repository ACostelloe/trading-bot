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
from bot.entry_gates import evaluate_moonshot_gate_for_trend_entry, evaluate_trend_buy_gates
from bot.event_controls import event_controls_for_symbol
from bot.execution import handle_paper_buy, handle_paper_sell, check_stop_or_take_profit
from bot.quote_context import build_quote_execution_context
from bot.state import load_portfolio, save_portfolio
from bot.logger import get_logger
from bot.alerts import send_telegram
from bot.parameter_manager import apply_approved_parameters, validate_approved_parameters
from bot.structured_log import (
    EVENT_POSITION_CLOSED,
    EVENT_POSITION_OPENED,
    EVENT_SIGNAL_GENERATED,
    EVENT_TRADE_BLOCKED,
    emit_event,
    get_structured_logger,
)


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
    moonshot_y = load_yaml_if_exists("config/moonshot_portfolio.yaml")
    mroot = moonshot_y.get("moonshot", {})
    quote_ctx = build_quote_execution_context(config, mroot)
    structured_logger = get_structured_logger(config)

    exchange = build_exchange(config)
    symbols = resolve_symbols(config, risk_cfg, logger)
    timeframe = config["market"]["timeframe"]
    limit = config["market"]["limit"]
    poll_seconds = config["market"]["poll_seconds"]
    state_file = config["state"]["file"]
    fee_rate = float(config.get("backtest", {}).get("fee_rate", 0.001))
    exec_cfg = config.get("execution") or {}
    max_trades_per_day = int(exec_cfg.get("max_live_trades_per_day", 0))
    allow_multiple_positions = bool(exec_cfg.get("allow_multiple_positions", False))

    portfolio = load_portfolio(
        state_file=state_file,
        starting_balance_usdt=config["risk"]["starting_balance_usdt"],
    )

    logger.info("Multi-symbol paper bot started for %s on %s", ", ".join(symbols), timeframe)

    trades_today = 0
    trade_day = _as_utc_timestamp(pd.Timestamp.utcnow()).date()

    while True:
        try:
            latest_prices: dict[str, float] = {}
            now_day = _as_utc_timestamp(pd.Timestamp.utcnow()).date()
            if now_day != trade_day:
                trade_day = now_day
                trades_today = 0

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
                            emit_event(
                                EVENT_POSITION_CLOSED,
                                {
                                    "channel": "paper",
                                    "symbol": symbol,
                                    "source": "trend",
                                    "reason": trigger,
                                    "pnl_quote": result["pnl"],
                                },
                                structured_logger=structured_logger,
                            )
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
                    emit_event(
                        EVENT_SIGNAL_GENERATED,
                        {
                            "channel": "paper",
                            "symbol": symbol,
                            "action": signal.action,
                            "reason": signal.reason,
                            "price": signal.price,
                        },
                        structured_logger=structured_logger,
                    )

                    if signal.action == "buy" and signal.price and signal.stop_loss and signal.take_profit:
                        spend_after = quote_ctx.spendable_trend_cash(symbol, portfolio.available_cash())
                        pre_gate = evaluate_trend_buy_gates(
                            symbol=symbol,
                            signal_price=float(signal.price),
                            signal_stop_loss=float(signal.stop_loss),
                            signal_take_profit=float(signal.take_profit),
                            bar_timestamp=_as_utc_timestamp(last_row["timestamp"]),
                            risk_cfg=risk_cfg,
                            config=config,
                            available_cash=portfolio.available_cash(),
                            open_positions_count=portfolio.open_positions_count(),
                            already_in_symbol=portfolio.has_position(symbol),
                            daily_pnl=portfolio.daily_pnl,
                            fee_rate=fee_rate,
                            starting_balance=float(config["risk"]["starting_balance_usdt"]),
                            trades_today=trades_today,
                            max_trades_per_day=max_trades_per_day,
                            allow_multiple_positions=allow_multiple_positions,
                            live_min_notional_check=bool(exec_cfg.get("live_min_notional_check", True)),
                            market_min_cost=None,
                            spendable_cash_after_buffer=spend_after,
                            manual_buy_mode=False,
                            manual_buy_notional=0.0,
                        )
                        if not pre_gate.allowed:
                            logger.info("Buy blocked for %s: %s", symbol, pre_gate.reason)
                            emit_event(
                                EVENT_TRADE_BLOCKED,
                                {
                                    "channel": "paper",
                                    "symbol": symbol,
                                    "side": "buy",
                                    "reason": pre_gate.reason,
                                    "meta": pre_gate.meta,
                                },
                                structured_logger=structured_logger,
                            )
                            continue

                        notional_pre = pre_gate.qty * float(signal.price)
                        current_equity = portfolio.mark_to_market(latest_prices)
                        moon_gate = evaluate_moonshot_gate_for_trend_entry(
                            symbol=symbol,
                            entry_notional=notional_pre,
                            current_equity=current_equity,
                            open_positions_count=portfolio.open_positions_count(),
                            config=config,
                            gate_result=pre_gate,
                        )
                        if not moon_gate.allowed:
                            logger.info("Buy blocked for %s: %s", symbol, moon_gate.reason)
                            emit_event(
                                EVENT_TRADE_BLOCKED,
                                {
                                    "channel": "paper",
                                    "symbol": symbol,
                                    "side": "buy",
                                    "reason": moon_gate.reason,
                                    "meta": moon_gate.meta,
                                },
                                structured_logger=structured_logger,
                            )
                            continue

                        try:
                            qty = moon_gate.qty
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
                            emit_event(
                                EVENT_POSITION_OPENED,
                                {
                                    "channel": "paper",
                                    "symbol": symbol,
                                    "source": "trend",
                                    "qty": result["qty"],
                                    "avg_price": result["price"],
                                },
                                structured_logger=structured_logger,
                            )
                            msg = (
                                f"BUY {symbol} qty={result['qty']:.6f} @ {result['price']:.2f} "
                                f"SL={result['stop_loss']:.2f} TP={result['take_profit']:.2f}"
                            )
                            logger.info(msg)
                            send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                            save_portfolio(state_file, portfolio)
                            trades_today += 1
                        except ValueError as exc:
                            logger.warning("Buy failed for %s: %s", symbol, exc)

                    elif signal.action == "sell" and portfolio.has_position(symbol):
                        result = handle_paper_sell(portfolio, symbol, signal.price or last_price, fee_rate)
                        emit_event(
                            EVENT_POSITION_CLOSED,
                            {
                                "channel": "paper",
                                "symbol": symbol,
                                "source": "trend",
                                "reason": "signal_sell",
                                "pnl_quote": result["pnl"],
                            },
                            structured_logger=structured_logger,
                        )
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
