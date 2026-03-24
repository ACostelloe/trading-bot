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
from bot.strategy import generate_signal, Signal
from bot.risk import check_trade_allowed
from bot.execution import check_stop_or_take_profit
from bot.state import load_portfolio, save_portfolio
from bot.logger import get_logger
from bot.alerts import send_telegram
from bot.parameter_manager import apply_approved_parameters, validate_approved_parameters
from bot.live_execution import place_live_market_buy, place_live_market_sell


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


def validate_live_startup(exchange, config: dict, risk_cfg: dict, symbols: list[str], logger) -> None:
    if config["exchange"].get("sandbox", True):
        raise RuntimeError("Refusing live startup while sandbox is enabled")

    exec_cfg = config.get("execution", {})
    if exec_cfg.get("mode") != "live":
        raise RuntimeError("Refusing live startup while execution.mode != live")
    if exec_cfg.get("require_explicit_live_flag", True) and not exec_cfg.get("mode") == "live":
        raise RuntimeError("Live flag not explicitly enabled")

    exchange.load_markets()
    bal = exchange.fetch_balance()
    if not bal:
        raise RuntimeError("Could not fetch live balance")

    free = bal.get("free", {})
    usdt_free = float(free.get("USDT", 0.0) or 0.0)
    buffer = float(exec_cfg.get("stablecoin_cash_buffer_usdt", 0.0))
    if usdt_free < buffer:
        raise RuntimeError(f"USDT free balance below buffer: {usdt_free:.2f} < {buffer:.2f}")

    now = pd.Timestamp.utcnow().tz_localize("UTC") if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow().tz_convert("UTC")
    blocked_symbols = []
    for s in symbols:
        _, blocked = event_controls_for_symbol(s, now, risk_cfg)
        if blocked:
            blocked_symbols.append(s)
    if blocked_symbols:
        raise RuntimeError(f"Symbols currently event-blocked at startup: {blocked_symbols}")

    logger.info("Live startup checks passed. USDT free=%.2f", usdt_free)


def main() -> None:
    raw_config = load_config("config/settings.yaml")
    config = apply_approved_parameters(raw_config)
    logger = get_logger(config["logging"]["file"])
    risk_cfg = load_yaml_if_exists("config/risk_events.yaml")

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

    exchange = build_exchange(config)
    symbols = config["market"]["symbols"]
    timeframe = config["market"]["timeframe"]
    limit = config["market"]["limit"]
    poll_seconds = config["market"]["poll_seconds"]
    state_file = config["state"]["file"]
    fee_rate = float(config.get("backtest", {}).get("fee_rate", 0.001))
    exec_cfg = config.get("execution", {})
    max_errors = int(exec_cfg.get("max_consecutive_api_errors", 5))
    max_trades_per_day = int(exec_cfg.get("max_live_trades_per_day", 1))
    allow_multiple_positions = bool(exec_cfg.get("allow_multiple_positions", False))
    manual_override = str(exec_cfg.get("manual_signal_override", "none")).lower().strip()
    manual_buy_usdt = float(exec_cfg.get("manual_buy_usdt", 0.0) or 0.0)
    stablecoin_buffer = float(exec_cfg.get("stablecoin_cash_buffer_usdt", 0.0) or 0.0)

    validate_live_startup(exchange, config, risk_cfg, symbols, logger)

    portfolio = load_portfolio(
        state_file=state_file,
        starting_balance_usdt=config["risk"]["starting_balance_usdt"],
    )

    logger.info("LIVE bot started for %s on %s", ", ".join(symbols), timeframe)
    consecutive_errors = 0
    trades_today = 0
    trade_day = _as_utc_timestamp(pd.Timestamp.utcnow()).date()

    while True:
        try:
            now_day = _as_utc_timestamp(pd.Timestamp.utcnow()).date()
            if now_day != trade_day:
                trade_day = now_day
                trades_today = 0

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
                            pos = portfolio.get_position(symbol)
                            if pos is None:
                                continue
                            qty = float(exchange.amount_to_precision(symbol, pos.qty))
                            order = place_live_market_sell(exchange, symbol, qty)
                            pnl = portfolio.close_position(symbol, last_price, fee_rate)
                            msg = f"{trigger.upper()} LIVE SELL {symbol} qty={qty} @ {last_price:.2f} | pnl={pnl:.2f}"
                            logger.info("%s | order=%s", msg, order)
                            send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                            save_portfolio(state_file, portfolio)
                            continue

                    signal = generate_signal(df, config, in_position=portfolio.has_position(symbol))
                    if manual_override == "buy":
                        signal = Signal(
                            action="buy",
                            reason="manual_test",
                            price=last_price,
                            stop_loss=last_price * 0.98,
                            take_profit=last_price * 1.02,
                        )
                    elif manual_override == "sell":
                        signal = Signal(
                            action="sell",
                            reason="manual_exit",
                            price=last_price,
                        )
                    logger.info(
                        "Symbol=%s Signal=%s reason=%s price=%s",
                        symbol,
                        signal.action,
                        signal.reason,
                        signal.price,
                    )

                    if signal.action == "buy" and signal.price and signal.stop_loss and signal.take_profit:
                        if trades_today >= max_trades_per_day:
                            logger.info("Trade limit reached for day (%d). Skipping.", max_trades_per_day)
                            continue
                        if (not allow_multiple_positions) and portfolio.open_positions_count() > 0:
                            logger.info("Multiple positions disabled. Existing position open; skipping %s", symbol)
                            continue

                        event_mult, event_block = event_controls_for_symbol(
                            symbol, _as_utc_timestamp(last_row["timestamp"]), risk_cfg
                        )
                        if event_block:
                            logger.info("Buy blocked for %s: event_window_blocked", symbol)
                            continue

                        qty = 0.0
                        # Manual plumbing mode: force a fixed USDT notional buy.
                        if manual_override == "buy" and manual_buy_usdt > 0:
                            spendable_cash = max(0.0, portfolio.available_cash() - stablecoin_buffer)
                            target_notional = min(manual_buy_usdt, spendable_cash)
                            if target_notional <= 0:
                                logger.info(
                                    "Buy blocked for %s: no spendable cash after buffer (cash=%.2f, buffer=%.2f)",
                                    symbol,
                                    portfolio.available_cash(),
                                    stablecoin_buffer,
                                )
                                continue
                            qty = (target_notional / float(signal.price)) * event_mult
                        else:
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
                            if not decision.allowed:
                                logger.info("Buy blocked for %s: %s", symbol, decision.reason)
                                continue
                            qty = decision.qty * event_mult

                        qty = float(exchange.amount_to_precision(symbol, qty))
                        if qty <= 0:
                            logger.info("Buy blocked for %s: precision-rounded qty <= 0", symbol)
                            continue

                        notional = qty * float(signal.price)
                        if exec_cfg.get("live_min_notional_check", True) and notional < config["risk"]["min_order_notional"]:
                            logger.info("Buy blocked for %s: notional %.2f below minimum", symbol, notional)
                            continue

                        logger.info(
                            "LIVE ORDER CHECK | symbol=%s qty=%.6f price=%.2f cash=%.2f",
                            symbol,
                            qty,
                            float(signal.price),
                            portfolio.available_cash(),
                        )
                        if qty <= 0:
                            raise RuntimeError("Invalid quantity")
                        if qty * float(signal.price) > portfolio.available_cash():
                            raise RuntimeError("Order exceeds available cash")

                        if exec_cfg.get("mode") != "live":
                            raise RuntimeError("Refusing to place live order while execution.mode != live")

                        order = place_live_market_buy(exchange, symbol, qty)
                        portfolio.open_position(
                            symbol=symbol,
                            qty=qty,
                            entry_price=float(signal.price),
                            stop_loss=float(signal.stop_loss),
                            take_profit=float(signal.take_profit),
                            fee_rate=fee_rate,
                            entry_time=str(last_row["timestamp"]),
                        )
                        msg = f"LIVE BUY {symbol} qty={qty:.6f} @ {float(signal.price):.2f}"
                        logger.info("%s | order=%s", msg, order)
                        send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                        save_portfolio(state_file, portfolio)
                        trades_today += 1

                    elif signal.action == "sell" and portfolio.has_position(symbol):
                        pos = portfolio.get_position(symbol)
                        if pos is None:
                            continue
                        qty = float(exchange.amount_to_precision(symbol, pos.qty))
                        exit_px = float(signal.price or last_price)
                        order = place_live_market_sell(exchange, symbol, qty)
                        pnl = portfolio.close_position(symbol, exit_px, fee_rate)
                        msg = f"LIVE SELL {symbol} qty={qty:.6f} @ {exit_px:.2f} | pnl={pnl:.2f}"
                        logger.info("%s | order=%s", msg, order)
                        send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                        save_portfolio(state_file, portfolio)

                except Exception as symbol_exc:
                    consecutive_errors += 1
                    logger.exception("Symbol cycle failed for %s: %s", symbol, symbol_exc)
                    send_telegram(
                        f"LIVE bot error for {symbol}: {symbol_exc}",
                        enabled=config["alerts"]["telegram_enabled"],
                    )
                    if consecutive_errors >= max_errors:
                        raise RuntimeError(f"Kill switch: consecutive API/errors reached {consecutive_errors}")

            consecutive_errors = 0
            equity = portfolio.mark_to_market(latest_prices)
            logger.info(
                "LIVE Portfolio Equity=%.2f Cash=%.2f RealizedPnL=%.2f OpenPositions=%d",
                equity,
                portfolio.cash_usdt,
                portfolio.realized_pnl,
                portfolio.open_positions_count(),
            )

        except KeyboardInterrupt:
            logger.info("Live bot stopped manually.")
            break
        except Exception as exc:
            logger.exception("Main LIVE cycle failed: %s", exc)
            send_telegram(f"LIVE bot fatal error: {exc}", enabled=config["alerts"]["telegram_enabled"])
            break

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
