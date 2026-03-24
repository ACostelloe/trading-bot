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
from bot.execution import check_stop_or_take_profit
from bot.state import load_portfolio, save_portfolio
from bot.logger import get_logger
from bot.alerts import send_telegram
from bot.parameter_manager import apply_approved_parameters, validate_approved_parameters
from bot.live_execution import place_live_market_buy, place_live_market_sell
from bot.event_controls import event_controls_for_symbol
from bot.entry_gates import (
    effective_min_notional,
    evaluate_moonshot_gate_for_trend_entry,
    evaluate_trend_buy_gates,
)
from bot.kill_switch import ConsecutiveFailureTracker
from bot.quote_context import build_quote_execution_context
from bot.structured_log import (
    EVENT_KILL_SWITCH_TRIPPED,
    EVENT_ORDER_FILLED,
    EVENT_ORDER_SUBMITTED,
    EVENT_POSITION_CLOSED,
    EVENT_POSITION_OPENED,
    EVENT_POSITION_REDUCED,
    EVENT_SIGNAL_GENERATED,
    EVENT_STARTUP_RECONCILIATION_DELTA,
    EVENT_TRADE_BLOCKED,
    emit_event,
    get_structured_logger,
)
from bot.unified_ledger import (
    UnifiedLedger,
    SOURCE_TREND,
    estimate_fee_quote,
    full_reconcile_snapshot,
    log_symbol_truth,
    make_client_order_id,
)


def _extract_fill_details(order: dict | None, fallback_price: float, fallback_qty: float) -> tuple[float, float]:
    o = order or {}
    filled = float(o.get("filled") or 0.0)
    avg = float(o.get("average") or 0.0)

    # Fallback to raw exchange payload fields when normalized fields are absent.
    info = o.get("info") or {}
    if filled <= 0:
        filled = float(info.get("executedQty") or fallback_qty or 0.0)
    if avg <= 0:
        cumm_quote = float(info.get("cummulativeQuoteQty") or 0.0)
        if filled > 0 and cumm_quote > 0:
            avg = cumm_quote / filled
        else:
            avg = fallback_price
    return filled, avg


def _refresh_ledger_exchange(exchange, ledger: UnifiedLedger, symbol: str) -> tuple[float, float]:
    bal = exchange.fetch_balance()
    fr = bal.get("free", {}) or {}
    tot = bal.get("total", {}) or {}
    base = symbol.split("/")[0]
    ex_free = float(fr.get(base, 0.0) or 0.0)
    ex_tot = float(tot.get(base, 0.0) or 0.0)
    ledger.update_exchange_from_balance(symbol, ex_free, ex_tot)
    return ex_free, ex_tot


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


def validate_live_startup(
    exchange,
    config: dict,
    risk_cfg: dict,
    symbols: list[str],
    logger,
    quote_ctx,
) -> None:
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

    free = bal.get("free", {}) or {}
    quote_ctx.validate_live_startup_balances(free)

    now = pd.Timestamp.utcnow().tz_localize("UTC") if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow().tz_convert("UTC")
    blocked_symbols = []
    for s in symbols:
        _, blocked = event_controls_for_symbol(s, now, risk_cfg)
        if blocked:
            blocked_symbols.append(s)
    if blocked_symbols:
        raise RuntimeError(f"Symbols currently event-blocked at startup: {blocked_symbols}")

    logger.info("Live startup checks passed. Quote buffers OK for %s", quote_ctx.startup_min_free_by_asset)


def main() -> None:
    raw_config = load_config("config/settings.yaml")
    config = apply_approved_parameters(raw_config)
    logger = get_logger(config["logging"]["file"])
    risk_cfg = load_yaml_if_exists("config/risk_events.yaml")
    moonshot_y = load_yaml_if_exists("config/moonshot_portfolio.yaml")
    mroot = moonshot_y.get("moonshot", {})
    quote_ctx = build_quote_execution_context(config, mroot)
    structured_logger = get_structured_logger(config)

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

    validate_live_startup(exchange, config, risk_cfg, symbols, logger, quote_ctx)

    portfolio = load_portfolio(
        state_file=state_file,
        starting_balance_usdt=config["risk"]["starting_balance_usdt"],
    )

    ledger_cfg = config.get("ledger") or {}
    ledger_path = ledger_cfg.get("file", "unified_ledger.json")
    if not os.path.isabs(ledger_path):
        ledger_path = os.path.abspath(ledger_path)
    moonshot_syms = [
        str(a["symbol"])
        for a in mroot.get("assets", [])
        if bool(a.get("enabled", True)) and not bool(a.get("manual_only", False))
    ]
    moonshot_state_path = str(mroot.get("state_file", "moonshot_state.json"))
    if not os.path.isabs(moonshot_state_path):
        moonshot_state_path = os.path.abspath(moonshot_state_path)

    ledger = UnifiedLedger.load(ledger_path, default_quote="USDT")
    ledger.path = ledger_path
    trend_prefix = str(ledger_cfg.get("trend_client_order_prefix", "trbot"))
    moonshot_order_prefix = str(mroot.get("client_order_id_prefix", "msbot"))
    strict_led = bool(
        ledger_cfg.get(
            "strict_reconcile_tagged_only",
            mroot.get("reconcile_strict_tagged_only", False),
        )
    )

    recon_deltas: list[dict] = []
    if bool(ledger_cfg.get("reconcile_on_startup", True)):
        full_reconcile_snapshot(
            exchange,
            ledger,
            trend_symbols=list(symbols),
            moonshot_symbols=moonshot_syms,
            trend_prefix=trend_prefix,
            moonshot_prefix=moonshot_order_prefix,
            lookback_days=int(ledger_cfg.get("reconcile_lookback_days", 90)),
            max_fetch_iterations=int(ledger_cfg.get("reconcile_max_fetch_iterations", 40)),
            strict_reconcile=strict_led,
            moonshot_legacy_path=moonshot_state_path if os.path.exists(moonshot_state_path) else None,
            logger=logger,
            reconciliation_deltas=recon_deltas,
        )
    for row in recon_deltas:
        emit_event(
            EVENT_STARTUP_RECONCILIATION_DELTA,
            row,
            structured_logger=structured_logger,
        )

    for sym in symbols:
        pos = portfolio.get_position(sym)
        if pos is None:
            continue
        if ledger.tracked_qty(sym, SOURCE_TREND) > 1e-12:
            continue
        logger.warning(
            "LEDGER: seeding trend slice from state.json for %s (no tagged replay match)",
            sym,
        )
        ledger.apply_buy(
            sym,
            SOURCE_TREND,
            pos.qty,
            pos.entry_price,
            float(getattr(pos, "entry_fee", 0.0) or 0.0),
            stop_loss=float(pos.stop_loss),
            take_profit=float(pos.take_profit),
            append_lot=True,
        )

    ledger.save()
    boot_syms = sorted(set(symbols) | set(moonshot_syms))
    for sym in boot_syms:
        try:
            tick = exchange.fetch_ticker(sym)
            px = float(tick.get("last") or tick.get("close") or 0.0)
            if px > 0:
                log_symbol_truth(logger, ledger, sym, px)
        except Exception as exc:
            logger.debug("LEDGER boot truth skip %s: %s", sym, exc)

    logger.info("LIVE bot started for %s on %s", ", ".join(symbols), timeframe)
    kill_tracker = ConsecutiveFailureTracker(max_errors)
    trades_today = 0
    trade_day = _as_utc_timestamp(pd.Timestamp.utcnow()).date()

    while True:
        try:
            cycle_had_error = False
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
                            _ex_free, ex_tot = _refresh_ledger_exchange(exchange, ledger, symbol)
                            tracked = ledger.tracked_qty(symbol, SOURCE_TREND)
                            raw_qty = min(float(pos.qty), tracked, ex_tot)
                            req_qty = float(exchange.amount_to_precision(symbol, raw_qty))
                            if req_qty <= 0:
                                logger.warning(
                                    "Skip %s exit: tracked=%.8f exch=%.8f pos=%.8f rounded sell=0",
                                    symbol,
                                    tracked,
                                    ex_tot,
                                    pos.qty,
                                )
                                continue
                            quote_ccy = symbol.split("/")[1]
                            cid = make_client_order_id(symbol, trend_prefix)
                            pre_qty = float(pos.qty)
                            emit_event(
                                EVENT_ORDER_SUBMITTED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "side": "sell",
                                    "qty": req_qty,
                                    "client_order_id": cid,
                                    "trigger": trigger,
                                },
                                structured_logger=structured_logger,
                            )
                            order = place_live_market_sell(
                                exchange, symbol, req_qty, {"newClientOrderId": cid}
                            )
                            filled_qty, filled_px = _extract_fill_details(
                                order, fallback_price=last_price, fallback_qty=req_qty
                            )
                            if filled_qty <= 0:
                                raise RuntimeError(f"Sell returned zero filled qty for {symbol}")
                            fee_q = estimate_fee_quote(order, filled_qty, filled_px, quote_ccy, fee_rate)
                            emit_event(
                                EVENT_ORDER_FILLED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "side": "sell",
                                    "filled_qty": filled_qty,
                                    "avg_price": filled_px,
                                    "fee_quote": fee_q,
                                    "order_id": order.get("id"),
                                },
                                structured_logger=structured_logger,
                            )
                            pnl = portfolio.close_position(symbol, filled_px, fee_rate, qty=filled_qty)
                            ledger.apply_sell(symbol, SOURCE_TREND, filled_qty, filled_px, fee_q)
                            _refresh_ledger_exchange(exchange, ledger, symbol)
                            ledger.save()
                            if pre_qty - filled_qty > 1e-8:
                                emit_event(
                                    EVENT_POSITION_REDUCED,
                                    {
                                        "channel": "live",
                                        "symbol": symbol,
                                        "source": "trend",
                                        "sold_qty": filled_qty,
                                        "remaining_qty": pre_qty - filled_qty,
                                        "pnl_quote": pnl,
                                    },
                                    structured_logger=structured_logger,
                                )
                            else:
                                emit_event(
                                    EVENT_POSITION_CLOSED,
                                    {
                                        "channel": "live",
                                        "symbol": symbol,
                                        "source": "trend",
                                        "reason": trigger,
                                        "pnl_quote": pnl,
                                    },
                                    structured_logger=structured_logger,
                                )
                            msg = (
                                f"{trigger.upper()} LIVE SELL {symbol} qty={filled_qty:.6f} "
                                f"@ {filled_px:.2f} | pnl={pnl:.2f}"
                            )
                            logger.info("%s | order=%s", msg, order)
                            log_symbol_truth(logger, ledger, symbol, last_price)
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
                    emit_event(
                        EVENT_SIGNAL_GENERATED,
                        {
                            "channel": "live",
                            "symbol": symbol,
                            "action": signal.action,
                            "reason": signal.reason,
                            "price": signal.price,
                        },
                        structured_logger=structured_logger,
                    )

                    if signal.action == "buy" and signal.price and signal.stop_loss and signal.take_profit:
                        mkt = exchange.markets.get(symbol) or {}
                        mco = float((((mkt.get("limits") or {}).get("cost") or {}).get("min") or 0.0) or 0.0)
                        market_min_cost = mco if mco > 0 else None

                        manual_mode = manual_override == "buy" and manual_buy_usdt > 0
                        spend_after_buf = quote_ctx.spendable_trend_cash(symbol, portfolio.available_cash())
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
                            market_min_cost=market_min_cost,
                            spendable_cash_after_buffer=spend_after_buf,
                            manual_buy_mode=manual_mode,
                            manual_buy_notional=manual_buy_usdt,
                        )
                        if not pre_gate.allowed:
                            logger.info("Buy blocked for %s: %s", symbol, pre_gate.reason)
                            emit_event(
                                EVENT_TRADE_BLOCKED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "side": "buy",
                                    "reason": pre_gate.reason,
                                    "meta": pre_gate.meta,
                                },
                                structured_logger=structured_logger,
                            )
                            continue

                        qty = float(exchange.amount_to_precision(symbol, pre_gate.qty))
                        if qty <= 0:
                            logger.info("Buy blocked for %s: precision-rounded qty <= 0", symbol)
                            emit_event(
                                EVENT_TRADE_BLOCKED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "side": "buy",
                                    "reason": "precision_rounded_qty_zero",
                                    "meta": {},
                                },
                                structured_logger=structured_logger,
                            )
                            continue

                        notional = qty * float(signal.price)
                        eff_floor = effective_min_notional(
                            risk_min=float(config["risk"]["min_order_notional"]),
                            live_min_notional_check=bool(exec_cfg.get("live_min_notional_check", True)),
                            market_min_cost=market_min_cost,
                        )
                        if notional < eff_floor:
                            logger.info(
                                "Buy blocked for %s: rounded notional %.2f below effective min %.2f",
                                symbol,
                                notional,
                                eff_floor,
                            )
                            emit_event(
                                EVENT_TRADE_BLOCKED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "side": "buy",
                                    "reason": "rounded_notional_below_effective_min",
                                    "meta": {"notional": notional, "effective_min": eff_floor},
                                },
                                structured_logger=structured_logger,
                            )
                            continue

                        current_equity = portfolio.mark_to_market(latest_prices)
                        moon_gate = evaluate_moonshot_gate_for_trend_entry(
                            symbol=symbol,
                            entry_notional=notional,
                            current_equity=current_equity,
                            open_positions_count=portfolio.open_positions_count(),
                            config=config,
                            gate_result=pre_gate,
                        )
                        if not moon_gate.allowed:
                            logger.info(
                                "Buy blocked for %s: moonshot_checklist_failed score=%s reasons=%s",
                                symbol,
                                moon_gate.meta.get("moonshot_score"),
                                moon_gate.meta.get("moonshot_reasons"),
                            )
                            emit_event(
                                EVENT_TRADE_BLOCKED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "side": "buy",
                                    "reason": moon_gate.reason,
                                    "meta": moon_gate.meta,
                                },
                                structured_logger=structured_logger,
                            )
                            continue

                        logger.info(
                            "LIVE ORDER CHECK | symbol=%s qty=%.6f price=%.2f cash=%.2f",
                            symbol,
                            qty,
                            float(signal.price),
                            portfolio.available_cash(),
                        )
                        if qty * float(signal.price) > portfolio.available_cash() + 1e-9:
                            raise RuntimeError("Order exceeds available cash")

                        if exec_cfg.get("mode") != "live":
                            raise RuntimeError("Refusing to place live order while execution.mode != live")

                        quote_ccy = symbol.split("/")[1]
                        cid = make_client_order_id(symbol, trend_prefix)
                        emit_event(
                            EVENT_ORDER_SUBMITTED,
                            {
                                "channel": "live",
                                "symbol": symbol,
                                "side": "buy",
                                "qty": qty,
                                "client_order_id": cid,
                            },
                            structured_logger=structured_logger,
                        )
                        order = place_live_market_buy(
                            exchange, symbol, qty, {"newClientOrderId": cid}
                        )
                        filled_qty, filled_px = _extract_fill_details(
                            order, fallback_price=float(signal.price), fallback_qty=qty
                        )
                        if filled_qty <= 0:
                            raise RuntimeError(f"Buy returned zero filled qty for {symbol}")
                        fee_q = estimate_fee_quote(order, filled_qty, filled_px, quote_ccy, fee_rate)
                        emit_event(
                            EVENT_ORDER_FILLED,
                            {
                                "channel": "live",
                                "symbol": symbol,
                                "side": "buy",
                                "filled_qty": filled_qty,
                                "avg_price": filled_px,
                                "fee_quote": fee_q,
                                "order_id": order.get("id"),
                            },
                            structured_logger=structured_logger,
                        )
                        portfolio.open_position(
                            symbol=symbol,
                            qty=filled_qty,
                            entry_price=filled_px,
                            stop_loss=float(signal.stop_loss),
                            take_profit=float(signal.take_profit),
                            fee_rate=fee_rate,
                            entry_time=str(last_row["timestamp"]),
                        )
                        ledger.apply_buy(
                            symbol,
                            SOURCE_TREND,
                            filled_qty,
                            filled_px,
                            fee_q,
                            stop_loss=float(signal.stop_loss),
                            take_profit=float(signal.take_profit),
                            append_lot=True,
                        )
                        _refresh_ledger_exchange(exchange, ledger, symbol)
                        ledger.save()
                        emit_event(
                            EVENT_POSITION_OPENED,
                            {
                                "channel": "live",
                                "symbol": symbol,
                                "source": "trend",
                                "qty": filled_qty,
                                "avg_price": filled_px,
                            },
                            structured_logger=structured_logger,
                        )
                        msg = f"LIVE BUY {symbol} qty={filled_qty:.6f} @ {filled_px:.2f}"
                        logger.info("%s | order=%s", msg, order)
                        log_symbol_truth(logger, ledger, symbol, last_price)
                        send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                        save_portfolio(state_file, portfolio)
                        trades_today += 1

                    elif signal.action == "sell" and portfolio.has_position(symbol):
                        pos = portfolio.get_position(symbol)
                        if pos is None:
                            continue
                        _ex_free, ex_tot = _refresh_ledger_exchange(exchange, ledger, symbol)
                        tracked = ledger.tracked_qty(symbol, SOURCE_TREND)
                        raw_qty = min(float(pos.qty), tracked, ex_tot)
                        req_qty = float(exchange.amount_to_precision(symbol, raw_qty))
                        if req_qty <= 0:
                            logger.warning(
                                "Skip %s signal sell: tracked=%.8f exch=%.8f pos=%.8f",
                                symbol,
                                tracked,
                                ex_tot,
                                pos.qty,
                            )
                            continue
                        quote_ccy = symbol.split("/")[1]
                        exit_px_hint = float(signal.price or last_price)
                        cid = make_client_order_id(symbol, trend_prefix)
                        pre_qty = float(pos.qty)
                        emit_event(
                            EVENT_ORDER_SUBMITTED,
                            {
                                "channel": "live",
                                "symbol": symbol,
                                "side": "sell",
                                "qty": req_qty,
                                "client_order_id": cid,
                                "trigger": "signal_sell",
                            },
                            structured_logger=structured_logger,
                        )
                        order = place_live_market_sell(
                            exchange, symbol, req_qty, {"newClientOrderId": cid}
                        )
                        filled_qty, filled_px = _extract_fill_details(
                            order, fallback_price=exit_px_hint, fallback_qty=req_qty
                        )
                        if filled_qty <= 0:
                            raise RuntimeError(f"Sell returned zero filled qty for {symbol}")
                        fee_q = estimate_fee_quote(order, filled_qty, filled_px, quote_ccy, fee_rate)
                        emit_event(
                            EVENT_ORDER_FILLED,
                            {
                                "channel": "live",
                                "symbol": symbol,
                                "side": "sell",
                                "filled_qty": filled_qty,
                                "avg_price": filled_px,
                                "fee_quote": fee_q,
                                "order_id": order.get("id"),
                            },
                            structured_logger=structured_logger,
                        )
                        pnl = portfolio.close_position(symbol, filled_px, fee_rate, qty=filled_qty)
                        ledger.apply_sell(symbol, SOURCE_TREND, filled_qty, filled_px, fee_q)
                        _refresh_ledger_exchange(exchange, ledger, symbol)
                        ledger.save()
                        if pre_qty - filled_qty > 1e-8:
                            emit_event(
                                EVENT_POSITION_REDUCED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "source": "trend",
                                    "sold_qty": filled_qty,
                                    "remaining_qty": pre_qty - filled_qty,
                                    "pnl_quote": pnl,
                                },
                                structured_logger=structured_logger,
                            )
                        else:
                            emit_event(
                                EVENT_POSITION_CLOSED,
                                {
                                    "channel": "live",
                                    "symbol": symbol,
                                    "source": "trend",
                                    "reason": "signal_sell",
                                    "pnl_quote": pnl,
                                },
                                structured_logger=structured_logger,
                            )
                        msg = f"LIVE SELL {symbol} qty={filled_qty:.6f} @ {filled_px:.2f} | pnl={pnl:.2f}"
                        logger.info("%s | order=%s", msg, order)
                        log_symbol_truth(logger, ledger, symbol, last_price)
                        send_telegram(msg, enabled=config["alerts"]["telegram_enabled"])
                        save_portfolio(state_file, portfolio)

                except Exception as symbol_exc:
                    cycle_had_error = True
                    logger.exception("Symbol cycle failed for %s: %s", symbol, symbol_exc)
                    send_telegram(
                        f"LIVE bot error for {symbol}: {symbol_exc}",
                        enabled=config["alerts"]["telegram_enabled"],
                    )
                    if kill_tracker.record_failure():
                        emit_event(
                            EVENT_KILL_SWITCH_TRIPPED,
                            {
                                "channel": "live",
                                "max_consecutive": max_errors,
                                "last_symbol": symbol,
                                "error": str(symbol_exc),
                            },
                            structured_logger=structured_logger,
                        )
                        raise RuntimeError(
                            f"Kill switch: consecutive API/errors reached {kill_tracker.count}"
                        )

            if not cycle_had_error:
                kill_tracker.record_success()
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
