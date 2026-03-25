from __future__ import annotations

import os
import sys
import time
from typing import Any

import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bot.entry_gates import moonshot_rebalance_skip_reason
from bot.exchange import build_exchange
from bot.logger import get_logger
from bot.account_equity import estimate_total_account_equity_usdt
from bot.binance_conversion import (
    ensure_binance_spot_before_stable_sell,
    merge_binance_funding_into_free,
)
from bot.moonshot_automation import (
    estimate_equity_quote,
    maybe_scanner_refresh,
    moonshot_open_positions_count,
)
from bot.moonshot_guard import evaluate_moonshot_entry
from bot.moonshot_plans import parse_asset_plans
from bot.quote_context import build_quote_execution_context
from bot.structured_log import (
    EVENT_MOONSHOT_CHECKLIST,
    EVENT_MOONSHOT_ORDER_INTENT,
    EVENT_MOONSHOT_REBALANCE_EVAL,
    EVENT_ORDER_FILLED,
    emit_event,
    get_structured_logger,
)
from bot.unified_ledger import (
    SOURCE_MOONSHOT,
    UnifiedLedger,
    estimate_fee_quote,
    full_reconcile_snapshot,
    make_client_order_id,
    symbols_existing_on_exchange,
)


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def extract_order_fill(order: dict | None, fallback_qty: float, fallback_px: float) -> tuple[float, float]:
    o = order or {}
    filled = float(o.get("filled") or 0.0)
    avg = float(o.get("average") or 0.0)
    info = o.get("info") or {}
    if filled <= 0:
        filled = float(info.get("executedQty") or fallback_qty or 0.0)
    if avg <= 0:
        cumm_quote = float(info.get("cummulativeQuoteQty") or 0.0)
        if filled > 0 and cumm_quote > 0:
            avg = cumm_quote / filled
        else:
            avg = fallback_px
    return filled, avg


def maybe_convert_to_quote(
    *,
    exchange,
    quote_asset: str,
    free_bal: dict,
    needed_quote: float,
    enabled_live_orders: bool,
    logger,
    source_assets: list[str],
    min_conversion_notional: float,
) -> float:
    # Include Binance funding-wallet stables in sizing (spot-only fetch misses them).
    free_bal = merge_binance_funding_into_free(exchange, free_bal, logger)
    current_quote_free = float(free_bal.get(quote_asset, 0.0) or 0.0)
    if needed_quote <= current_quote_free:
        return current_quote_free

    deficit = needed_quote - current_quote_free

    for src in source_assets:
        src = str(src).upper()
        if src == quote_asset:
            continue
        pair = f"{src}/{quote_asset}"
        market = exchange.markets.get(pair)
        if not market or not bool(market.get("active", True)):
            continue

        src_free = float(free_bal.get(src, 0.0) or 0.0)
        if src_free <= 0:
            continue

        ticker = exchange.fetch_ticker(pair)
        px = float(ticker.get("last") or ticker.get("close") or 0.0)
        if px <= 0:
            continue

        max_quote_from_src = src_free * px
        if max_quote_from_src <= 0:
            continue

        convert_quote = min(deficit, max_quote_from_src)
        if convert_quote <= 0:
            continue
        # Exchange min notional: sell at least min_conversion_notional when we have room.
        if max_quote_from_src < min_conversion_notional:
            continue
        if convert_quote < min_conversion_notional:
            convert_quote = min(min_conversion_notional, max_quote_from_src)

        sell_qty = convert_quote / px
        sell_qty = float(exchange.amount_to_precision(pair, sell_qty))
        if sell_qty <= 0:
            continue

        if enabled_live_orders:
            ensure_binance_spot_before_stable_sell(exchange, src, sell_qty, logger)
            order = exchange.create_market_sell_order(pair, sell_qty)
            logger.info(
                "AUTO CONVERT %s->%s qty=%.8f est_quote=%.2f | order_id=%s",
                src,
                quote_asset,
                sell_qty,
                convert_quote,
                order.get("id"),
            )
        else:
            logger.info(
                "DRY RUN AUTO CONVERT %s->%s qty=%.8f est_quote=%.2f",
                src,
                quote_asset,
                sell_qty,
                convert_quote,
            )

        # Re-fetch balance after conversion (or dry-run estimate).
        if enabled_live_orders:
            fresh = exchange.fetch_balance().get("free", {}) or {}
            return float(fresh.get(quote_asset, 0.0) or 0.0)
        return current_quote_free + convert_quote

    return current_quote_free


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    settings = load_yaml("config/settings.yaml")
    portfolio_path = os.path.join(root, "config", "moonshot_portfolio.yaml")
    moonshot_root = load_yaml(portfolio_path).get("moonshot", {})
    quote_ctx = build_quote_execution_context(settings, moonshot_root)
    logger = get_logger(settings["logging"]["file"])
    structured = get_structured_logger(settings)

    if settings["exchange"].get("sandbox", True):
        raise RuntimeError("Refusing moonshot runner while sandbox is enabled")

    exec_cfg = settings.get("execution", {})
    runner_cfg = moonshot_root.get("runner") or {}
    if exec_cfg.get("mode") != "live":
        if not bool(runner_cfg.get("allow_when_settings_execution_not_live", False)):
            raise RuntimeError(
                "Refusing moonshot runner while execution.mode != live "
                "(set moonshot.runner.allow_when_settings_execution_not_live: true in moonshot_portfolio.yaml to override)"
            )
        logger.warning(
            "[MOONSHOT_RUN] execution.mode=%s (not live); runner override enabled — use enabled_live_orders: false for safety",
            exec_cfg.get("mode"),
        )

    enabled_live_orders = bool(moonshot_root.get("enabled_live_orders", False))
    poll_seconds = int(moonshot_root.get("poll_seconds", 60))
    quote_asset = str(quote_ctx.moonshot_quote_asset or moonshot_root.get("quote_asset", "USDC")).upper()
    stablecoin_buffer = float(quote_ctx.moonshot_spend_buffer_quote)
    min_order_notional = float(moonshot_root.get("min_order_notional", 10.0))
    auto_convert_to_quote = bool(quote_ctx.conversion.enabled)
    conversion_source_assets = list(quote_ctx.conversion.source_assets)
    min_conversion_notional = float(quote_ctx.conversion.min_conversion_notional)
    rebalance_tol = float(moonshot_root.get("rebalance_tolerance_pct", 5.0)) / 100.0
    take_profit_mult = 1.0 + (float(moonshot_root.get("take_profit_pct", 100.0)) / 100.0)
    tp_sell_fraction = float(moonshot_root.get("take_profit_sell_fraction", 0.30))
    stop_loss_mult = 1.0 - (float(moonshot_root.get("stop_loss_pct", 18.0)) / 100.0)
    legacy_moonshot_state = str(moonshot_root.get("state_file", "moonshot_state.json"))
    client_order_prefix = str(moonshot_root.get("client_order_id_prefix", "msbot"))
    fee_rate = float(settings.get("backtest", {}).get("fee_rate", 0.001))

    plans = parse_asset_plans(moonshot_root)
    if not plans:
        raise RuntimeError("No assets configured in config/moonshot_portfolio.yaml")

    exchange = build_exchange(settings)
    exchange.load_markets()

    ledger_cfg = settings.get("ledger") or {}
    ledger_path = ledger_cfg.get("file", "unified_ledger.json")
    if not os.path.isabs(ledger_path):
        ledger_path = os.path.join(root, ledger_path)
    ledger = UnifiedLedger.load(ledger_path, default_quote=quote_asset)
    ledger.path = ledger_path
    trend_symbols = list(settings.get("market", {}).get("symbols", []))
    moonshot_syms = [p.symbol for p in plans if p.enabled and not p.manual_only]
    moonshot_syms, moon_missing = symbols_existing_on_exchange(exchange, moonshot_syms)
    if moon_missing:
        logger.warning("[MOONSHOT_RUN] symbols not on exchange (skipped): %s", moon_missing)
    _auto_on = bool((moonshot_root.get("scanner_automation") or {}).get("enabled", False))
    logger.info(
        "[MOONSHOT_RUN] tradable_plans=%s scanner_automation=%s poll_seconds=%d",
        moonshot_syms,
        "on" if _auto_on else "off",
        poll_seconds,
    )
    trend_prefix = str(ledger_cfg.get("trend_client_order_prefix", "trbot"))
    strict_led = bool(
        ledger_cfg.get(
            "strict_reconcile_tagged_only",
            moonshot_root.get("reconcile_strict_tagged_only", False),
        )
    )
    legacy_path = legacy_moonshot_state
    if not os.path.isabs(legacy_path):
        legacy_path = os.path.join(root, legacy_path)

    if bool(ledger_cfg.get("reconcile_on_startup", moonshot_root.get("reconcile_on_startup", True))):
        full_reconcile_snapshot(
            exchange,
            ledger,
            trend_symbols=trend_symbols,
            moonshot_symbols=moonshot_syms,
            trend_prefix=trend_prefix,
            moonshot_prefix=client_order_prefix,
            lookback_days=int(
                ledger_cfg.get("reconcile_lookback_days", moonshot_root.get("reconcile_lookback_days", 90))
            ),
            max_fetch_iterations=int(
                ledger_cfg.get(
                    "reconcile_max_fetch_iterations",
                    moonshot_root.get("reconcile_max_fetch_iterations", 40),
                )
            ),
            strict_reconcile=strict_led,
            moonshot_legacy_path=legacy_path if os.path.exists(legacy_path) else None,
            logger=logger,
        )
    ledger.save()

    logger.info(
        "Moonshot runner started: live_orders=%s quote=%s assets=%d (moonshot prefix=%s ledger=%s)",
        enabled_live_orders,
        quote_asset,
        len(plans),
        client_order_prefix,
        ledger_path,
    )

    scanner_loop_state: dict[str, Any] = {"last_scan_monotonic": 0.0}

    while True:
        try:
            moonshot_root, plans, moonshot_syms, scan_tick = maybe_scanner_refresh(
                root=root,
                portfolio_path=portfolio_path,
                moonshot_root=moonshot_root,
                plans=plans,
                moonshot_syms=moonshot_syms,
                settings=settings,
                quote_asset=quote_asset,
                logger=logger,
                state=scanner_loop_state,
            )
            if scan_tick is not None:
                logger.info("[MOONSHOT_RUN] scanner_tick result=%s syms=%s", scan_tick, moonshot_syms)

            bal = exchange.fetch_balance()
            free_bal = bal.get("free", {}) or {}
            total_bal = bal.get("total", {}) or {}
            quote_free = float(free_bal.get(quote_asset, 0.0) or 0.0)
            total_target_notional = sum(
                float(p.target_usdc)
                for p in plans
                if p.enabled and (not p.manual_only)
            )
            needed_quote = total_target_notional + stablecoin_buffer
            if auto_convert_to_quote:
                quote_free = maybe_convert_to_quote(
                    exchange=exchange,
                    quote_asset=quote_asset,
                    free_bal=free_bal,
                    needed_quote=needed_quote,
                    enabled_live_orders=enabled_live_orders,
                    logger=logger,
                    source_assets=conversion_source_assets,
                    min_conversion_notional=min_conversion_notional,
                )

            checklist_on = bool((settings.get("moonshot_checklist") or {}).get("enabled", False))
            equity_quote = 0.0
            open_moon_n = 0
            if checklist_on:
                ev_mode = str((settings.get("risk") or {}).get("equity_valuation") or "binance_total").strip()
                if ev_mode == "moonshot_symbols_only":
                    equity_quote = estimate_equity_quote(
                        exchange,
                        quote_asset=quote_asset,
                        free_bal=free_bal,
                        total_bal=total_bal,
                        valuation_symbols=moonshot_syms,
                        logger=logger,
                    )
                else:
                    equity_quote = estimate_total_account_equity_usdt(exchange, logger)
                    logger.info(
                        "[MOONSHOT_RUN] equity_binance_est_usdt=%.2f (risk.starting_balance_usdt=%s)",
                        equity_quote,
                        (settings.get("risk") or {}).get("starting_balance_usdt"),
                    )
                open_moon_n = moonshot_open_positions_count(ledger, moonshot_syms)

            for plan in plans:
                if not plan.enabled:
                    continue
                if plan.manual_only:
                    logger.info("MANUAL ONLY %s (%s) target=%.2f", plan.name, plan.symbol, plan.target_usdc)
                    continue
                market = exchange.markets.get(plan.symbol)
                if not market:
                    logger.info("SKIP %s (%s): symbol not available on exchange", plan.name, plan.symbol)
                    continue
                if not bool(market.get("active", True)):
                    logger.info("SKIP %s (%s): market not active/tradable", plan.name, plan.symbol)
                    continue

                try:
                    ticker = exchange.fetch_ticker(plan.symbol)
                    last_price = float(ticker.get("last") or ticker.get("close") or 0.0)
                    if last_price <= 0:
                        logger.info("SKIP %s (%s): invalid last price", plan.name, plan.symbol)
                        continue

                    base_asset, quote = plan.symbol.split("/")
                    if quote.upper() != quote_asset:
                        logger.info(
                            "SKIP %s (%s): quote asset mismatch (%s != %s)",
                            plan.name,
                            plan.symbol,
                            quote,
                            quote_asset,
                        )
                        continue

                    ledger.update_exchange_from_balance(
                        plan.symbol,
                        float(free_bal.get(base_asset, 0.0) or 0.0),
                        float(total_bal.get(base_asset, 0.0) or 0.0),
                    )
                    exchange_total_base = ledger.ensure_symbol(plan.symbol).exchange_total_base
                    ms = ledger.slice(plan.symbol, SOURCE_MOONSHOT)
                    managed_qty, avg_entry = ms.tracked_qty, ms.avg_entry
                    if managed_qty > exchange_total_base + 1e-8:
                        logger.warning(
                            "%s: managed_qty %.8f exceeds exchange total %.8f — sells capped; review state/reconcile",
                            plan.symbol,
                            managed_qty,
                            exchange_total_base,
                        )
                    position_value = managed_qty * last_price

                    skip_buy_this_tick = False
                    if managed_qty > 0 and avg_entry > 0:
                        if last_price >= avg_entry * take_profit_mult:
                            raw_sell = managed_qty * tp_sell_fraction
                            sell_qty = float(exchange.amount_to_precision(plan.symbol, raw_sell))
                            sell_qty = min(sell_qty, managed_qty, exchange_total_base)
                            if sell_qty > 0:
                                skip_buy_this_tick = True
                                cid = make_client_order_id(plan.symbol, client_order_prefix)
                                if enabled_live_orders:
                                    order = exchange.create_market_sell_order(
                                        plan.symbol, sell_qty, {"newClientOrderId": cid}
                                    )
                                    filled, fill_px = extract_order_fill(order, sell_qty, last_price)
                                    filled = min(filled, managed_qty, exchange_total_base)
                                    fee_q = estimate_fee_quote(order, filled, fill_px, quote_asset, fee_rate)
                                    ledger.apply_sell(
                                        plan.symbol,
                                        SOURCE_MOONSHOT,
                                        filled,
                                        fill_px,
                                        fee_q,
                                    )
                                    logger.info(
                                        "TP SELL %s qty=%.8f (filled=%.8f) @ %.8f | order_id=%s",
                                        plan.symbol,
                                        sell_qty,
                                        filled,
                                        fill_px,
                                        order.get("id"),
                                    )
                                else:
                                    logger.info(
                                        "DRY RUN TP SELL %s qty=%.8f @ %.8f",
                                        plan.symbol,
                                        sell_qty,
                                        last_price,
                                    )
                        elif last_price <= avg_entry * stop_loss_mult:
                            raw_sell = managed_qty
                            sell_qty = float(exchange.amount_to_precision(plan.symbol, raw_sell))
                            sell_qty = min(sell_qty, managed_qty, exchange_total_base)
                            if sell_qty > 0:
                                skip_buy_this_tick = True
                                cid = make_client_order_id(plan.symbol, client_order_prefix)
                                if enabled_live_orders:
                                    order = exchange.create_market_sell_order(
                                        plan.symbol, sell_qty, {"newClientOrderId": cid}
                                    )
                                    filled, fill_px = extract_order_fill(order, sell_qty, last_price)
                                    filled = min(filled, managed_qty, exchange_total_base)
                                    fee_q = estimate_fee_quote(order, filled, fill_px, quote_asset, fee_rate)
                                    ledger.apply_sell(
                                        plan.symbol,
                                        SOURCE_MOONSHOT,
                                        filled,
                                        fill_px,
                                        fee_q,
                                    )
                                    logger.info(
                                        "STOP SELL %s qty=%.8f (filled=%.8f) @ %.8f | order_id=%s",
                                        plan.symbol,
                                        sell_qty,
                                        filled,
                                        fill_px,
                                        order.get("id"),
                                    )
                                else:
                                    logger.info(
                                        "DRY RUN STOP SELL %s qty=%.8f @ %.8f",
                                        plan.symbol,
                                        sell_qty,
                                        last_price,
                                    )

                    if skip_buy_this_tick:
                        ledger.save()
                        continue

                    ms = ledger.slice(plan.symbol, SOURCE_MOONSHOT)
                    managed_qty, avg_entry = ms.tracked_qty, ms.avg_entry
                    position_value = managed_qty * last_price
                    untracked_base = max(0.0, exchange_total_base - managed_qty)

                    min_target = plan.target_usdc * (1.0 - rebalance_tol)
                    if position_value >= min_target:
                        logger.info(
                            "MONITOR %s managed_usd=%.2f exch_usd=%.2f untracked_base=%.8f target=%.2f px=%.8f",
                            plan.symbol,
                            position_value,
                            exchange_total_base * last_price,
                            untracked_base,
                            plan.target_usdc,
                            last_price,
                        )
                        continue

                    needed_notional = max(0.0, plan.target_usdc - position_value)
                    spendable_quote = max(0.0, quote_free - stablecoin_buffer)
                    buy_notional = min(needed_notional, spendable_quote)
                    market_min_notional = float(
                        (((market.get("limits") or {}).get("cost") or {}).get("min") or 0.0)
                    )
                    effective_min_notional = max(min_order_notional, market_min_notional)
                    rebalance_skip = moonshot_rebalance_skip_reason(
                        needed_notional=needed_notional,
                        spendable_quote=spendable_quote,
                        effective_min_notional=effective_min_notional,
                    )
                    emit_event(
                        EVENT_MOONSHOT_REBALANCE_EVAL,
                        {
                            "symbol": plan.symbol,
                            "plan_name": plan.name,
                            "managed_qty": managed_qty,
                            "avg_entry": avg_entry,
                            "exchange_total_base": exchange_total_base,
                            "last_price": last_price,
                            "position_value_quote": position_value,
                            "target_usdc": plan.target_usdc,
                            "min_target_usdc": min_target,
                            "rebalance_tol_pct": rebalance_tol * 100.0,
                            "needed_notional": needed_notional,
                            "quote_free": quote_free,
                            "stablecoin_buffer": stablecoin_buffer,
                            "spendable_quote": spendable_quote,
                            "buy_notional_cap": buy_notional,
                            "effective_min_notional": effective_min_notional,
                            "market_min_notional": market_min_notional,
                            "skip_reason": rebalance_skip or "ok",
                            "moonshot_checklist_enabled": checklist_on,
                        },
                        structured_logger=structured,
                    )
                    logger.info(
                        "[MOONSHOT_RUN] rebalance_eval symbol=%s need=%.4f spendable=%.4f eff_min=%.4f skip=%s",
                        plan.symbol,
                        needed_notional,
                        spendable_quote,
                        effective_min_notional,
                        rebalance_skip or "ok",
                    )
                    if rebalance_skip:
                        logger.info(
                            "BUY SKIP %s need=%.2f spendable=%.2f min=%.2f (market_min=%.2f)",
                            plan.symbol,
                            needed_notional,
                            spendable_quote,
                            effective_min_notional,
                            market_min_notional,
                        )
                        continue

                    buy_qty = buy_notional / last_price
                    buy_qty = float(exchange.amount_to_precision(plan.symbol, buy_qty))
                    if buy_qty <= 0:
                        logger.info("BUY SKIP %s qty rounded to zero", plan.symbol)
                        continue
                    # Re-check notional after precision rounding to avoid exchange NOTIONAL filter failures.
                    final_notional = buy_qty * last_price
                    if final_notional < effective_min_notional:
                        logger.info(
                            "BUY SKIP %s rounded_notional=%.2f below min=%.2f",
                            plan.symbol,
                            final_notional,
                            effective_min_notional,
                        )
                        continue

                    _risk = settings.get("risk") or {}
                    _sb = float(_risk.get("starting_balance_usdt") or 347.0)
                    _dd_pct = (
                        max(0.0, (_sb - float(equity_quote)) / _sb * 100.0) if _sb > 0 else 0.0
                    )
                    mdec = evaluate_moonshot_entry(
                        symbol=plan.symbol,
                        entry_notional=float(buy_notional),
                        current_equity=float(equity_quote) if checklist_on else float(
                            settings.get("risk", {}).get("starting_balance_usdt", 347.0)
                        ),
                        open_positions_count=int(open_moon_n) if checklist_on else 0,
                        config=settings,
                    )
                    if checklist_on:
                        emit_event(
                            EVENT_MOONSHOT_CHECKLIST,
                            {
                                "symbol": plan.symbol,
                                "allowed": mdec.allowed,
                                "checklist_score": mdec.score,
                                "checklist_min": mdec.min_required,
                                "reasons": mdec.reasons,
                                "entry_notional": buy_notional,
                                "equity_quote_est": equity_quote,
                                "starting_balance_usdt": _sb,
                                "drawdown_pct_vs_start": round(_dd_pct, 4),
                                "open_moonshot_positions": open_moon_n,
                            },
                            structured_logger=structured,
                        )
                    if not mdec.allowed:
                        logger.info(
                            "[MOONSHOT_RUN] checklist_block symbol=%s score=%d/%d reasons=%s",
                            plan.symbol,
                            mdec.score,
                            mdec.min_required,
                            mdec.reasons,
                        )
                        continue

                    if enabled_live_orders:
                        cid = make_client_order_id(plan.symbol, client_order_prefix)
                        emit_event(
                            EVENT_MOONSHOT_ORDER_INTENT,
                            {
                                "symbol": plan.symbol,
                                "side": "buy",
                                "qty": buy_qty,
                                "last_price": last_price,
                                "est_notional_quote": buy_notional,
                                "client_order_id": cid,
                                "order_type": "market",
                            },
                            structured_logger=structured,
                        )
                        logger.info(
                            "[MOONSHOT_RUN] order_submit market_buy symbol=%s qty=%.8f cid=%s est_cost<=%.4f",
                            plan.symbol,
                            buy_qty,
                            cid,
                            buy_notional,
                        )
                        order = exchange.create_market_buy_order(
                            plan.symbol, buy_qty, {"newClientOrderId": cid}
                        )
                        filled_qty, avg_price = extract_order_fill(order, buy_qty, last_price)
                        if filled_qty <= 0:
                            logger.warning("BUY %s reported zero fill; ledger unchanged | order=%s", plan.symbol, order)
                        else:
                            fee_q = estimate_fee_quote(order, filled_qty, avg_price, quote_asset, fee_rate)
                            ledger.apply_buy(
                                plan.symbol,
                                SOURCE_MOONSHOT,
                                filled_qty,
                                avg_price,
                                fee_q,
                                append_lot=True,
                            )
                            ms_after = ledger.slice(plan.symbol, SOURCE_MOONSHOT)
                            emit_event(
                                EVENT_ORDER_FILLED,
                                {
                                    "source": SOURCE_MOONSHOT,
                                    "symbol": plan.symbol,
                                    "side": "buy",
                                    "filled_qty": filled_qty,
                                    "avg_price": avg_price,
                                    "fee_quote_est": fee_q,
                                    "order_id": order.get("id"),
                                    "client_order_id": cid,
                                    "tracked_qty_after": ms_after.tracked_qty,
                                    "avg_entry_after": ms_after.avg_entry,
                                },
                                structured_logger=structured,
                            )
                            logger.info(
                                "[MOONSHOT_RUN] ledger_apply_buy symbol=%s filled=%.8f avg=%.8f fee_est=%.6f tracked_after=%.8f avg_entry_after=%.8f",
                                plan.symbol,
                                filled_qty,
                                avg_price,
                                fee_q,
                                ms_after.tracked_qty,
                                ms_after.avg_entry,
                            )
                        logger.info(
                            "BUY %s qty=%.8f @ %.8f cost<=%.2f | order_id=%s client=%s",
                            plan.symbol,
                            filled_qty,
                            avg_price,
                            buy_notional,
                            order.get("id"),
                            cid,
                        )
                    else:
                        logger.info(
                            "DRY RUN BUY %s qty=%.8f @ %.8f cost<=%.2f",
                            plan.symbol,
                            buy_qty,
                            last_price,
                            buy_notional,
                        )

                except Exception as asset_exc:
                    logger.exception("Asset cycle failed for %s: %s", plan.symbol, asset_exc)

            ledger.save()
            logger.info("Moonshot cycle complete | free_%s=%.4f", quote_asset, quote_free)

        except KeyboardInterrupt:
            logger.info("Moonshot runner stopped manually.")
            break
        except Exception as exc:
            logger.exception("Moonshot main cycle failed: %s", exc)

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
