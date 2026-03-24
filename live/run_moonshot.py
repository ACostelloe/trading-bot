from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass

import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bot.entry_gates import moonshot_rebalance_skip_reason
from bot.exchange import build_exchange
from bot.logger import get_logger
from bot.quote_context import build_quote_execution_context
from bot.unified_ledger import (
    SOURCE_MOONSHOT,
    UnifiedLedger,
    estimate_fee_quote,
    full_reconcile_snapshot,
    make_client_order_id,
)


@dataclass
class AssetPlan:
    name: str
    symbol: str
    target_usdc: float
    enabled: bool
    manual_only: bool


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_asset_plans(cfg: dict) -> list[AssetPlan]:
    plans: list[AssetPlan] = []
    for row in cfg.get("assets", []):
        plans.append(
            AssetPlan(
                name=str(row.get("name", row.get("symbol", "unknown"))),
                symbol=str(row.get("symbol", "")).strip(),
                target_usdc=float(row.get("target_usdc", 0.0) or 0.0),
                enabled=bool(row.get("enabled", True)),
                manual_only=bool(row.get("manual_only", False)),
            )
        )
    return plans


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
    current_quote_free = float(free_bal.get(quote_asset, 0.0) or 0.0)
    if needed_quote <= current_quote_free:
        return current_quote_free

    deficit = needed_quote - current_quote_free
    if deficit < min_conversion_notional:
        return current_quote_free

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
        if convert_quote < min_conversion_notional:
            continue

        sell_qty = convert_quote / px
        sell_qty = float(exchange.amount_to_precision(pair, sell_qty))
        if sell_qty <= 0:
            continue

        if enabled_live_orders:
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
    moonshot_root = load_yaml("config/moonshot_portfolio.yaml").get("moonshot", {})
    quote_ctx = build_quote_execution_context(settings, moonshot_root)
    logger = get_logger(settings["logging"]["file"])

    if settings["exchange"].get("sandbox", True):
        raise RuntimeError("Refusing moonshot runner while sandbox is enabled")

    exec_cfg = settings.get("execution", {})
    if exec_cfg.get("mode") != "live":
        raise RuntimeError("Refusing moonshot runner while execution.mode != live")

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

    while True:
        try:
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
                    if moonshot_rebalance_skip_reason(
                        needed_notional=needed_notional,
                        spendable_quote=spendable_quote,
                        effective_min_notional=effective_min_notional,
                    ):
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

                    if enabled_live_orders:
                        cid = make_client_order_id(plan.symbol, client_order_prefix)
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
