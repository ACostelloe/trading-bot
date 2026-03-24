from __future__ import annotations

import json
import os
import secrets
import sys
import time
from dataclasses import dataclass

import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bot.exchange import build_exchange
from bot.logger import get_logger
from bot.moonshot_lots import apply_trade_to_avg_cost, trade_client_order_id


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


def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


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


def make_moonshot_client_order_id(symbol: str, prefix: str) -> str:
    """Binance clientOrderId max 36 chars; must stay unique enough for spot orders."""
    sym = symbol.replace("/", "").replace(":", "")[:8]
    suf = secrets.token_hex(4)
    cid = f"{prefix}{sym}{suf}"
    return cid[:36]


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


def read_managed_row(state: dict, symbol: str) -> tuple[float, float]:
    row = state.get(symbol) or {}
    mq = row.get("managed_qty", row.get("qty"))
    if mq is None:
        mq = 0.0
    mq = float(mq or 0.0)
    ae = float(row.get("avg_entry", 0.0) or 0.0)
    return mq, ae


def write_managed_row(state: dict, symbol: str, managed_qty: float, avg_entry: float) -> None:
    state[symbol] = {"managed_qty": float(managed_qty), "avg_entry": float(avg_entry)}


def apply_buy_fill_state(state: dict, symbol: str, buy_qty: float, buy_price: float) -> None:
    old_mq, old_avg = read_managed_row(state, symbol)
    new_mq = old_mq + buy_qty
    if new_mq <= 0:
        write_managed_row(state, symbol, 0.0, 0.0)
        return
    new_avg = ((old_mq * old_avg) + (buy_qty * buy_price)) / new_mq
    write_managed_row(state, symbol, new_mq, new_avg)


def apply_sell_fill_state(state: dict, symbol: str, sold_qty: float) -> None:
    old_mq, old_avg = read_managed_row(state, symbol)
    sold_qty = max(0.0, min(float(sold_qty), old_mq))
    new_mq = max(0.0, old_mq - sold_qty)
    if new_mq <= 0:
        write_managed_row(state, symbol, 0.0, 0.0)
    else:
        write_managed_row(state, symbol, new_mq, old_avg)


def fetch_tagged_trades(
    exchange,
    symbol: str,
    client_prefix: str,
    since_ms: int,
    max_fetch_iterations: int,
    logger,
) -> list[dict]:
    trades: list[dict] = []
    cursor_since = since_ms
    for _ in range(max(1, max_fetch_iterations)):
        batch = exchange.fetch_my_trades(symbol, since=cursor_since, limit=500)
        if not batch:
            break
        tagged = [t for t in batch if trade_client_order_id(t).startswith(client_prefix)]
        trades.extend(tagged)
        if len(batch) < 500:
            break
        cursor_since = int(batch[-1]["timestamp"] or 0) + 1
        if cursor_since <= since_ms:
            break
    trades.sort(key=lambda x: int(x.get("timestamp") or 0))
    warn_after = 4000
    if len(trades) >= warn_after:
        logger.warning(
            "%s: %d tagged trades in window — reconcile may be slow; narrow lookback or archive state",
            symbol,
            len(trades),
        )
    return trades


def reconcile_position_from_tagged_trades(
    exchange,
    symbol: str,
    client_prefix: str,
    since_ms: int,
    max_fetch_iterations: int,
    logger,
) -> tuple[float, float]:
    base_code, quote_code = symbol.split("/")
    trades = fetch_tagged_trades(
        exchange, symbol, client_prefix, since_ms, max_fetch_iterations, logger
    )
    qty = 0.0
    cost_basis = 0.0
    for t in trades:
        qty, cost_basis = apply_trade_to_avg_cost(qty, cost_basis, t, base_code, quote_code)
    avg = cost_basis / qty if qty > 0 else 0.0
    return qty, avg


def run_startup_reconcile(
    exchange,
    plans: list[AssetPlan],
    state: dict,
    moonshot_root: dict,
    logger,
) -> None:
    if not moonshot_root.get("reconcile_on_startup", True):
        return
    prefix = str(moonshot_root.get("client_order_id_prefix", "msbot"))
    lookback_days = int(moonshot_root.get("reconcile_lookback_days", 90))
    max_fetch_iterations = int(moonshot_root.get("reconcile_max_fetch_iterations", 40))
    since_ms = int(time.time() * 1000) - lookback_days * 86400 * 1000

    for plan in plans:
        if not plan.enabled or plan.manual_only:
            continue
        market = exchange.markets.get(plan.symbol)
        if not market or not bool(market.get("active", True)):
            continue
        tag_mq, tag_av = reconcile_position_from_tagged_trades(
            exchange, plan.symbol, prefix, since_ms, max_fetch_iterations, logger
        )
        file_mq, file_av = read_managed_row(state, plan.symbol)
        strict = bool(moonshot_root.get("reconcile_strict_tagged_only", False))

        if strict and tag_mq > 1e-12 and tag_av > 0:
            write_managed_row(state, plan.symbol, tag_mq, tag_av)
            logger.info(
                "RECONCILE(strict) %s managed_qty=%.8f avg_entry=%.8f (tagged prefix=%s)",
                plan.symbol,
                tag_mq,
                tag_av,
                prefix,
            )
        elif tag_mq > 1e-12 and tag_av > 0 and file_mq > 1e-12:
            rel = abs(tag_mq - file_mq) / max(file_mq, 1e-12)
            if rel <= 0.05:
                write_managed_row(state, plan.symbol, tag_mq, tag_av)
                logger.info(
                    "RECONCILE %s managed_qty=%.8f avg_entry=%.8f (tagged agrees with file within 5%%)",
                    plan.symbol,
                    tag_mq,
                    tag_av,
                )
            else:
                logger.warning(
                    "%s: tagged managed=%.8f vs file=%.8f (%.1f%% diff) — keeping FILE state; "
                    "enable reconcile_strict_tagged_only or widen reconcile_lookback_days",
                    plan.symbol,
                    tag_mq,
                    file_mq,
                    rel * 100,
                )
        elif tag_mq > 1e-12 and tag_av > 0:
            write_managed_row(state, plan.symbol, tag_mq, tag_av)
            logger.info(
                "RECONCILE %s managed_qty=%.8f avg_entry=%.8f (from tagged trades only)",
                plan.symbol,
                tag_mq,
                tag_av,
            )
        else:
            if file_mq > 0:
                logger.warning(
                    "%s: no tagged trades in lookback; keeping file managed_qty=%.8f",
                    plan.symbol,
                    file_mq,
                )


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
    logger = get_logger(settings["logging"]["file"])

    if settings["exchange"].get("sandbox", True):
        raise RuntimeError("Refusing moonshot runner while sandbox is enabled")

    exec_cfg = settings.get("execution", {})
    if exec_cfg.get("mode") != "live":
        raise RuntimeError("Refusing moonshot runner while execution.mode != live")

    enabled_live_orders = bool(moonshot_root.get("enabled_live_orders", False))
    poll_seconds = int(moonshot_root.get("poll_seconds", 60))
    quote_asset = str(moonshot_root.get("quote_asset", "USDC")).upper()
    stablecoin_buffer = float(moonshot_root.get("stablecoin_buffer_quote", 20.0))
    min_order_notional = float(moonshot_root.get("min_order_notional", 10.0))
    auto_convert_to_quote = bool(moonshot_root.get("auto_convert_to_quote", False))
    conversion_source_assets = moonshot_root.get("conversion_source_assets", ["USDC"])
    min_conversion_notional = float(moonshot_root.get("min_conversion_notional", 5.0))
    rebalance_tol = float(moonshot_root.get("rebalance_tolerance_pct", 5.0)) / 100.0
    take_profit_mult = 1.0 + (float(moonshot_root.get("take_profit_pct", 100.0)) / 100.0)
    tp_sell_fraction = float(moonshot_root.get("take_profit_sell_fraction", 0.30))
    stop_loss_mult = 1.0 - (float(moonshot_root.get("stop_loss_pct", 18.0)) / 100.0)
    state_file = str(moonshot_root.get("state_file", "moonshot_state.json"))
    client_order_prefix = str(moonshot_root.get("client_order_id_prefix", "msbot"))

    plans = parse_asset_plans(moonshot_root)
    if not plans:
        raise RuntimeError("No assets configured in config/moonshot_portfolio.yaml")

    exchange = build_exchange(settings)
    exchange.load_markets()
    state = load_json(state_file)
    run_startup_reconcile(exchange, plans, state, moonshot_root, logger)
    save_json(state_file, state)

    logger.info(
        "Moonshot runner started: live_orders=%s quote=%s assets=%d (tracked-lot prefix=%s)",
        enabled_live_orders,
        quote_asset,
        len(plans),
        client_order_prefix,
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

                    exchange_total_base = float(total_bal.get(base_asset, 0.0) or 0.0)
                    managed_qty, avg_entry = read_managed_row(state, plan.symbol)
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
                                cid = make_moonshot_client_order_id(plan.symbol, client_order_prefix)
                                if enabled_live_orders:
                                    order = exchange.create_market_sell_order(
                                        plan.symbol, sell_qty, {"newClientOrderId": cid}
                                    )
                                    filled, fill_px = extract_order_fill(order, sell_qty, last_price)
                                    filled = min(filled, managed_qty, exchange_total_base)
                                    apply_sell_fill_state(state, plan.symbol, filled)
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
                                cid = make_moonshot_client_order_id(plan.symbol, client_order_prefix)
                                if enabled_live_orders:
                                    order = exchange.create_market_sell_order(
                                        plan.symbol, sell_qty, {"newClientOrderId": cid}
                                    )
                                    filled, fill_px = extract_order_fill(order, sell_qty, last_price)
                                    filled = min(filled, managed_qty, exchange_total_base)
                                    apply_sell_fill_state(state, plan.symbol, filled)
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
                        save_json(state_file, state)
                        continue

                    managed_qty, avg_entry = read_managed_row(state, plan.symbol)
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
                    if buy_notional < effective_min_notional:
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
                        cid = make_moonshot_client_order_id(plan.symbol, client_order_prefix)
                        order = exchange.create_market_buy_order(
                            plan.symbol, buy_qty, {"newClientOrderId": cid}
                        )
                        filled_qty, avg_price = extract_order_fill(order, buy_qty, last_price)
                        if filled_qty <= 0:
                            logger.warning("BUY %s reported zero fill; state unchanged | order=%s", plan.symbol, order)
                        else:
                            apply_buy_fill_state(state, plan.symbol, filled_qty, avg_price)
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

            save_json(state_file, state)
            logger.info("Moonshot cycle complete | free_%s=%.4f", quote_asset, quote_free)

        except KeyboardInterrupt:
            logger.info("Moonshot runner stopped manually.")
            break
        except Exception as exc:
            logger.exception("Moonshot main cycle failed: %s", exc)

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
