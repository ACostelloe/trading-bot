from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass

import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bot.exchange import build_exchange
from bot.logger import get_logger


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


def update_state_after_buy(state: dict, symbol: str, buy_qty: float, buy_price: float) -> None:
    row = state.setdefault(symbol, {"qty": 0.0, "avg_entry": 0.0})
    old_qty = float(row.get("qty", 0.0))
    old_avg = float(row.get("avg_entry", 0.0))
    new_qty = old_qty + buy_qty
    if new_qty <= 0:
        row["qty"] = 0.0
        row["avg_entry"] = 0.0
        return
    row["avg_entry"] = ((old_qty * old_avg) + (buy_qty * buy_price)) / new_qty
    row["qty"] = new_qty


def update_state_after_sell(state: dict, symbol: str, sell_qty: float) -> None:
    row = state.setdefault(symbol, {"qty": 0.0, "avg_entry": 0.0})
    old_qty = float(row.get("qty", 0.0))
    remaining = max(0.0, old_qty - sell_qty)
    row["qty"] = remaining
    if remaining == 0.0:
        row["avg_entry"] = 0.0


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
    rebalance_tol = float(moonshot_root.get("rebalance_tolerance_pct", 5.0)) / 100.0
    take_profit_mult = 1.0 + (float(moonshot_root.get("take_profit_pct", 100.0)) / 100.0)
    tp_sell_fraction = float(moonshot_root.get("take_profit_sell_fraction", 0.30))
    stop_loss_mult = 1.0 - (float(moonshot_root.get("stop_loss_pct", 18.0)) / 100.0)
    state_file = str(moonshot_root.get("state_file", "moonshot_state.json"))

    plans = parse_asset_plans(moonshot_root)
    if not plans:
        raise RuntimeError("No assets configured in config/moonshot_portfolio.yaml")

    exchange = build_exchange(settings)
    exchange.load_markets()
    state = load_json(state_file)

    logger.info(
        "Moonshot runner started: live_orders=%s quote=%s assets=%d",
        enabled_live_orders,
        quote_asset,
        len(plans),
    )

    while True:
        try:
            bal = exchange.fetch_balance()
            free_bal = bal.get("free", {}) or {}
            total_bal = bal.get("total", {}) or {}
            quote_free = float(free_bal.get(quote_asset, 0.0) or 0.0)

            for plan in plans:
                if not plan.enabled:
                    continue
                if plan.manual_only:
                    logger.info("MANUAL ONLY %s (%s) target=%.2f", plan.name, plan.symbol, plan.target_usdc)
                    continue
                if plan.symbol not in exchange.markets:
                    logger.info("SKIP %s (%s): symbol not available on exchange", plan.name, plan.symbol)
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

                    total_base_qty = float(total_bal.get(base_asset, 0.0) or 0.0)
                    position_value = total_base_qty * last_price

                    state_row = state.setdefault(plan.symbol, {"qty": 0.0, "avg_entry": 0.0})
                    avg_entry = float(state_row.get("avg_entry", 0.0) or 0.0)

                    if total_base_qty > 0 and avg_entry > 0:
                        if last_price >= avg_entry * take_profit_mult:
                            sell_qty = total_base_qty * tp_sell_fraction
                            sell_qty = float(exchange.amount_to_precision(plan.symbol, sell_qty))
                            if sell_qty > 0:
                                if enabled_live_orders:
                                    order = exchange.create_market_sell_order(plan.symbol, sell_qty)
                                    update_state_after_sell(state, plan.symbol, sell_qty)
                                    logger.info(
                                        "TP SELL %s qty=%.8f @ %.8f | order_id=%s",
                                        plan.symbol,
                                        sell_qty,
                                        last_price,
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
                            sell_qty = float(exchange.amount_to_precision(plan.symbol, total_base_qty))
                            if sell_qty > 0:
                                if enabled_live_orders:
                                    order = exchange.create_market_sell_order(plan.symbol, sell_qty)
                                    update_state_after_sell(state, plan.symbol, sell_qty)
                                    logger.info(
                                        "STOP SELL %s qty=%.8f @ %.8f | order_id=%s",
                                        plan.symbol,
                                        sell_qty,
                                        last_price,
                                        order.get("id"),
                                    )
                                else:
                                    logger.info(
                                        "DRY RUN STOP SELL %s qty=%.8f @ %.8f",
                                        plan.symbol,
                                        sell_qty,
                                        last_price,
                                    )

                    min_target = plan.target_usdc * (1.0 - rebalance_tol)
                    if position_value >= min_target:
                        logger.info(
                            "MONITOR %s pos=%.2f target=%.2f px=%.8f",
                            plan.symbol,
                            position_value,
                            plan.target_usdc,
                            last_price,
                        )
                        continue

                    needed_notional = max(0.0, plan.target_usdc - position_value)
                    spendable_quote = max(0.0, quote_free - stablecoin_buffer)
                    buy_notional = min(needed_notional, spendable_quote)
                    if buy_notional < min_order_notional:
                        logger.info(
                            "BUY SKIP %s need=%.2f spendable=%.2f min=%.2f",
                            plan.symbol,
                            needed_notional,
                            spendable_quote,
                            min_order_notional,
                        )
                        continue

                    buy_qty = buy_notional / last_price
                    buy_qty = float(exchange.amount_to_precision(plan.symbol, buy_qty))
                    if buy_qty <= 0:
                        logger.info("BUY SKIP %s qty rounded to zero", plan.symbol)
                        continue

                    if enabled_live_orders:
                        order = exchange.create_market_buy_order(plan.symbol, buy_qty)
                        filled_qty = float(order.get("filled") or buy_qty)
                        avg_price = float(order.get("average") or last_price)
                        update_state_after_buy(state, plan.symbol, filled_qty, avg_price)
                        logger.info(
                            "BUY %s qty=%.8f @ %.8f cost<=%.2f | order_id=%s",
                            plan.symbol,
                            filled_qty,
                            avg_price,
                            buy_notional,
                            order.get("id"),
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
