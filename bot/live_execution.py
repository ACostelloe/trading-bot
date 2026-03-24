from __future__ import annotations


def place_live_market_buy(exchange, symbol: str, qty: float) -> dict:
    order = exchange.create_market_buy_order(symbol, qty)
    return order


def place_live_market_sell(exchange, symbol: str, qty: float) -> dict:
    order = exchange.create_market_sell_order(symbol, qty)
    return order
