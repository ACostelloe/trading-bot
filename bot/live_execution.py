from __future__ import annotations


def place_live_market_buy(exchange, symbol: str, qty: float, params: dict | None = None) -> dict:
    if params:
        return exchange.create_market_buy_order(symbol, qty, params)
    return exchange.create_market_buy_order(symbol, qty)


def place_live_market_sell(exchange, symbol: str, qty: float, params: dict | None = None) -> dict:
    if params:
        return exchange.create_market_sell_order(symbol, qty, params)
    return exchange.create_market_sell_order(symbol, qty)
