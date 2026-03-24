from __future__ import annotations

from bot.portfolio import Portfolio


def handle_paper_buy(
    portfolio: Portfolio,
    symbol: str,
    qty: float,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    fee_rate: float,
    entry_time: str | None = None,
) -> dict:
    portfolio.open_position(
        symbol=symbol,
        qty=qty,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        fee_rate=fee_rate,
        entry_time=entry_time,
    )
    return {
        "status": "filled",
        "side": "buy",
        "symbol": symbol,
        "qty": qty,
        "price": entry_price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
    }


def handle_paper_sell(
    portfolio: Portfolio,
    symbol: str,
    exit_price: float,
    fee_rate: float,
) -> dict:
    pnl = portfolio.close_position(symbol, exit_price, fee_rate)
    return {
        "status": "filled",
        "side": "sell",
        "symbol": symbol,
        "price": exit_price,
        "pnl": pnl,
    }


def check_stop_or_take_profit(portfolio: Portfolio, symbol: str, last_price: float) -> str | None:
    pos = portfolio.get_position(symbol)
    if not pos:
        return None

    if last_price <= pos.stop_loss:
        return "stop_loss"

    if last_price >= pos.take_profit:
        return "take_profit"

    return None
