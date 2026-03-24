from __future__ import annotations

import json
import os
from bot.portfolio import Portfolio


def load_portfolio(state_file: str, starting_balance_usdt: float) -> Portfolio:
    if not os.path.exists(state_file):
        return Portfolio(cash_usdt=starting_balance_usdt)

    with open(state_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return Portfolio.from_dict(data)


def save_portfolio(state_file: str, portfolio: Portfolio) -> None:
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(portfolio.to_dict(), f, indent=2)
