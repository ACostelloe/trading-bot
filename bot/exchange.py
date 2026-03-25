from __future__ import annotations

import os
from dotenv import load_dotenv

from bot.swyftx_exchange import SwyftxExchange


def build_exchange(config: dict):
    load_dotenv()

    exchange_name = config["exchange"]["name"]
    sandbox = config["exchange"].get("sandbox", True)

    name = str(exchange_name).lower().strip()
    if name == "swyftx":
        return SwyftxExchange(
            api_key=os.getenv("SWYFTX_API_KEY", ""),
            base_url=os.getenv("SWYFTX_BASE_URL", "").strip() or None,
            user_agent=os.getenv("SWYFTX_USER_AGENT", "trading-bot/1.0"),
            demo=bool(sandbox),
        )

    import ccxt

    exchange_class = getattr(ccxt, exchange_name)
    opts: dict = {
        "defaultType": "spot",
    }
    if name == "binance":
        opts["warnOnFetchOpenOrdersWithoutSymbol"] = False
    exchange = exchange_class(
        {
            "apiKey": os.getenv("BINANCE_API_KEY", ""),
            "secret": os.getenv("BINANCE_API_SECRET", ""),
            "enableRateLimit": True,
            "options": opts,
        }
    )

    # Important: sandbox must be the first call after exchange creation.
    if sandbox:
        exchange.set_sandbox_mode(True)

    return exchange
