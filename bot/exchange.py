from __future__ import annotations

import os
import ccxt
from dotenv import load_dotenv


def build_exchange(config: dict):
    load_dotenv()

    exchange_name = config["exchange"]["name"]
    sandbox = config["exchange"].get("sandbox", True)

    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class(
        {
            "apiKey": os.getenv("BINANCE_API_KEY", ""),
            "secret": os.getenv("BINANCE_API_SECRET", ""),
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
            },
        }
    )

    # Important: sandbox must be the first call after exchange creation.
    if sandbox:
        exchange.set_sandbox_mode(True)

    return exchange
