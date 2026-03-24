from __future__ import annotations

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange


def add_indicators(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    out = df.copy()

    ema_fast = config["strategy"]["ema_fast"]
    ema_slow = config["strategy"]["ema_slow"]
    rsi_period = config["strategy"]["rsi_period"]
    atr_period = config["strategy"]["atr_period"]

    out["ema_fast"] = EMAIndicator(close=out["close"], window=ema_fast).ema_indicator()
    out["ema_slow"] = EMAIndicator(close=out["close"], window=ema_slow).ema_indicator()
    out["rsi"] = RSIIndicator(close=out["close"], window=rsi_period).rsi()
    out["atr"] = AverageTrueRange(
        high=out["high"], low=out["low"], close=out["close"], window=atr_period
    ).average_true_range()

    return out
