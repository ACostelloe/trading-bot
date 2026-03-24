from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional
import pandas as pd

SignalType = Literal["buy", "sell", "hold"]


@dataclass
class Signal:
    action: SignalType
    reason: str
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


def generate_signal(df: pd.DataFrame, config: dict, in_position: bool) -> Signal:
    if len(df) < 3:
        return Signal(action="hold", reason="not_enough_data")

    prev_row = df.iloc[-2]
    row = df.iloc[-1]

    rsi_min = config["strategy"]["rsi_entry_min"]
    stop_atr_multiple = config["strategy"]["stop_atr_multiple"]
    take_profit_rr = config["strategy"]["take_profit_rr"]

    crossed_up = prev_row["ema_fast"] <= prev_row["ema_slow"] and row["ema_fast"] > row["ema_slow"]
    crossed_down = prev_row["ema_fast"] >= prev_row["ema_slow"] and row["ema_fast"] < row["ema_slow"]

    if not in_position and crossed_up and row["rsi"] > rsi_min:
        entry = float(row["close"])
        stop = float(entry - (row["atr"] * stop_atr_multiple))
        risk_per_unit = entry - stop
        take_profit = float(entry + (risk_per_unit * take_profit_rr))

        return Signal(
            action="buy",
            reason="ema_cross_up_rsi_confirmed",
            price=entry,
            stop_loss=stop,
            take_profit=take_profit,
        )

    if in_position and crossed_down:
        return Signal(
            action="sell",
            reason="ema_cross_down_exit",
            price=float(row["close"]),
        )

    return Signal(action="hold", reason="no_signal")
