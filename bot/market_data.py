from __future__ import annotations

import pandas as pd


def symbols_from_config(config: dict) -> list[str]:
    """Resolve tradable symbols: prefer market.symbols (list), else market.symbol (string)."""
    m = config["market"]
    syms = m.get("symbols")
    if syms:
        return [str(s).strip() for s in syms if str(s).strip()]
    one = m.get("symbol")
    if one:
        return [str(one).strip()]
    raise ValueError("config['market'] must define 'symbols' (list) or 'symbol' (string)")


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int = 300) -> pd.DataFrame:
    rows = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(
        rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def is_data_fresh(df: pd.DataFrame, timeframe: str, max_lag_seconds: int = 180) -> bool:
    if df.empty:
        return False

    tf_to_seconds = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }

    tf_seconds = tf_to_seconds.get(timeframe, 900)
    last_ts = df["timestamp"].iloc[-1].timestamp()
    now_ts = pd.Timestamp.utcnow().timestamp()

    return (now_ts - last_ts) <= (tf_seconds + max_lag_seconds)
