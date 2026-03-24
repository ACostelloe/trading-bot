"""
Smoke test: exchange reachability, public candles per symbol, and (if keys are set) spot balance.

Run from project root:
  python live/verify_connection.py

This uses the same config/settings.yaml and .env as the main bot.
"""
from __future__ import annotations

import os
import sys
import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


def _root_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    os.chdir(_root_dir())

    from dotenv import load_dotenv

    load_dotenv()

    config_path = os.path.join("config", "settings.yaml")
    config = load_config(config_path)

    from bot.exchange import build_exchange
    from bot.market_data import symbols_from_config

    symbols = symbols_from_config(config)
    timeframe = config["market"]["timeframe"]
    limit = min(int(config["market"].get("limit", 5)), 5)

    print("=== Crypto bot: API / data smoke test ===\n")
    print(f"Config: {config_path}")
    print(f"Exchange: {config['exchange']['name']} | sandbox={config['exchange'].get('sandbox', True)}")
    print(f"Symbols ({len(symbols)}): {symbols}")
    print(f"Timeframe: {timeframe}\n")

    key_set = bool(os.getenv("BINANCE_API_KEY", "").strip())
    secret_set = bool(os.getenv("BINANCE_API_SECRET", "").strip())
    print(f"BINANCE_API_KEY set: {key_set}")
    print(f"BINANCE_API_SECRET set: {secret_set}\n")

    exchange = build_exchange(config)

    try:
        exchange.load_markets()
        print("[OK] load_markets() - exchange reachable, markets loaded.")
    except Exception as e:
        print(f"[FAIL] load_markets(): {e}")
        return

    for sym in symbols:
        if sym not in exchange.markets:
            print(f"[WARN] {sym!r} not in markets dict; candle fetch may fail.")

    candle_ok = 0
    for sym in symbols:
        try:
            rows = exchange.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            if not rows:
                print(f"[FAIL] {sym} fetch_ohlcv: no rows")
                continue
            last = rows[-1]
            _ts_ms, _o, _h, _l, c, v = last
            print(f"[OK] {sym} fetch_ohlcv(limit={limit}) close={c} vol={v}")
            candle_ok += 1
        except Exception as e:
            print(f"[FAIL] {sym} fetch_ohlcv: {e}")

    if candle_ok == 0:
        print("\n[FAIL] No symbols returned candles.")
        return

    if not (key_set and secret_set):
        print("\n[SKIP] fetch_balance - add BINANCE_API_KEY / BINANCE_API_SECRET to .env to test private API.")
        return

    try:
        bal = exchange.fetch_balance()
        free = bal.get("free") or {}
        total = bal.get("total") or {}
        interesting = []
        for coin in sorted(set(free) | set(total)):
            t = float(total.get(coin) or 0)
            f = float(free.get(coin) or 0)
            if t > 0 or f > 0:
                interesting.append(f"  {coin}: free={f} total={t}")
        if interesting:
            print("\n[OK] fetch_balance() - non-zero balances:")
            print("\n".join(interesting[:20]))
            if len(interesting) > 20:
                print(f"  ... ({len(interesting) - 20} more)")
        else:
            print("\n[OK] fetch_balance() - success (all zero or empty totals).")
    except Exception as e:
        print(f"\n[FAIL] fetch_balance: {e}")
        print("  Check: Spot Testnet keys in .env, key permissions, IP allowlist on the key.")
        return

    print("\n=== All checked steps passed - safe to run live/run_paper.py ===")


if __name__ == "__main__":
    main()
