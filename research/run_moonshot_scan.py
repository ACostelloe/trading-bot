"""
Run the Binance public moonshot scanner from the repo root.

  cd /path/to/trading-bot
  python research/run_moonshot_scan.py

Swyftx workflow (discovery still uses Binance public liquidity + OHLCV; Swyftx has no
equivalent free bulk 24h ticker feed). Filter to assets Swyftx lists, map to BASE/AUD:

  python research/run_moonshot_scan.py --swyftx
  python research/run_moonshot_scan.py --swyftx --binance-quote USDT

Candidates are for research only; wire into execution only behind your own gates.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.moonshot_scanner import (
    MoonshotScanner,
    ScannerConfig,
    load_coingecko_map_yaml,
    scan_to_ccxt_symbols,
)


def fetch_swyftx_asset_codes(*, base_url: str = "https://api.swyftx.com.au", timeout: int = 20) -> set[str]:
    """Public GET /markets/assets/ — no API key."""
    url = base_url.rstrip("/") + "/markets/assets/"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    rows = r.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected /markets/assets/ payload: {type(rows)}")
    return {str(x.get("code") or "").upper() for x in rows if x.get("code")}


def picks_to_swyftx_aud(picks: list[dict], swyftx_codes: set[str]) -> list[dict]:
    """Keep rows whose base_asset exists on Swyftx; we intend to trade BASE/AUD."""
    out: list[dict] = []
    for p in picks:
        b = str(p.get("base_asset") or "").upper()
        if b and b in swyftx_codes and b != "AUD":
            out.append({**p, "swyftx_symbol": f"{b}/AUD"})
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parser = argparse.ArgumentParser(description="Moonshot candidate scanner (Binance public data).")
    parser.add_argument(
        "--swyftx",
        action="store_true",
        help="After scan, keep only bases listed on Swyftx and print BASE/AUD (discovery still Binance).",
    )
    parser.add_argument(
        "--binance-quote",
        default=None,
        help="Binance quote for scanning (default: USDC, or USDT when --swyftx).",
    )
    parser.add_argument(
        "--swyftx-api-base",
        default="https://api.swyftx.com.au",
        help="Swyftx API root for asset list (use demo URL if you need sandbox assets).",
    )
    parser.add_argument(
        "--min-24h-quote-volume",
        type=float,
        default=1_500_000,
        help="Binance 24h quote volume floor for broad filter (lower for more names).",
    )
    parser.add_argument(
        "--min-24h-change-pct",
        type=float,
        default=4.0,
        help="Min Binance 24h %% change for broad filter.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=8,
        help="Ranked shortlist size after deep filters.",
    )
    parser.add_argument(
        "--min-rel-volume",
        type=float,
        default=None,
        help="Deep filter: vs MA(volume); lower (e.g. 1.0) for more passes (default: scanner default 1.3).",
    )
    parser.add_argument(
        "--min-breakout-pct",
        type=float,
        default=None,
        help="Deep filter: %% vs recent high; lower / negative for more passes (default -2).",
    )
    parser.add_argument(
        "--min-atr-pct",
        type=float,
        default=None,
        help="Deep filter: min ATR%%; lower for more passes (default 2.0).",
    )
    parser.add_argument(
        "--min-listing-days",
        type=int,
        default=None,
        help="Min days of 1h history on Binance (default 21); lower e.g. 7 for newer listings.",
    )
    parser.add_argument(
        "--skip-book-spread",
        action="store_true",
        help="Do not drop names for wide Binance bookTicker spread (faster, looser).",
    )
    parser.add_argument(
        "--book-spread-max",
        type=float,
        default=None,
        help="Max bid/ask spread %% on Binance bookTicker (0 disables check). Default: 0.35; with --swyftx default is 0 (skip).",
    )
    parser.add_argument(
        "--signal-lookback",
        type=int,
        default=None,
        help="1h candles fetched per symbol (default 120). Use ~504+ if min-listing-days is 21 (504h≈21d window).",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gecko_path = os.path.join(root, "config", "scanner_coingecko_map.yaml")
    gecko_map = load_coingecko_map_yaml(gecko_path) if os.path.isfile(gecko_path) else {}

    binance_quote = args.binance_quote or ("USDT" if args.swyftx else "USDC")

    if args.book_spread_max is not None:
        spread_max = float(args.book_spread_max)
    elif args.swyftx:
        spread_max = 0.0
    else:
        spread_max = 0.35

    cfg = ScannerConfig(
        quote_asset=binance_quote,
        min_24h_quote_volume=float(args.min_24h_quote_volume),
        min_24h_price_change_pct=float(args.min_24h_change_pct),
        max_symbols_after_broad_filter=30,
        top_n=int(args.top_n),
        persist_path=os.path.join(root, "research", "moonshot_scan_state.json"),
        btc_regime_enabled=True,
        btc_min_24h_change_pct=-3.0,
        book_ticker_max_spread_pct=spread_max,
        coingecko_id_by_base=gecko_map,
    )
    if args.min_rel_volume is not None:
        cfg.min_rel_volume = float(args.min_rel_volume)
    if args.min_breakout_pct is not None:
        cfg.min_breakout_distance_pct = float(args.min_breakout_pct)
    if args.min_atr_pct is not None:
        cfg.min_atr_pct = float(args.min_atr_pct)
    if args.min_listing_days is not None:
        cfg.min_listing_days = int(args.min_listing_days)
    if args.skip_book_spread:
        cfg.book_ticker_max_spread_pct = 0.0
    # Default Swyftx path: Binance filters are strict; slightly looser deep gates unless overridden above.
    if args.swyftx:
        if args.min_rel_volume is None:
            cfg.min_rel_volume = 1.05
        if args.min_breakout_pct is None:
            cfg.min_breakout_distance_pct = -5.0
        if args.min_atr_pct is None:
            cfg.min_atr_pct = 1.5
    if args.signal_lookback is not None:
        cfg.signal_lookback = int(args.signal_lookback)
    # MoonshotScanner's "listing_days" is derived from the first kline in the fetch window, not exchange listing date.
    # With 120×1h bars, window ≈5d — min_listing_days=21 rejects everyone unless lookback ≥ ~21×24.
    if cfg.signal_interval == "1h" and cfg.min_listing_days > 0:
        need = int(cfg.min_listing_days * 24) + 5
        if cfg.signal_lookback < need:
            logging.info(
                "Increasing signal_lookback %d -> %d so min_listing_days=%d can be satisfied (1h bars).",
                cfg.signal_lookback,
                need,
                cfg.min_listing_days,
            )
            cfg.signal_lookback = need
    scanner = MoonshotScanner(cfg)
    picks = scanner.scan()
    symbols = scan_to_ccxt_symbols(picks)

    if args.swyftx:
        try:
            sx = fetch_swyftx_asset_codes(base_url=args.swyftx_api_base)
        except Exception as exc:
            logging.error("Swyftx asset fetch failed: %s", exc)
            raise SystemExit(1) from exc
        picks_s = picks_to_swyftx_aud(picks, sx)
        symbols_aud = [p["swyftx_symbol"] for p in picks_s]
        scanner.last_scan_meta["swyftx_api_base"] = args.swyftx_api_base
        scanner.last_scan_meta["swyftx_tradable_bases_count"] = len(sx)
        scanner.last_scan_meta["swyftx_shortlist_count"] = len(picks_s)
        scanner.last_scan_meta["binance_quote_used"] = binance_quote

    print("scan_meta:", scanner.last_scan_meta)
    print("CCXT symbols (Binance scan):", symbols)
    if args.swyftx:
        print("Swyftx pairs (BASE/AUD, listed assets only):", symbols_aud)
    print()
    if args.swyftx:
        picks = picks_s
        symbols = symbols_aud
    if not picks:
        print("No moonshot candidates found.")
        return
    for i, p in enumerate(picks, 1):
        d = p.get("rank_delta")
        delta_s = f" Δrank={d:+d}" if d is not None else ""
        sp = p.get("book_spread_pct")
        sp_s = f" spread={sp}%" if sp is not None else ""
        mc = p.get("market_cap_usd")
        mc_s = f" mcap=${mc:,.0f}" if mc else ""
        swy = p.get("swyftx_symbol")
        swy_s = f" -> {swy}" if swy else ""
        print(
            f"{i:02d}. {p['symbol']:12s} "
            f"score={p['moonshot_score']:6.2f}{delta_s} "
            f"chg24h={p['price_change_pct_24h']:6.2f}% "
            f"qv24h={p['quote_volume_24h']:,.0f} "
            f"rv={p['rel_volume']:.2f} "
            f"atr%={p['atr_pct']:.2f} "
            f"brk%={p['breakout_distance_pct']:.2f} "
            f"age={p['listing_days']:.0f}d "
            f"minNotional={p['min_notional']}"
            f"{sp_s}{mc_s}{swy_s}"
        )


if __name__ == "__main__":
    main()
