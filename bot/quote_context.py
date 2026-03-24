"""
Single source of truth for quote assets, spend buffers, and conversion policy.

Trend buffers come from execution.*; moonshot buffers and conversion from moonshot_portfolio.yaml.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConversionPolicy:
    enabled: bool
    source_assets: tuple[str, ...]
    min_conversion_notional: float


@dataclass(frozen=True)
class QuoteExecutionContext:
    """Buffers keyed by stablecoin / quote asset code (uppercase)."""

    trend_quote_assets: frozenset[str]
    moonshot_quote_asset: str | None
    startup_min_free_by_asset: dict[str, float]
    trend_spend_buffer_by_quote: dict[str, float]
    moonshot_spend_buffer_quote: float
    conversion: ConversionPolicy

    def quote_for_symbol(self, symbol: str) -> str:
        return str(symbol.split("/")[1]).upper()

    def trend_spend_buffer_for_symbol(self, symbol: str) -> float:
        q = self.quote_for_symbol(symbol)
        return float(self.trend_spend_buffer_by_quote.get(q, 0.0) or 0.0)

    def spendable_trend_cash(self, symbol: str, portfolio_cash_quote: float) -> float:
        """Cash available to open a trend trade after the configured quote buffer."""
        return max(0.0, float(portfolio_cash_quote) - self.trend_spend_buffer_for_symbol(symbol))

    def validate_live_startup_balances(self, free_bal: dict) -> None:
        """Require free[asset] >= min for each asset with a positive minimum."""
        for asset, need in self.startup_min_free_by_asset.items():
            need_f = float(need or 0.0)
            if need_f <= 0:
                continue
            au = str(asset).upper()
            have = float((free_bal or {}).get(au, 0.0) or 0.0)
            if have < need_f:
                raise RuntimeError(
                    f"Live startup: {au} free balance {have:.8f} below required buffer {need_f:.8f}"
                )


def build_quote_execution_context(
    config: dict,
    moonshot_root: dict | None = None,
) -> QuoteExecutionContext:
    moon = moonshot_root or {}
    symbols = list((config.get("market") or {}).get("symbols", []) or [])
    quotes = {str(s).split("/")[1].upper() for s in symbols if "/" in str(s)}
    trend_quotes = frozenset(quotes) if quotes else frozenset({"USDT"})

    exec_cfg = config.get("execution") or {}
    usdt_buf = float(exec_cfg.get("stablecoin_cash_buffer_usdt", 0.0) or 0.0)

    trend_buf: dict[str, float] = {}
    for q in trend_quotes:
        if q == "USDT":
            trend_buf[q] = usdt_buf
        elif q == "USDC":
            trend_buf[q] = float(exec_cfg.get("stablecoin_cash_buffer_usdc", 0.0) or 0.0)
        else:
            key = f"stablecoin_cash_buffer_{q.lower()}"
            trend_buf[q] = float(exec_cfg.get(key, 0.0) or 0.0)

    moon_q = str(moon.get("quote_asset", "") or "").strip().upper() or None
    moon_buf = float(moon.get("stablecoin_buffer_quote", 0.0) or 0.0)

    startup_min: dict[str, float] = {}
    for q in trend_quotes:
        startup_min[q] = float(trend_buf.get(q, 0.0) or 0.0)
    if moon_q:
        startup_min[moon_q] = max(float(startup_min.get(moon_q, 0.0) or 0.0), moon_buf)

    conv = ConversionPolicy(
        enabled=bool(moon.get("auto_convert_to_quote", False)),
        source_assets=tuple(str(x).upper() for x in (moon.get("conversion_source_assets") or ["USDC"])),
        min_conversion_notional=float(moon.get("min_conversion_notional", 5.0) or 0.0),
    )

    return QuoteExecutionContext(
        trend_quote_assets=trend_quotes,
        moonshot_quote_asset=moon_q,
        startup_min_free_by_asset=startup_min,
        trend_spend_buffer_by_quote=trend_buf,
        moonshot_spend_buffer_quote=moon_buf,
        conversion=conv,
    )
