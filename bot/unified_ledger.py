"""
Single portfolio ledger: exchange balances vs bot-tracked lots by source (trend / moonshot).

Startup builds a truth snapshot: balances, open orders summary, replay of tagged fills,
tracked_qty, avg cost, realized PnL per (symbol, source).
"""

from __future__ import annotations

import json
import secrets
import time
from dataclasses import dataclass, field, asdict
from typing import Any

from bot.moonshot_lots import apply_trade_to_avg_cost, trade_client_order_id

SOURCE_TREND = "trend"
SOURCE_MOONSHOT = "moonshot"

LEDGER_VERSION = 1


@dataclass
class PositionSlice:
    source: str
    tracked_qty: float = 0.0
    cost_basis_quote: float = 0.0
    realized_pnl_quote: float = 0.0
    fees_paid_quote: float = 0.0
    stop_loss: float | None = None
    take_profit: float | None = None
    lots: list[dict[str, Any]] = field(default_factory=list)

    @property
    def avg_entry(self) -> float:
        return self.cost_basis_quote / self.tracked_qty if self.tracked_qty > 1e-12 else 0.0


@dataclass
class SymbolBook:
    symbol: str
    base: str
    quote: str
    exchange_free_base: float = 0.0
    exchange_total_base: float = 0.0
    slices: dict[str, PositionSlice] = field(default_factory=dict)

    def untracked_base(self) -> float:
        managed = sum(s.tracked_qty for s in self.slices.values())
        return max(0.0, self.exchange_total_base - managed)


@dataclass
class UnifiedLedger:
    path: str
    quote_currency: str
    symbols: dict[str, SymbolBook] = field(default_factory=dict)
    cash_free_quote: dict[str, float] = field(default_factory=dict)
    cash_total_quote: dict[str, float] = field(default_factory=dict)
    open_orders_count: int = 0
    open_orders_notional_quote_est: float = 0.0
    last_full_reconcile_ms: int = 0
    last_error: str = ""

    def ensure_symbol(self, symbol: str) -> SymbolBook:
        if symbol not in self.symbols:
            base, quote = symbol.split("/")
            self.symbols[symbol] = SymbolBook(
                symbol=symbol,
                base=base,
                quote=quote,
                slices={
                    SOURCE_TREND: PositionSlice(source=SOURCE_TREND),
                    SOURCE_MOONSHOT: PositionSlice(source=SOURCE_MOONSHOT),
                },
            )
        return self.symbols[symbol]

    def slice(self, symbol: str, source: str) -> PositionSlice:
        book = self.ensure_symbol(symbol)
        if source not in book.slices:
            book.slices[source] = PositionSlice(source=source)
        return book.slices[source]

    def tracked_qty(self, symbol: str, source: str) -> float:
        return float(self.slice(symbol, source).tracked_qty)

    def apply_buy(
        self,
        symbol: str,
        source: str,
        qty: float,
        price: float,
        fee_quote: float,
        *,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        append_lot: bool = True,
    ) -> None:
        sl = self.slice(symbol, source)
        q = float(qty)
        if q <= 0:
            return
        px = float(price)
        fq = float(fee_quote)
        old_q = sl.tracked_qty
        sl.tracked_qty = old_q + q
        sl.cost_basis_quote += q * px + fq
        sl.fees_paid_quote += fq
        if stop_loss is not None:
            sl.stop_loss = float(stop_loss)
        if take_profit is not None:
            sl.take_profit = float(take_profit)
        if append_lot:
            sl.lots.append(
                {
                    "side": "buy",
                    "qty": q,
                    "price": px,
                    "fee_quote": fq,
                    "ts_ms": int(time.time() * 1000),
                    "source": source,
                }
            )

    def apply_sell(
        self,
        symbol: str,
        source: str,
        qty: float,
        price: float,
        fee_quote: float,
    ) -> float:
        """Reduce tracked slice; return incremental realized PnL in quote."""
        sl = self.slice(symbol, source)
        sold = max(0.0, min(float(qty), sl.tracked_qty))
        if sold <= 0:
            return 0.0
        px = float(price)
        fq = float(fee_quote)
        avg = sl.avg_entry
        proceeds = sold * px - fq
        cost_rel = sold * avg
        pnl_chunk = proceeds - cost_rel
        sl.realized_pnl_quote += pnl_chunk
        sl.tracked_qty -= sold
        sl.cost_basis_quote -= sold * avg
        if sl.tracked_qty <= 1e-12:
            sl.tracked_qty = 0.0
            sl.cost_basis_quote = 0.0
            sl.stop_loss = None
            sl.take_profit = None
        sl.fees_paid_quote += fq
        sl.lots.append(
            {
                "side": "sell",
                "qty": sold,
                "price": px,
                "fee_quote": fq,
                "ts_ms": int(time.time() * 1000),
                "source": source,
                "pnl_quote": pnl_chunk,
            }
        )
        return pnl_chunk

    def unrealized_quote(self, symbol: str, source: str, mark_price: float) -> float:
        sl = self.slice(symbol, source)
        if sl.tracked_qty <= 0:
            return 0.0
        return sl.tracked_qty * float(mark_price) - sl.cost_basis_quote

    def update_exchange_from_balance(self, symbol: str, free: float, total: float) -> None:
        book = self.ensure_symbol(symbol)
        book.exchange_free_base = float(free)
        book.exchange_total_base = float(total)

    def to_serializable(self) -> dict[str, Any]:
        def ser_book(b: SymbolBook) -> dict[str, Any]:
            d = asdict(b)
            d["slices"] = {k: asdict(v) for k, v in b.slices.items()}
            return d

        return {
            "version": LEDGER_VERSION,
            "quote_currency": self.quote_currency,
            "path": self.path,
            "symbols": {k: ser_book(v) for k, v in self.symbols.items()},
            "cash_free_quote": self.cash_free_quote,
            "cash_total_quote": self.cash_total_quote,
            "open_orders_count": self.open_orders_count,
            "open_orders_notional_quote_est": self.open_orders_notional_quote_est,
            "last_full_reconcile_ms": self.last_full_reconcile_ms,
            "last_error": self.last_error,
        }

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.to_serializable(), f, indent=2, sort_keys=True)

    @classmethod
    def load(cls, path: str, default_quote: str = "USDT") -> UnifiedLedger:
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except FileNotFoundError:
            return cls(path=path, quote_currency=default_quote)
        led = cls(
            path=path,
            quote_currency=str(raw.get("quote_currency", default_quote)),
            cash_free_quote=dict(raw.get("cash_free_quote", {})),
            cash_total_quote=dict(raw.get("cash_total_quote", {})),
            open_orders_count=int(raw.get("open_orders_count", 0)),
            open_orders_notional_quote_est=float(raw.get("open_orders_notional_quote_est", 0.0)),
            last_full_reconcile_ms=int(raw.get("last_full_reconcile_ms", 0)),
            last_error=str(raw.get("last_error", "")),
        )
        for sym, row in raw.get("symbols", {}).items():
            slices_raw = row.get("slices", {})
            slices: dict[str, PositionSlice] = {}
            for sk, sv in slices_raw.items():
                slices[sk] = PositionSlice(
                    source=str(sv.get("source", sk)),
                    tracked_qty=float(sv.get("tracked_qty", 0.0)),
                    cost_basis_quote=float(sv.get("cost_basis_quote", 0.0)),
                    realized_pnl_quote=float(sv.get("realized_pnl_quote", 0.0)),
                    fees_paid_quote=float(sv.get("fees_paid_quote", 0.0)),
                    stop_loss=sv.get("stop_loss"),
                    take_profit=sv.get("take_profit"),
                    lots=list(sv.get("lots", [])),
                )
            led.symbols[sym] = SymbolBook(
                symbol=str(row.get("symbol", sym)),
                base=str(row["base"]),
                quote=str(row["quote"]),
                exchange_free_base=float(row.get("exchange_free_base", 0.0)),
                exchange_total_base=float(row.get("exchange_total_base", 0.0)),
                slices=slices,
            )
        for _sym, book in led.symbols.items():
            for src in (SOURCE_TREND, SOURCE_MOONSHOT):
                if src not in book.slices:
                    book.slices[src] = PositionSlice(source=src)
        return led


def make_client_order_id(symbol: str, prefix: str) -> str:
    sym = symbol.replace("/", "").replace(":", "")[:8]
    suf = secrets.token_hex(4)
    return f"{prefix}{sym}{suf}"[:36]


def estimate_fee_quote(
    order: dict | None,
    filled_base: float,
    avg_px: float,
    quote_ccy: str,
    fallback_rate: float,
) -> float:
    """Prefer exchange-reported fee in quote; else notional * fallback_rate."""
    o = order or {}
    fee = o.get("fee")
    if isinstance(fee, dict):
        c = float(fee.get("cost") or 0.0)
        cy = str(fee.get("currency") or "").upper()
        if c > 0 and cy == str(quote_ccy).upper():
            return c
    fb = max(0.0, float(filled_base))
    px = max(0.0, float(avg_px))
    return fb * px * max(0.0, float(fallback_rate))


def fetch_my_trades_window(
    exchange,
    symbol: str,
    since_ms: int,
    max_iterations: int,
) -> list[dict]:
    out: list[dict] = []
    cursor = since_ms
    for _ in range(max(1, max_iterations)):
        batch = exchange.fetch_my_trades(symbol, since=cursor, limit=500)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 500:
            break
        cursor = int(batch[-1]["timestamp"] or 0) + 1
        if cursor <= since_ms:
            break
    out.sort(key=lambda x: int(x.get("timestamp") or 0))
    return out


def replay_tagged_slice(trades: list[dict], base: str, quote: str) -> tuple[float, float, float]:
    """Return tracked_qty, cost_basis_quote, realized_pnl_quote from replay."""
    q = 0.0
    cost = 0.0
    realized = 0.0
    for t in trades:
        side = str(t.get("side") or "").lower()
        amount = float(t.get("amount") or 0.0)
        price = float(t.get("price") or 0.0)
        if amount <= 0 or price <= 0:
            continue
        if side == "sell" and q > 0:
            sold = min(amount, q)
            avg = cost / q if q > 0 else 0.0
            fee = t.get("fee") or {}
            fee_c = float(fee.get("cost") or 0.0)
            fee_ccy = str(fee.get("currency") or "")
            sell_fee_q = fee_c if fee_ccy == quote else 0.0
            proceeds = sold * price - sell_fee_q
            cost_rel = sold * avg
            realized += proceeds - cost_rel
        q, cost = apply_trade_to_avg_cost(q, cost, t, base, quote)
    return q, cost, realized


def import_legacy_moonshot_json(ledger: UnifiedLedger, legacy_path: str) -> None:
    try:
        with open(legacy_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return
    for sym, row in raw.items():
        if "/" not in str(sym):
            continue
        mq = float(row.get("managed_qty", row.get("qty", 0)) or 0.0)
        ae = float(row.get("avg_entry", 0.0) or 0.0)
        if mq <= 0:
            continue
        sl = ledger.slice(sym, SOURCE_MOONSHOT)
        if sl.tracked_qty > 1e-12:
            continue
        sl.tracked_qty = mq
        sl.cost_basis_quote = mq * ae


def full_reconcile_snapshot(
    exchange,
    ledger: UnifiedLedger,
    *,
    trend_symbols: list[str],
    moonshot_symbols: list[str],
    trend_prefix: str,
    moonshot_prefix: str,
    lookback_days: int,
    max_fetch_iterations: int,
    strict_reconcile: bool,
    moonshot_legacy_path: str | None,
    logger,
    reconciliation_deltas: list[dict[str, Any]] | None = None,
) -> None:
    """Refresh exchange snapshot, open orders, and rebuild tagged slices."""
    since_ms = int(time.time() * 1000) - int(lookback_days) * 86400 * 1000
    bal = exchange.fetch_balance()
    free = bal.get("free", {}) or {}
    total = bal.get("total", {}) or {}

    for q in {ledger.quote_currency, "USDT", "USDC", "BUSD"}:
        if q in free:
            ledger.cash_free_quote[q] = float(free.get(q) or 0.0)
        if q in total:
            ledger.cash_total_quote[q] = float(total.get(q) or 0.0)

    try:
        oo = exchange.fetch_open_orders()
        ledger.open_orders_count = len(oo)
        ledger.open_orders_notional_quote_est = 0.0
    except Exception as exc:
        ledger.last_error = f"fetch_open_orders: {exc}"
        logger.warning("fetch_open_orders failed: %s", exc)
        ledger.open_orders_count = -1

    universe = sorted(set(trend_symbols) | set(moonshot_symbols))
    for sym in universe:
        book = ledger.ensure_symbol(sym)
        base = book.base
        book.exchange_free_base = float(free.get(base, 0.0) or 0.0)
        book.exchange_total_base = float(total.get(base, 0.0) or 0.0)

    if moonshot_legacy_path:
        import_legacy_moonshot_json(ledger, moonshot_legacy_path)

    for sym in universe:
        book = ledger.ensure_symbol(sym)
        base, quote = book.base, book.quote
        all_trades: list[dict] = []
        if sym not in exchange.markets:
            logger.warning("LEDGER reconcile: skip trade history (unknown market): %s", sym)
        else:
            try:
                all_trades = fetch_my_trades_window(exchange, sym, since_ms, max_fetch_iterations)
            except Exception as exc:
                logger.warning("LEDGER reconcile: fetch_my_trades failed %s: %s", sym, exc)

        for src, prefix in ((SOURCE_MOONSHOT, moonshot_prefix), (SOURCE_TREND, trend_prefix)):
            if src == SOURCE_MOONSHOT and sym not in moonshot_symbols:
                continue
            if src == SOURCE_TREND and sym not in trend_symbols:
                continue
            tagged = [t for t in all_trades if trade_client_order_id(t).startswith(prefix)]
            tag_q, tag_cost, tag_r = replay_tagged_slice(tagged, base, quote)

            sl = ledger.slice(sym, src)
            file_q = sl.tracked_qty
            file_cost = sl.cost_basis_quote
            file_r = sl.realized_pnl_quote

            if tag_q <= 1e-12:
                continue

            def _push_delta(mode: str) -> None:
                if reconciliation_deltas is None:
                    return
                reconciliation_deltas.append(
                    {
                        "symbol": sym,
                        "source": src,
                        "mode": mode,
                        "before_tracked_qty": file_q,
                        "after_tracked_qty": sl.tracked_qty,
                        "before_cost_basis_quote": file_cost,
                        "after_cost_basis_quote": sl.cost_basis_quote,
                        "before_realized_quote": file_r,
                        "after_realized_quote": sl.realized_pnl_quote,
                        "tagged_tracked_qty": tag_q,
                        "tagged_cost_basis_quote": tag_cost,
                        "tagged_realized_hist_quote": tag_r,
                    }
                )

            if strict_reconcile:
                sl.tracked_qty, sl.cost_basis_quote = tag_q, tag_cost
                sl.realized_pnl_quote = tag_r
                _push_delta("strict_tagged_overwrite")
                logger.info(
                    "LEDGER strict %s %s qty=%.8f avg=%.8f realized_hist=%.4f",
                    sym,
                    src,
                    tag_q,
                    tag_cost / tag_q if tag_q else 0.0,
                    tag_r,
                )
                continue

            if file_q <= 1e-12:
                sl.tracked_qty, sl.cost_basis_quote = tag_q, tag_cost
                sl.realized_pnl_quote = tag_r
                _push_delta("file_empty_tagged_fill")
                continue

            rel = abs(tag_q - file_q) / max(file_q, 1e-12)
            if rel <= 0.05:
                sl.tracked_qty, sl.cost_basis_quote = tag_q, tag_cost
                sl.realized_pnl_quote = tag_r
                _push_delta("tagged_within_5pct")
            else:
                logger.warning(
                    "LEDGER %s %s tagged=%.8f file=%.8f (%.1f%%) — keeping file slice",
                    sym,
                    src,
                    tag_q,
                    file_q,
                    rel * 100,
                )
                if reconciliation_deltas is not None:
                    reconciliation_deltas.append(
                        {
                            "symbol": sym,
                            "source": src,
                            "mode": "kept_file_disagree",
                            "before_tracked_qty": file_q,
                            "after_tracked_qty": sl.tracked_qty,
                            "tagged_tracked_qty": tag_q,
                            "relative_diff_pct": rel * 100.0,
                        }
                    )

    ledger.last_full_reconcile_ms = int(time.time() * 1000)
    ledger.last_error = ""
    logger.info(
        "LEDGER snapshot | open_orders=%s currencies=%s symbols=%d",
        ledger.open_orders_count,
        list(ledger.cash_free_quote.keys())[:6],
        len(ledger.symbols),
    )


def log_symbol_truth(logger, ledger: UnifiedLedger, symbol: str, mark_price: float) -> None:
    book = ledger.symbols.get(symbol)
    if not book:
        return
    parts = []
    for src, sl in book.slices.items():
        if sl.tracked_qty <= 1e-12:
            continue
        u = ledger.unrealized_quote(symbol, src, mark_price)
        parts.append(
            f"{src}:tracked={sl.tracked_qty:.6f} avg={sl.avg_entry:.6f} "
            f"real={sl.realized_pnl_quote:.2f} unreal~{u:.2f}"
        )
    logger.info(
        "LEDGER %s exch_total=%.6f free=%.6f untracked=%.6f | %s",
        symbol,
        book.exchange_total_base,
        book.exchange_free_base,
        book.untracked_base(),
        " ".join(parts) or "no_managed_slices",
    )
