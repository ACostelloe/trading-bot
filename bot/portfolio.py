from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Position:
    symbol: str
    qty: float
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: Optional[str] = None
    entry_notional: float = 0.0
    entry_fee: float = 0.0


@dataclass
class Portfolio:
    cash_usdt: float
    realized_pnl: float = 0.0
    daily_pnl: float = 0.0
    positions: dict[str, Position] | None = None

    def __post_init__(self) -> None:
        if self.positions is None:
            self.positions = {}

    def has_position(self, symbol: str) -> bool:
        return symbol in self.positions

    def get_position(self, symbol: str) -> Optional[Position]:
        return self.positions.get(symbol)

    def open_positions_count(self) -> int:
        return len(self.positions)

    def available_cash(self) -> float:
        return self.cash_usdt

    def mark_to_market(self, prices: dict[str, float]) -> float:
        total_equity = self.cash_usdt
        for symbol, pos in self.positions.items():
            last_price = prices.get(symbol, pos.entry_price)
            total_equity += pos.qty * last_price
        return total_equity

    def can_afford(self, entry_price: float, qty: float, fee_rate: float) -> bool:
        notional = entry_price * qty
        fee = notional * fee_rate
        total_cost = notional + fee
        return self.cash_usdt >= total_cost

    def open_position(
        self,
        symbol: str,
        qty: float,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        fee_rate: float,
        entry_time: Optional[str] = None,
    ) -> None:
        notional = qty * entry_price
        fee = notional * fee_rate
        total_cost = notional + fee

        if total_cost > self.cash_usdt:
            raise ValueError(
                f"Insufficient cash to open {symbol}: need {total_cost:.2f}, have {self.cash_usdt:.2f}"
            )

        self.cash_usdt -= total_cost
        self.positions[symbol] = Position(
            symbol=symbol,
            qty=qty,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            entry_time=entry_time,
            entry_notional=notional,
            entry_fee=fee,
        )

    def close_position(self, symbol: str, exit_price: float, fee_rate: float, qty: float | None = None) -> float:
        pos = self.positions.get(symbol)
        if not pos:
            return 0.0

        close_qty = float(pos.qty if qty is None else qty)
        if close_qty <= 0:
            return 0.0
        if close_qty > pos.qty:
            close_qty = pos.qty

        exit_notional = close_qty * exit_price
        exit_fee = exit_notional * fee_rate

        self.cash_usdt += exit_notional - exit_fee

        # Realize cost basis proportionally when closing partial size.
        ratio = close_qty / pos.qty if pos.qty > 0 else 0.0
        entry_notional_closed = pos.entry_notional * ratio
        entry_fee_closed = pos.entry_fee * ratio
        net_pnl = (exit_notional - exit_fee) - (entry_notional_closed + entry_fee_closed)

        self.realized_pnl += net_pnl
        self.daily_pnl += net_pnl

        remaining_qty = pos.qty - close_qty
        if remaining_qty <= 0:
            del self.positions[symbol]
        else:
            pos.qty = remaining_qty
            pos.entry_notional = max(0.0, pos.entry_notional - entry_notional_closed)
            pos.entry_fee = max(0.0, pos.entry_fee - entry_fee_closed)
        return net_pnl

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Portfolio":
        raw_positions = data.get("positions", {}) or {}
        if not raw_positions and data.get("position"):
            p = data["position"]
            sym = p["symbol"]
            raw_positions = {sym: dict(p)}

        positions: dict[str, Position] = {}
        for symbol, pos_data in raw_positions.items():
            if not isinstance(pos_data, dict):
                continue
            q = float(pos_data["qty"])
            ep = float(pos_data["entry_price"])
            en = float(pos_data.get("entry_notional", q * ep))
            ef = float(pos_data.get("entry_fee", pos_data.get("entry_fee_paid", 0.0)))
            positions[symbol] = Position(
                symbol=str(pos_data.get("symbol", symbol)),
                qty=q,
                entry_price=ep,
                stop_loss=float(pos_data["stop_loss"]),
                take_profit=float(pos_data["take_profit"]),
                entry_time=pos_data.get("entry_time"),
                entry_notional=en,
                entry_fee=ef,
            )

        return cls(
            cash_usdt=float(data["cash_usdt"]),
            realized_pnl=float(data.get("realized_pnl", 0.0)),
            daily_pnl=float(data.get("daily_pnl", 0.0)),
            positions=positions,
        )
