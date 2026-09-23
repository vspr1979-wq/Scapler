"""Broker-agnostic contracts (plan.md §4.1).

Adapters map broker-specific wires onto these canonical types. The OrderAgent
applies SideGuard before calling ``place_market_order``; adapters themselves
only ever receive an ``OrderRequest`` whose intent is BUY_TO_OPEN/SELL_TO_CLOSE.
"""
from __future__ import annotations

import abc
import dataclasses
from typing import AsyncIterator, Protocol

from ..core.messages import InstrumentKey, OptionType, OrderRequest, Tick


@dataclasses.dataclass(frozen=True)
class OrderAck:
    client_id: str
    broker_order_id: str
    status: str


@dataclasses.dataclass(frozen=True)
class Position:
    feed_key: str
    qty: int               # signed; longs positive
    avg_price: float


@dataclasses.dataclass(frozen=True)
class InstrumentMeta:
    instrument: InstrumentKey
    lot_size: int
    index_symbol: str      # NIFTY / BANKNIFTY / SENSEX / FINNIFTY / MIDCPNIFTY
    expiry: str            # ISO date


@dataclasses.dataclass(frozen=True)
class MasterTable:
    """Parsed exchange instrument master. Source of truth for tokens, lots,
    expiries. Strike STEP stays a Settings value (plan.md §1)."""
    options: dict[str, InstrumentMeta]     # feed_key -> meta
    indices: dict[str, str]                # canonical index -> feed_key
    source: str = ""

    def expiries(self, index: str) -> list[str]:
        return sorted({m.expiry for m in self.options.values()
                       if m.index_symbol == index})

    def nearest_expiry(self, index: str, today: str) -> str | None:
        live = [e for e in self.expiries(index) if e >= today]
        return min(live) if live else None

    def lot(self, index: str, expiry: str) -> int | None:
        for m in self.options.values():
            if m.index_symbol == index and m.expiry == expiry:
                return m.lot_size
        return None

    def strikes(self, index: str, expiry: str,
                opt: OptionType | None = None) -> list[float]:
        s = {m.instrument.strike for m in self.options.values()
             if m.index_symbol == index and m.expiry == expiry
             and (opt is None or m.instrument.option_type is opt)}
        return sorted(s)

    def meta(self, index: str, expiry: str, strike: float,
             opt: OptionType) -> InstrumentMeta | None:
        for m in self.options.values():
            ik = m.instrument
            if (m.index_symbol == index and m.expiry == expiry
                    and ik.strike == strike and ik.option_type is opt):
                return m
        return None


class FeedHandle(Protocol):
    """Async iterator of canonical Ticks; close() tears the socket down."""
    def __aiter__(self) -> AsyncIterator[Tick]: ...
    async def close(self) -> None: ...


class BrokerAdapter(abc.ABC):
    name: str = "base"

    @abc.abstractmethod
    def auth_url(self, state: str) -> str: ...
    @abc.abstractmethod
    async def login(self, code: str) -> None: ...
    @abc.abstractmethod
    async def load_master(self, today: str | None = None) -> MasterTable: ...
    @abc.abstractmethod
    async def open_feed(self, keys: list[str], mode: str = "full") -> FeedHandle: ...
    @abc.abstractmethod
    async def place_market_order(self, req: OrderRequest) -> OrderAck: ...
    @abc.abstractmethod
    async def cancel_order(self, broker_order_id: str) -> None: ...
    @abc.abstractmethod
    async def positions(self) -> list[Position]: ...
    @abc.abstractmethod
    async def disconnect(self) -> None: ...
