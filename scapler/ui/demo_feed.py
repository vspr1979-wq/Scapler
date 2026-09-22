"""DEMO/REPLAY feed — synthetic fixture data for UI development & preview.

⚠ This module NEVER runs in live mode. It produces clearly-labelled
synthetic ticks (same fixtures the unit tests use) and a stub broker that
fills locally. The UI shows a permanent red "DEMO — SYNTHETIC DATA · STUB
BROKER · NO REAL ORDERS" banner whenever ``demo=True``. Recorded real-tick
replay (plan §0.3) is a separate, Phase-6 recorder concern.
"""
from __future__ import annotations

import asyncio
import time

from ..brokers.base import InstrumentMeta, MasterTable, OrderAck
from ..core.messages import InstrumentKey, OptionType, Tick

INDEX = "BANKNIFTY"
EXPIRY = "2026-09-29"
CENTER = 51200.0
STEP = 100.0
LOT = 30


def demo_master() -> MasterTable:
    options = {}
    y, m, d = EXPIRY[2:4], EXPIRY[5:7], EXPIRY[8:10]
    for i in range(-10, 11):
        strike = CENTER + i * STEP
        for opt in OptionType:
            c = "C" if opt is OptionType.CE else "P"
            ik = InstrumentKey(exchange="NSE_FO", token=f"{int(strike)}{c}",
                               symbol=f"{INDEX}{y}{m}{d}{c}{int(strike)}",
                               strike=strike, expiry=EXPIRY, option_type=opt)
            options[ik.feed_key] = InstrumentMeta(
                instrument=ik, lot_size=LOT, index_symbol=INDEX,
                expiry=EXPIRY)
    indices = {INDEX: f"NSE_INDEX|{INDEX}"}
    return MasterTable(options=options, indices=indices, source="demo")


class DemoBroker:
    """Local fill simulator for DEMO mode only (no network, no orders)."""
    name = "demo-stub"

    def __init__(self, master: MasterTable) -> None:
        self.master = master
        self.orders: list = []

    async def place_market_order(self, req) -> OrderAck:
        self.orders.append(req)
        await asyncio.sleep(0.005)
        return OrderAck(client_id=req.client_id,
                        broker_order_id="DEMO-" + req.client_id,
                        status="FILLED")


def _t0() -> int:
    return (time.time_ns() // 60_000_000_000) * 60 + 120


def demo_stream(min_t0: int = 0) -> list[Tick]:
    """One scripted 60-minute demo day on a future minute grid.

    0-29 up-leg → CE signal on a volume-spike candle → ladder exit (T1 ✓ →
    trail SL=BE → T2 ✓ → T3 flat); 30-59 pullback → setup BREAK → FSM
    disarms/re-arms (no-spam rule visible); next cycle signals again.
    Spot mean-reverts around the ATM (window never drifts into unfed
    strikes); ATM±2 strikes are quoted with moneyness-shaped premiums and
    deltas so the delta-band picker has a real choice. Volume spikes every
    4th bar gate the setup exactly like the real rule. Grid is always in the
    future and chained across cycles so the candle wall-clock flush never
    races and no tick is ever "late".
    """
    T0 = max(_t0(), min_t0)
    T0 = (T0 // 60) * 60
    SK = f"NSE_INDEX|{INDEX}"
    strikes = [CENTER + j * STEP for j in range(-2, 3)]
    ticks: list[Tick] = []
    px, cum = CENTER - 150.0, 5000.0

    def phase_ch(i: int) -> float:
        if i < 30:                              # up-leg
            return 25.0 if i % 2 == 0 else -10.0
        return -20.0 if i % 2 == 0 else 8.0     # pullback (setup break)

    for i in range(60):
        base = T0 + i * 60
        ch = phase_ch(i)
        px = px * 0.95 + CENTER * 0.05          # keep cycles bounded
        o = px
        c = px + ch
        wick = 5.0
        h, l = max(o, c) + wick, min(o, c) - wick
        v = 900.0 if i % 4 == 3 else 300.0      # volume spike gates the setup
        for sec, ltp, frac in ((1, o, 0.0), (20, h, 1 / 3),
                               (40, l, 1 / 3), (55, c, 1 / 3)):
            cum += frac * v
            ticks.append(Tick(key=SK, exch_ts_ns=(base + sec) * 10**9,
                              ltp=ltp, volume=cum))
        for j, strike in enumerate(strikes):
            for side, sign in (("C", 1.0), ("P", -1.0)):
                m = (c - strike) * sign                     # moneyness
                prem = max(3.0, 148.2 + m * 0.7 - abs(j) * 45.0)
                d = min(0.95, max(0.05, 0.53 + m * 0.004)) * sign
                g = max(0.0005, 0.0045 - abs(j) * 0.0008)
                key = f"NSE_FO|{int(strike)}{side}"
                ticks.append(Tick(key=key,
                                  exch_ts_ns=(base + 56 + j) * 10**9,
                                  ltp=prem, bid=prem - 0.05, ask=prem + 0.05,
                                  delta=d, gamma=g))
        px = c
    ticks.append(Tick(key=SK, exch_ts_ns=(T0 + 60 * 60 + 1) * 10**9,
                      ltp=px, volume=cum))
    return ticks


class DemoFeedHandle:
    """Paced async iterator over demo_stream(); loops forever.

    30 ms pacing ≈ one 60-minute demo day every ~9 s, slow enough for the
    bus coalescer and for a human to watch the ladder work.
    """

    def __init__(self, pace_s: float = 0.015, cycle_pause_s: float = 2.0):
        self.pace_s = pace_s
        self.cycle_pause_s = cycle_pause_s
        self._t: list[Tick] = []
        self._next_t0 = 0        # chain cycles on ONE forward timeline: a
        # regenerated grid must never fall behind the candle builder's last
        # bucket, or every tick would be dropped as "late".
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> Tick:
        if self.closed:
            raise StopAsyncIteration
        if not self._t:
            await asyncio.sleep(self.cycle_pause_s)
            if self.closed:
                raise StopAsyncIteration
            self._t = demo_stream(self._next_t0)
            if self._t:
                self._next_t0 = self._t[-1].exch_ts_ns // 10**9 + 61
        await asyncio.sleep(self.pace_s)
        return self._t.pop(0)

    async def close(self) -> None:
        self.closed = True


class DemoAdapter:
    name = "demo"

    def __init__(self, master: MasterTable) -> None:
        self.master = master

    async def open_feed(self, keys, mode="full"):
        return DemoFeedHandle()
