"""DEMO/REPLAY feed — synthetic fixture data for UI development & preview.

⚠ This module NEVER runs in live mode. It produces clearly-labelled
synthetic ticks (same fixtures the unit tests use) and a stub broker that
fills locally. The UI shows a permanent red "DEMO — SYNTHETIC DATA · STUB
BROKER · NO REAL ORDERS" banner whenever ``demo=True``. Recorded real-tick
replay (plan §0.3) is a separate, recorder-based concern.

All FIVE indices are streamed simultaneously (spots + ATM±2 option quotes
each) so the instant index switch — user directive, Phase 6 — has live data
waiting for every dropdown entry, exactly like a real feed subscription.
"""
from __future__ import annotations

import asyncio
import time

from ..brokers.base import InstrumentMeta, MasterTable, OrderAck
from ..core.messages import InstrumentKey, OptionType, Tick

EXPIRY = "2026-09-29"

#        name          exch    center    step   lot  abbr
INDICES = (
    ("NIFTY",       "NSE",  24500.0,   50.0,  65, "N"),
    ("SENSEX",      "BSE",  80100.0,  100.0,  20, "S"),
    ("BANKNIFTY",   "NSE",  51200.0,  100.0,  30, "B"),
    ("FINNIFTY",    "NSE",  24050.0,   50.0,  60, "F"),
    ("MIDCPNIFTY",  "NSE",  11820.0,   25.0, 120, "M"),
)


def demo_master(radius: int = 10) -> MasterTable:
    options: dict[str, InstrumentMeta] = {}
    indices: dict[str, str] = {}
    y, m, d = EXPIRY[2:4], EXPIRY[5:7], EXPIRY[8:10]
    for name, exch, center, step, lot, abbr in INDICES:
        indices[name] = f"{exch}_INDEX|{name}"
        for i in range(-radius, radius + 1):
            strike = center + i * step
            for opt in OptionType:
                c = "C" if opt is OptionType.CE else "P"
                ik = InstrumentKey(
                    exchange=f"{exch}_FO", token=f"{abbr}{int(strike)}{c}",
                    symbol=f"{name}{y}{m}{d}{c}{int(strike)}",
                    strike=strike, expiry=EXPIRY, option_type=opt)
                options[ik.feed_key] = InstrumentMeta(
                    instrument=ik, lot_size=lot, index_symbol=name,
                    expiry=EXPIRY)
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
    """One scripted 60-minute demo day on a future minute grid, all indices.

    Each index runs an up-leg/pullback cycle (phase-staggered by 6 minutes
    so the five FSMs sit in different states — switching index instantly
    shows a different picture). Volume spikes every 4th bar gate the setup;
    spot mean-reverts to its center so windows stay inside the fed strikes;
    premium tracks moneyness with delta/gamma for the delta-band picker.
    Grid is chained across cycles — never "late" for the candle builder.
    """
    T0 = max(_t0(), min_t0)
    T0 = (T0 // 60) * 60
    ticks: list[Tick] = []
    state = {name: center - 150.0
             for name, _, center, _, _, _ in INDICES}
    cum = {name: 5000.0 for name, *_ in INDICES}

    for i in range(60):
        base = T0 + i * 60
        for k, (name, exch, center, step, lot, abbr) in enumerate(INDICES):
            off = k * 6                          # phase stagger per index
            j = (i + off) % 60
            if j < 30:                           # up-leg
                ch = 25.0 if j % 2 == 0 else -10.0
            else:                                # pullback (setup break)
                ch = -20.0 if j % 2 == 0 else 8.0
            px = state[name] * 0.95 + center * 0.05
            o = px
            c = px + ch
            wick = 5.0
            h, l = max(o, c) + wick, min(o, c) - wick
            v = 900.0 if j % 4 == 3 else 300.0
            SK = f"{exch}_INDEX|{name}"
            for sec, ltp, frac in ((1, o, 0.0), (20, h, 1 / 3),
                                   (40, l, 1 / 3), (55, c, 1 / 3)):
                cum[name] += frac * v
                ticks.append(Tick(key=SK, exch_ts_ns=(base + sec) * 10**9,
                                  ltp=ltp, volume=cum[name]))
            for jj in range(-2, 3):
                strike = center + jj * step
                for side, sign in (("C", 1.0), ("P", -1.0)):
                    mny = (c - strike) * sign
                    prem = max(3.0, 148.2 + mny * 0.7 - abs(jj) * 45.0)
                    dl = min(0.95, max(0.05, 0.53 + mny * 0.004)) * sign
                    g = max(0.0005, 0.0045 - abs(jj) * 0.0008)
                    key = f"{exch}_FO|{abbr}{int(strike)}{side}"
                    ticks.append(Tick(
                        key=key, exch_ts_ns=(base + 50 + jj * 2 +
                                             (1 if sign < 0 else 0)) * 10**9,
                        ltp=prem, bid=prem - 0.05, ask=prem + 0.05,
                        delta=dl, gamma=g))
            state[name] = c
    for name, exch, *_ in INDICES:
        ticks.append(Tick(key=f"{exch}_INDEX|{name}",
                          exch_ts_ns=(T0 + 60 * 60 + 1) * 10**9,
                          ltp=state[name], volume=cum[name]))
    return ticks


class DemoFeedHandle:
    """Paced async iterator over demo_stream(); loops forever.

    8 ms pacing ≈ one 60-minute demo day (all five indices, 4200+ ticks)
    every ~35 s — slow enough for the bus coalescer and for a human to
    watch the ladder work.
    """

    def __init__(self, pace_s: float = 0.008, cycle_pause_s: float = 2.0):
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
