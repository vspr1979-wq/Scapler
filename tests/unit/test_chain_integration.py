"""Phase-3 integration: ticks → MarketData → Candle → Indicator → setup → FSM.

Asserts (a) plumbing parity: agent-chain indicators == direct engine on the
same closed candles to 1e-9; (b) F6 through the real chain: a 44-candle
uptrend with volume spikes produces EXACTLY ONE CE signal — later spikes stay
silent because re-arm requires a break that never comes.
"""
import asyncio

import pytest

from scapler.agents.candle import CandleBuilderAgent
from scapler.agents.indicator import IndicatorAgent
from scapler.agents.market_data import MarketDataAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings, Setup
from scapler.core.messages import (
    CandleClosed, OptionType as OT, SignalStateEnum as S, Tick, Topic,
)
from scapler.strategy.indicators import IndicatorEngine
from scapler.strategy.setup import setup_broken, setup_valid
from scapler.strategy.signal_fsm import SignalEngine
from tests._feed import FakeAdapter

import time as _time

KEY = "NSE_INDEX|NIFTY 50"
# future minute grid: the wall-clock flush timer must not race the sequence
T0 = (_time.time_ns() // 60_000_000_000) * 60 + 120


def crafted_candles(n=44):
    """+0.5/-0.2 alternating drift (RSI≈71) with a volume spike every 4th bar."""
    rows = []
    px = 100.0
    for i in range(n):
        o = px
        ch = 0.5 if i % 2 == 0 else -0.2
        c = px + ch
        h = max(o, c) + 0.1
        l = min(o, c) - 0.1
        v = 300.0 if i % 4 == 3 else 100.0
        rows.append((o, h, l, c, v))
        px = c
    return rows


def candles_to_ticks(rows):
    ticks, cum = [], 5000.0        # session volume already running pre-window
    for i, (o, h, l, c, v) in enumerate(rows):
        base = T0 + i * 60
        # open tick only sets the volume baseline (dv=0); the bar's volume
        # arrives with the h/l/c ticks → bar volume == v exactly
        for sec, ltp, frac in ((1, o, 0.0), (20, h, 1 / 3),
                               (40, l, 1 / 3), (55, c, 1 / 3)):
            cum += frac * v
            ticks.append(Tick(key=KEY, exch_ts_ns=(base + sec) * 10**9,
                              ltp=ltp, volume=cum))
    # one tick in the next minute closes the final bar
    ticks.append(Tick(key=KEY, exch_ts_ns=(T0 + len(rows) * 60 + 1) * 10**9,
                      ltp=rows[-1][3], volume=cum))
    return ticks


def settings():
    return Settings(signal_ttl_candles=3,
                    setup=Setup(atr_min={k: 0.5 for k in
                                         ("NIFTY", "BANKNIFTY", "SENSEX",
                                          "FINNIFTY", "MIDCPNIFTY")}))


class Watcher:
    """Stand-in for SignalAgent wiring (agent shell lands in Phase 4)."""

    def __init__(self, bus: EventBus, cfg: Settings) -> None:
        self.cfg = cfg
        self.fsm = SignalEngine(cfg)
        self.last_candle: CandleClosed | None = None
        self.signals = []
        self.ready = []
        self._inbox_c = Inbox()
        self._inbox_i = Inbox()
        bus.subscribe(self._inbox_c, Topic.CANDLE_CLOSED)
        bus.subscribe(self._inbox_i, Topic.INDICATORS_READY)
        self._task = None

    async def start(self):
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while True:
            env = await self._inbox_c.get()
            self.last_candle = env.payload
            env = await self._inbox_i.get()
            ind = env.payload
            self.ready.append(ind)
            c = self.last_candle
            atr_min = self.cfg.setup.atr_min.get("NIFTY", 0.0)
            valid = setup_valid(OT.CE, ind, c, self.cfg.setup, atr_min)
            broken = setup_broken(OT.CE, ind, c)
            for st in self.fsm.on_candle(c.close_ts,
                                         {OT.CE: valid, OT.PE: False},
                                         {OT.CE: broken, OT.PE: False}):
                if st.new is S.SIGNALED:
                    self.signals.append(st)

    async def stop(self):
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass


def approx(x):
    return pytest.approx(x, rel=1e-9, abs=1e-12)


async def test_chain_one_signal_per_episode_and_indicator_parity():
    cfg = settings()
    rows = crafted_candles()
    bus = EventBus()
    adapter = FakeAdapter(candles_to_ticks(rows))
    md = MarketDataAgent(bus, adapter, [KEY], mode="ltpc")
    cb = CandleBuilderAgent(bus, KEY)
    ia = IndicatorAgent(bus)
    w = Watcher(bus, cfg)
    for a in (md, cb, ia):
        await a.start()
    await w.start()
    await asyncio.sleep(0.6)
    await w.stop()
    for a in (md, cb, ia):
        await a.stop()

    # (a) plumbing parity vs direct engine on identical closed candles
    closed = [CandleClosed(key=KEY, tf="1m", open_ts=T0 + i * 60,
                           close_ts=T0 + (i + 1) * 60, o=o, h=h, l=l, c=c,
                           volume=v)
              for i, (o, h, l, c, v) in enumerate(rows)]
    direct = IndicatorEngine(KEY)
    ref = [direct.on_candle(c) for c in closed]
    assert len(w.ready) == len(ref)
    for got, exp in zip(w.ready, ref):
        assert got.vwap == approx(exp.vwap)
        assert got.ema9 == approx(exp.ema9)
        assert got.ema21 == approx(exp.ema21)
        assert got.rsi == approx(exp.rsi)
        assert got.atr == approx(exp.atr)
        assert got.vol_ratio == approx(exp.vol_ratio)

    # (b) exactly one signal across the whole trending episode
    assert len(w.signals) == 1
    assert w.signals[0].side is OT.CE


async def test_market_data_stats():
    bus = EventBus()
    adapter = FakeAdapter(candles_to_ticks(crafted_candles(3)))
    md = MarketDataAgent(bus, adapter, [KEY])
    await md.start()
    await asyncio.sleep(0.2)
    await md.stop()
    assert md.ticks_total == 13          # 3 bars × 4 ticks + closing tick
    assert adapter.handle.closed
