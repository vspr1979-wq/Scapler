"""CandleBuilderAgent — ticks → CLOSED 1-min bars only (plan §5-F6/F7).

Emits ``candle.closed`` exactly once per completed minute bar:
  * when the first tick of the NEXT bar arrives, and
  * via a 5 s flush task when the feed goes quiet after a bar end
    (so the last bar of a quiet spell still closes).
Minute buckets are epoch-minute aligned; IST offset is whole minutes so the
bucket grid equals the IST grid.

Volume: feed cumulative volume delta when the feed provides it (options);
index spot feeds carry none → per-tick activity proxy (1 per tick). The
proxy keeps VWAP/vol-ratio meaningful without inventing market data.
"""
from __future__ import annotations

import asyncio
import dataclasses
import time

from ..core.clock import mono_ns
from ..core.messages import CandleClosed, Topic
from .base_imports import Agent

TF_NS = 60 * 10**9
FLUSH_GRACE_NS = 2 * 10**9


@dataclasses.dataclass
class _Bar:
    bucket: int
    o: float
    h: float
    l: float
    c: float
    v: float


class CandleBuilderAgent(Agent):
    name = "candle"
    topics = (Topic.TICK_RAW,)

    def __init__(self, bus, key: str, tf_ns: int = TF_NS,
                 flush_s: float = 5.0) -> None:
        super().__init__(bus)
        self.key = key
        self.tf = tf_ns
        self._flush_s = flush_s
        self._bar: _Bar | None = None
        self._last_cum_vol: float | None = None
        self.candles_closed = 0

    async def on_start(self) -> None:
        self._flush = asyncio.get_running_loop().create_task(self._flush_loop())

    async def _flush_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._flush_s)
                self._maybe_flush(time.time_ns())
        except asyncio.CancelledError:
            raise

    def _maybe_flush(self, now_ns: int) -> None:
        if self._bar is not None and now_ns >= self._bar.bucket + self.tf + FLUSH_GRACE_NS:
            self._close_bar()

    async def on_message(self, env) -> None:
        tick = env.payload
        if tick.key != self.key:
            return
        bucket = (tick.exch_ts_ns // self.tf) * self.tf
        if self._bar is not None and bucket < self._bar.bucket:
            return                                     # late tick for a closed bar
        if tick.volume > 0:
            # cumulative feed volume (vtt): bar volume = deltas; the very
            # first tick of the session only establishes the baseline
            dv = 0.0 if self._last_cum_vol is None else \
                max(0.0, tick.volume - self._last_cum_vol)
            self._last_cum_vol = tick.volume
        else:
            dv = 1.0                                  # activity proxy
        if self._bar is None or bucket > self._bar.bucket:
            if self._bar is not None:
                self._close_bar()
            self._bar = _Bar(bucket=bucket, o=tick.ltp, h=tick.ltp,
                             l=tick.ltp, c=tick.ltp, v=dv)
        else:
            b = self._bar
            b.h = max(b.h, tick.ltp)
            b.l = min(b.l, tick.ltp)
            b.c = tick.ltp
            b.v += dv

    def _close_bar(self) -> None:
        b = self._bar
        self._bar = None
        self.candles_closed += 1
        self.publish(Topic.CANDLE_CLOSED, CandleClosed(
            key=self.key, tf="1m", open_ts=b.bucket, close_ts=b.bucket + self.tf,
            o=b.o, h=b.h, l=b.l, c=b.c, volume=b.v))

    async def on_stop(self) -> None:
        if getattr(self, "_flush", None) is not None:
            self._flush.cancel()
            try:
                await self._flush
            except (asyncio.CancelledError, Exception):
                pass
