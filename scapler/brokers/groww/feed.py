"""Groww live feed handle.

Design (plan §4.3): the official ``growwapi.GrowwFeed`` WS client is the
preferred transport on the Windows box (callback → asyncio bridge). The
dependency-free baseline implemented here is the documented REST LTP batch
endpoint (≤50 symbols/call, Live-Data bucket 10 req/s): one call per segment
per cycle → ~23 keys in 2 calls at 4 Hz.

Consequences, documented and accepted for v1:
  * poll mode carries LTP + local receive timestamp (no exchange ts in batch)
  * no greeks/depth in batch → strike pick uses the ATM fallback (plan §5-F5)
  * exits remain tick-driven on these LTPs (4 Hz is inside the SL budget for
    5-30 pt targets; WS mode removes the caveat entirely)
"""
from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

from ...core.messages import Tick
from .rest import GrowwRest


class _PollHandle:
    def __init__(self, rest: GrowwRest,
                 groups: dict[str, list[tuple[str, str]]],   # segment → [(exch_sym, key)]
                 interval: float) -> None:
        self._rest = rest
        self._groups = groups
        self._interval = interval
        self._last: dict[str, float] = {}
        self._q: asyncio.Queue[Tick | None] = asyncio.Queue(maxsize=50_000)
        self._task = asyncio.get_running_loop().create_task(self._pump())
        self.cycles = 0

    async def _pump(self) -> None:
        try:
            while True:
                for segment, items in self._groups.items():
                    try:
                        prices = await self._rest.ltp_batch(
                            segment, [sym for sym, _ in items])
                    except RuntimeError:
                        continue                    # stale cycle, watchdog sees gap
                    now = time.time_ns()
                    for sym, key in items:
                        px = prices.get(sym)
                        if px is None or self._last.get(key) == px:
                            continue
                        self._last[key] = px
                        if self._q.full():
                            try:
                                self._q.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                        self._q.put_nowait(Tick(key=key, exch_ts_ns=now,
                                                ltp=px))
                self.cycles += 1
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            self._q.put_nowait(None)

    def __aiter__(self) -> AsyncIterator[Tick]:
        return self

    async def __anext__(self) -> Tick:
        item = await self._q.get()
        if item is None:
            raise StopAsyncIteration
        return item

    async def close(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass


class GrowwPollFeed:
    def __init__(self, rest: GrowwRest, interval: float = 0.25) -> None:
        self._rest = rest
        self._interval = interval

    async def open(self, subs: dict[str, tuple[str, str, str]]) -> _PollHandle:
        """subs: feed_key → (exchange, segment, trading_symbol)."""
        groups: dict[str, list[tuple[str, str]]] = {}
        for key, (exch, segment, symbol) in subs.items():
            groups.setdefault(segment, []).append((f"{exch}_{symbol}", key))
        return _PollHandle(self._rest, groups, self._interval)
