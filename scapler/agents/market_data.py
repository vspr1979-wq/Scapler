"""MarketDataAgent — owns the broker feed handle, publishes tick.raw.

Hot path: decode already happened in the adapter; here we stamp stats and
publish (bus coalesces latest-wins per key downstream). Ticks/s + staleness
are exposed for the status bar and the Watchdog (Phase 6).
"""
from __future__ import annotations

import asyncio

from ..core.clock import mono_ns
from ..core.messages import Topic
from .base_imports import Agent


class MarketDataAgent(Agent):
    name = "market_data"
    topics: tuple[str, ...] = (Topic.WINDOW_REBUILT,)

    def __init__(self, bus, adapter, keys: list[str], mode: str = "full") -> None:
        super().__init__(bus)
        self.adapter = adapter
        self.keys = keys
        self.mode = mode
        self.handle = None
        self.ticks_total = 0
        self.last_tick_mono = 0
        self._window_count = 0
        self._window_start = mono_ns()
        self.ticks_per_s = 0.0

    async def on_start(self) -> None:
        self.handle = await self.adapter.open_feed(self.keys, self.mode)
        self._pump = asyncio.get_running_loop().create_task(self._run())

    async def on_message(self, env) -> None:
        # window.rebuilt (plan §3.2): adopt the strike-window footprint —
        # hot-resubscribe when the adapter supports it, else next reconnect.
        if env.topic == Topic.WINDOW_REBUILT:
            self.keys = list(env.payload["keys"])
            update = getattr(self.adapter, "update_subs", None)
            if update is not None and self.handle is not None:
                await update(self.keys)

    async def _run(self) -> None:
        async for tick in self.handle:
            self.ticks_total += 1
            self._window_count += 1
            self.last_tick_mono = mono_ns()
            dt = (self.last_tick_mono - self._window_start) / 1e9
            if dt >= 1.0:
                self.ticks_per_s = self._window_count / dt
                self._window_count = 0
                self._window_start = self.last_tick_mono
            self.publish(Topic.TICK_RAW, tick, key=tick.key)

    async def on_stop(self) -> None:
        if getattr(self, "_pump", None) is not None:
            self._pump.cancel()
            try:
                await self._pump
            except (asyncio.CancelledError, Exception):
                pass
        if self.handle is not None:
            await self.handle.close()

    def stale_for_s(self) -> float:
        if not self.last_tick_mono:
            return float("inf")
        return (mono_ns() - self.last_tick_mono) / 1e9
