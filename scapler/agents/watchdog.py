"""WatchdogAgent — stale-feed reconnect + square-off + orphan policy (F8).

1 Hz loop (off the hot path):
  * feed staleness > ``stale_feed_s`` while the session is live →
    publish ``feed.reconnect`` (MarketData re-opens the handle); retries at
    most every ``reconnect_stale_s``
  * IST wall clock ≥ ``square_off`` → publish ``kill.switch(source=
    watchdog)`` once: PositionExit force-closes with reason SQUARE_OFF and
    Risk blocks entries for the rest of the day
  * orphan policy hook: on start, ``orphan_check`` callback (runtime injects
    broker positions lookup when live) — square_off | alert_only
  * publishes ``watchdog.status`` every second for the status bar.
"""
from __future__ import annotations

import asyncio

from ..core.clock import ist_hhmm, mono_ns
from ..core.config import Settings
from ..core.messages import KillSwitch, Topic
from .base_imports import Agent


def _ist_date() -> str:
    from ..core.clock import ist_now
    return ist_now().date().isoformat()


class WatchdogAgent(Agent):
    name = "watchdog"
    topics = (Topic.TICK_RAW, Topic.KILL_SWITCH)

    def __init__(self, bus, settings: Settings, clock_fn=None,
                 orphan_check=None, date_fn=None) -> None:
        super().__init__(bus)
        self.cfg = settings
        self._clock = clock_fn or ist_hhmm
        self._date = date_fn
        self.orphan_check = orphan_check      # async fn() -> list[Position]
        self.reconnects = 0
        self.squared_off = False
        self.killed = False
        self.session_day = ""
        self._last_tick = 0
        self._last_reconnect = 0.0
        self.feed_latency_ms = 0.0

    async def on_start(self) -> None:
        if self.orphan_check is not None:
            try:
                orphans = await self.orphan_check()
            except Exception:
                orphans = []
            if orphans and self.cfg.orphan_policy == "square_off":
                self.publish(Topic.KILL_SWITCH, KillSwitch(source="watchdog"))
        self._loop = asyncio.get_running_loop().create_task(self._watch())

    async def _watch(self) -> None:
        try:
            while True:
                await asyncio.sleep(1.0)
                now = mono_ns()
                stale_s = (now - self._last_tick) / 1e9 if self._last_tick \
                    else float("inf")
                session_live = self.cfg.entry_window[0] <= self._clock() \
                    <= "23:59"
                if self._last_tick and stale_s > self.cfg.stale_feed_s \
                        and not self.killed and session_live \
                        and (now / 1e9 - self._last_reconnect) \
                        >= self.cfg.reconnect_stale_s:
                    self._last_reconnect = now / 1e9
                    self.reconnects += 1
                    self.publish(Topic.FEED_RECONNECT,
                                 f"stale {stale_s:.0f}s")
                today = self._date() if self._date else _ist_date()
                if self.session_day and today != self.session_day:
                    # new IST day → daily breakers/session roll everywhere
                    self.session_day = today
                    self.squared_off = False
                    self.killed = False
                    self.reconnects = 0
                    self.publish(Topic.SESSION_NEW_DAY, today)
                elif not self.session_day:
                    self.session_day = today
                hhmm = self._clock()
                if not self.squared_off and hhmm >= self.cfg.square_off \
                        and not self.killed:
                    self.squared_off = True
                    self.publish(Topic.KILL_SWITCH,
                                 KillSwitch(source="watchdog"))
                self.publish(Topic.WATCHDOG_STATUS, {
                    "stale_s": None if stale_s == float("inf")
                    else round(stale_s, 1),
                    "reconnects": self.reconnects,
                    "squared_off": self.squared_off,
                    "feed_latency_ms": round(self.feed_latency_ms, 1),
                    "square_off_at": self.cfg.square_off})
        except asyncio.CancelledError:
            raise

    async def on_message(self, env) -> None:
        if env.topic == Topic.TICK_RAW:
            self._last_tick = mono_ns()
            exch = env.payload.exch_ts_ns
            if exch > 10**18:                  # sane epoch-ns stamp only
                import time
                self.feed_latency_ms = max(
                    0.0, (time.time_ns() - exch) / 1e6)
            return
        if env.topic == Topic.KILL_SWITCH:
            self.killed = True

    async def on_stop(self) -> None:
        if getattr(self, "_loop", None) is not None:
            self._loop.cancel()
            try:
                await self._loop
            except asyncio.CancelledError:
                pass
