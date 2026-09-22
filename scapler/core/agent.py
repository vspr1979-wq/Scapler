"""Agent base class: asyncio task + inbox + supervised restart + heartbeat.

An agent owns exactly one job and one piece of state (plan.md §3.1).
Agents never import each other — only the EventBus and the message catalog.
Logging stays OFF the hot path: per-message work is a dispatch table lookup;
only supervision events (crash/restart/stop) touch ``logging``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .clock import mono_ns
from .messages import WAKE_TOPIC, AgentHealth

if TYPE_CHECKING:  # pragma: no cover
    from .bus import EventBus

log = logging.getLogger(__name__)


class Agent:
    name: str = "agent"
    topics: tuple[str, ...] = ()

    def __init__(self, bus: "EventBus") -> None:
        self.bus = bus
        from .bus import Inbox  # local import avoids cycle at module load
        self.inbox = Inbox()
        self.state = "INIT"
        self.restarts = 0
        self.last_beat = mono_ns()
        self.last_error: BaseException | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        if self.topics:
            bus.subscribe(self.inbox, *self.topics)

    # ── overrides ────────────────────────────────────────────────────
    async def on_start(self) -> None: ...
    async def on_message(self, env) -> None: ...
    async def on_stop(self) -> None: ...

    # ── lifecycle ───────────────────────────────────────────────────
    async def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._supervised(), name=f"agent:{self.name}")
        return self._task

    async def _supervised(self) -> None:
        backoff = 0.05
        while not self._stopping:
            try:
                self.state = "RUN"
                await self.on_start()
                while not self._stopping:
                    env = await self.inbox.get()
                    if env.topic == WAKE_TOPIC:
                        continue
                    await self.on_message(env)
                    self.last_beat = mono_ns()
                    self.bus.stats.record_hop(self.name, mono_ns() - env.ts_mono)
                await self.on_stop()
                self.state = "STOP"
                return
            except asyncio.CancelledError:
                self.state = "STOP"
                raise
            except Exception as exc:  # crash → supervised restart w/ backoff
                self.last_error = exc
                self.restarts += 1
                self.state = f"RESTARTING:{type(exc).__name__}"
                log.error("agent %s crashed (%s); restart #%d in %.0f ms",
                          self.name, exc, self.restarts, backoff * 1000)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 1.0)

    async def stop(self) -> None:
        self._stopping = True
        self.inbox.wake()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    # ── helpers ──────────────────────────────────────────────────────
    def publish(self, topic: str, payload: object, key: str | None = None):
        return self.bus.publish(topic, payload, key)

    def health(self) -> AgentHealth:
        return AgentHealth(
            name=self.name,
            state=self.state,
            restarts=self.restarts,
            inbox_depth=self.inbox.depth(),
            p99_ms=round(self.bus.stats.p99_ms(self.name), 3),
            beat_age_s=round((mono_ns() - self.last_beat) / 1e9, 2),
        )
