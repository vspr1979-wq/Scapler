"""In-process pub/sub EventBus + per-agent Inbox.

Messaging rules (plan.md §3.2):
  1. fire-and-forget hot path — ``publish`` is synchronous, never awaits consumers
  2. latest-wins coalescing per (topic, key) on hot topics → no queue bloat
  3. immutable msgspec envelopes → no locks, no copies
  4. priority lane — order/kill topics jump ahead of telemetry in every inbox
  5. single-writer state ownership lives in the agents, not the bus
"""
from __future__ import annotations

import asyncio
import itertools
from collections import deque

from .clock import mono_ns
from .messages import COALESCED_TOPICS, PRIORITY_TOPICS, WAKE_TOPIC, Envelope


class Inbox:
    """One per agent. Priority lane + latest-wins slots + FIFO lane."""

    __slots__ = ("_prio", "_norm", "_slots", "_ev", "_coalesced")

    def __init__(self, coalesced: frozenset[str] = COALESCED_TOPICS) -> None:
        self._prio: deque[Envelope] = deque()
        self._norm: deque[Envelope] = deque()
        self._slots: dict[tuple[str, str | None], Envelope] = {}
        self._ev = asyncio.Event()
        self._coalesced = coalesced

    # producer side (called synchronously from bus.publish) ──────────
    def push(self, env: Envelope) -> None:
        if env.topic in self._coalesced:
            self._slots[(env.topic, env.key)] = env      # latest wins
        elif env.topic in PRIORITY_TOPICS:
            self._prio.append(env)
        else:
            self._norm.append(env)
        self._ev.set()

    def wake(self) -> None:
        self._norm.append(Envelope(seq=-1, topic=WAKE_TOPIC, key=None,
                                   ts_mono=0, payload=None))
        self._ev.set()

    # consumer side ───────────────────────────────────────────────────
    def _pop(self) -> Envelope | None:
        if self._prio:
            return self._prio.popleft()
        if self._slots:
            return self._slots.pop(next(iter(self._slots)))
        if self._norm:
            return self._norm.popleft()
        return None

    async def get(self) -> Envelope:
        while True:
            self._ev.clear()
            env = self._pop()
            if env is not None:
                return env
            await self._ev.wait()

    def depth(self) -> int:
        return len(self._prio) + len(self._norm) + len(self._slots)


class BusStats:
    """Publish counters + per-agent hop latency (publish → consumed)."""

    __slots__ = ("published", "_hops")

    def __init__(self) -> None:
        self.published: dict[str, int] = {}
        self._hops: dict[str, deque[int]] = {}

    def record_publish(self, topic: str) -> None:
        self.published[topic] = self.published.get(topic, 0) + 1

    def record_hop(self, agent: str, dt_ns: int) -> None:
        d = self._hops.get(agent)
        if d is None:
            d = self._hops[agent] = deque(maxlen=1024)
        d.append(dt_ns)

    def p99_ms(self, agent: str) -> float:
        d = self._hops.get(agent)
        if not d:
            return 0.0
        s = sorted(d)
        return s[min(len(s) - 1, int(len(s) * 0.99))] / 1e6

    def p50_ms(self, agent: str) -> float:
        d = self._hops.get(agent)
        if not d:
            return 0.0
        s = sorted(d)
        return s[len(s) // 2] / 1e6


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[str, list[Inbox]] = {}
        self._seq = itertools.count(1)
        self.stats = BusStats()

    def subscribe(self, inbox: Inbox, *topics: str) -> None:
        for t in topics:
            self._subs.setdefault(t, []).append(inbox)

    def publish(self, topic: str, payload: object, key: str | None = None) -> Envelope:
        """Fire-and-forget. Never awaits. Safe to call from any agent coroutine."""
        env = Envelope(seq=next(self._seq), topic=topic, key=key,
                       ts_mono=mono_ns(), payload=payload)
        targets = self._subs.get(topic)
        if targets:
            for inbox in targets:
                inbox.push(env)
        self.stats.record_publish(topic)
        return env

    def subscriber_count(self, topic: str) -> int:
        return len(self._subs.get(topic, ()))
