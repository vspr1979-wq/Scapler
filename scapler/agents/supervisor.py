"""SupervisorAgent — agent health fan-out + kill orchestration hook (F10).

Owns no trading logic. Every ``beat_s`` it publishes one ``agent.health``
per registered agent (the Agent base already restarts crashed tasks with
backoff; the supervisor makes that visible). Agents register by object
reference handed in by the runtime — no agent imports another agent.
Full crash-injection + watchdog orchestration lands in Phase 6.
"""
from __future__ import annotations

import asyncio

from ..core.messages import Topic
from .base_imports import Agent


class SupervisorAgent(Agent):
    name = "supervisor"
    topics = (Topic.KILL_SWITCH,)

    def __init__(self, bus, agents=(), beat_s: float = 1.0) -> None:
        super().__init__(bus)
        self.registry: list[Agent] = list(agents)
        self.beat_s = beat_s
        self.killed = False

    def register(self, agent: "Agent") -> None:
        if agent not in self.registry:
            self.registry.append(agent)

    async def on_start(self) -> None:
        self._beat = asyncio.get_running_loop().create_task(self._beat_loop())

    async def _beat_loop(self) -> None:
        try:
            while True:
                for a in list(self.registry) + [self]:
                    self.publish(Topic.AGENT_HEALTH, a.health())
                await asyncio.sleep(self.beat_s)
        except asyncio.CancelledError:
            raise

    async def on_message(self, env) -> None:
        if env.topic == Topic.KILL_SWITCH:
            self.killed = True       # Phase 6: halt Signal/Order + cancel pendings

    async def on_stop(self) -> None:
        if getattr(self, "_beat", None) is not None:
            self._beat.cancel()
            try:
                await self._beat
            except asyncio.CancelledError:
                pass
