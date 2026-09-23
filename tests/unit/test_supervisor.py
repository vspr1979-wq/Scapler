"""SupervisorAgent: 1 Hz (test: 50 ms) health fan-out incl. itself."""
import asyncio

from scapler.agents.supervisor import SupervisorAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.messages import AgentHealth, KillSwitch, Topic
from scapler.agents.base_imports import Agent


class Dummy(Agent):
    name = "dummy"


async def test_health_beats_include_self_and_registry():
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.AGENT_HEALTH)
    d = Dummy(bus)
    await d.start()
    sup = SupervisorAgent(bus, agents=(d,), beat_s=0.05)
    await sup.start()
    await asyncio.sleep(0.18)
    names = set()
    while out.depth():
        p = (await out.get()).payload
        assert isinstance(p, AgentHealth)
        names.add(p.name)
    assert {"dummy", "supervisor"} <= names
    await sup.stop()
    await d.stop()


async def test_kill_flag():
    bus = EventBus()
    sup = SupervisorAgent(bus)
    await sup.start()
    bus.publish(Topic.KILL_SWITCH, KillSwitch(source="ui"))
    await asyncio.sleep(0.05)
    assert sup.killed is True
    await sup.stop()
