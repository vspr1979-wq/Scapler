import asyncio

from scapler.core.agent import Agent
from scapler.core.bus import EventBus


class Echo(Agent):
    name = "echo"
    topics = ("work",)

    def __init__(self, bus, fail_first=False):
        super().__init__(bus)
        self.got = []
        self._fail_first = fail_first

    async def on_message(self, env):
        if self._fail_first:
            self._fail_first = False
            raise RuntimeError("injected crash")
        self.got.append(env.payload)


async def wait_for_count(agent, n, timeout=2.0):
    async def _cond():
        while len(agent.got) < n:
            await asyncio.sleep(0.002)
    await asyncio.wait_for(_cond(), timeout)


async def test_agent_consumes_topic():
    bus = EventBus()
    a = Echo(bus)
    await a.start()
    bus.publish("work", 42)
    await wait_for_count(a, 1)
    assert a.got == [42]
    assert a.state == "RUN"
    h = a.health()
    assert h.name == "echo" and h.restarts == 0
    await a.stop()
    assert a.state == "STOP"


async def test_crash_then_supervised_restart():
    bus = EventBus()
    a = Echo(bus, fail_first=True)
    await a.start()
    bus.publish("work", 1)           # crashes the agent
    await asyncio.sleep(0.15)        # restart backoff 50 ms
    assert a.restarts == 1
    bus.publish("work", 2)           # processed after restart
    await wait_for_count(a, 1)
    assert a.got == [2]
    assert a.state == "RUN"
    await a.stop()


async def test_hop_latency_recorded():
    bus = EventBus()
    a = Echo(bus)
    await a.start()
    for i in range(20):
        bus.publish("work", i)
        await asyncio.sleep(0)
    await wait_for_count(a, 20)
    assert bus.stats.p99_ms("echo") >= 0.0
    assert bus.stats.published.get("work") == 20
    await a.stop()


async def test_stop_is_clean_when_idle():
    bus = EventBus()
    a = Echo(bus)
    await a.start()
    await asyncio.sleep(0.01)
    await a.stop()                   # blocked in inbox.get() → wake works
    assert a.state == "STOP"
