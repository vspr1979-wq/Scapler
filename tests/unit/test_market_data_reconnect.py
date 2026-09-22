"""MarketDataAgent: feed.reconnect re-opens the handle; window.rebuilt
updates the subscription footprint."""
import asyncio

from scapler.agents.market_data import MarketDataAgent
from scapler.core.bus import EventBus
from scapler.core.messages import Tick, Topic


class Handle:
    """Endless paced tick source (ltp counts up per handle generation)."""

    def __init__(self, key, gen):
        self.key, self.gen, self.n = key, gen, 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.002)
        self.n += 1
        return Tick(key=self.key, exch_ts_ns=0, ltp=self.gen + self.n / 100)

    async def close(self):
        self.closed = True


class Adapter:
    name = "fake"

    def __init__(self):
        self.opens = 0
        self.handles = []

    async def open_feed(self, keys, mode="full"):
        self.opens += 1
        h = Handle(keys[0], self.opens)
        self.handles.append(h)
        self.last_keys = list(keys)
        return h


async def test_reconnect_reopens_and_resumes_publishing():
    bus = EventBus()
    ad = Adapter()
    md = MarketDataAgent(bus, ad, ["K"])
    await md.start()
    await asyncio.sleep(0.05)
    assert ad.opens == 1 and md.ticks_total >= 1
    bus.publish(Topic.FEED_RECONNECT, "stale 7s")
    await asyncio.sleep(0.1)
    assert ad.opens == 2
    assert ad.handles[0].closed
    assert md.reconnects == 1
    n = md.ticks_total
    await asyncio.sleep(0.05)
    assert md.ticks_total > n                      # new handle pumps
    await md.stop()


async def test_window_rebuilt_updates_keys():
    bus = EventBus()
    ad = Adapter()
    md = MarketDataAgent(bus, ad, ["OLD"])
    await md.start()
    bus.publish(Topic.WINDOW_REBUILT,
                {"index": "NIFTY", "center": 24500.0, "step": 50.0,
                 "keys": ["NEW1", "NEW2"], "strikes": [], "map": {}})
    await asyncio.sleep(0.05)
    assert md.keys == ["NEW1", "NEW2"]
    bus.publish(Topic.FEED_RECONNECT, "index switch")
    await asyncio.sleep(0.1)
    assert ad.last_keys == ["NEW1", "NEW2"]        # re-open uses new footprint
    await md.stop()
