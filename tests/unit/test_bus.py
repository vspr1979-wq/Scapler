import asyncio

import pytest

from scapler.core.bus import EventBus, Inbox
from scapler.core.messages import Topic, Tick


async def collect(inbox: Inbox, n: int):
    out = []
    for _ in range(n):
        out.append(await inbox.get())
    return out


async def test_pubsub_fifo_delivery():
    bus = EventBus()
    inb = Inbox()
    bus.subscribe(inb, "a")
    for i in range(5):
        bus.publish("a", i)
    envs = await collect(inb, 5)
    assert [e.payload for e in envs] == [0, 1, 2, 3, 4]
    assert [e.seq for e in envs] == sorted(e.seq for e in envs)


async def test_priority_lane_jumps_ahead():
    bus = EventBus()
    inb = Inbox()
    bus.subscribe(inb, "telemetry", Topic.ORDER_REQUEST)
    for i in range(3):
        bus.publish("telemetry", f"t{i}")
    bus.publish(Topic.ORDER_REQUEST, "ORDER")
    env = await inb.get()
    assert env.topic == Topic.ORDER_REQUEST and env.payload == "ORDER"


async def test_coalescing_latest_wins():
    bus = EventBus()
    inb = Inbox()
    bus.subscribe(inb, Topic.TICK_RAW)
    for i in range(1000):
        bus.publish(Topic.TICK_RAW, Tick(key="K", exch_ts_ns=i, ltp=float(i)), key="K")
    assert inb.depth() == 1                      # no queue bloat
    env = await inb.get()
    assert env.payload.ltp == 999.0              # latest wins
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(inb.get(), 0.05)  # nothing left behind


async def test_coalescing_per_key_slots():
    bus = EventBus()
    inb = Inbox()
    bus.subscribe(inb, Topic.TICK_RAW)
    for i in range(50):
        bus.publish(Topic.TICK_RAW, Tick(key="A", exch_ts_ns=i, ltp=float(i)), key="A")
        bus.publish(Topic.TICK_RAW, Tick(key="B", exch_ts_ns=i, ltp=-float(i)), key="B")
    assert inb.depth() == 2
    seen = {e.key: e.payload.ltp for e in await collect(inb, 2)}
    assert seen == {"A": 49.0, "B": -49.0}


async def test_unsubscribed_topic_is_dropped():
    bus = EventBus()
    inb = Inbox()
    bus.subscribe(inb, "a")
    bus.publish("b", 1)
    assert inb.depth() == 0
    assert bus.subscriber_count("b") == 0


async def test_wake_sentinel():
    inb = Inbox()
    inb.wake()
    env = await asyncio.wait_for(inb.get(), 0.5)
    assert env.topic == "__wake__" and env.seq == -1
