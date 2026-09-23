"""WatchdogAgent: stale-feed reconnect, square-off kill, status stream."""
import asyncio

from scapler.agents.watchdog import WatchdogAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.clock import mono_ns
from scapler.core.config import Settings
from scapler.core.messages import KillSwitch, Tick, Topic


async def _wd(clock="10:30", cfg=None):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.FEED_RECONNECT, Topic.KILL_SWITCH,
                  Topic.WATCHDOG_STATUS)
    w = WatchdogAgent(bus, cfg or Settings(), clock_fn=lambda: clock)
    await w.start()
    return bus, out, w


async def _collect(out, topic, wait=1.4):
    got = []
    end = asyncio.get_event_loop().time() + wait
    while asyncio.get_event_loop().time() < end:
        try:
            env = await asyncio.wait_for(out.get(), 0.2)
        except asyncio.TimeoutError:
            continue
        if env.topic == topic:
            got.append(env.payload)
    return got


async def test_stale_feed_triggers_reconnect_once():
    bus, out, w = await _wd()
    # fresh tick → no reconnect even after a beat
    bus.publish(Topic.TICK_RAW, Tick(key="k", exch_ts_ns=0, ltp=1.0), key="k")
    assert await _collect(out, Topic.FEED_RECONNECT, wait=1.3) == []
    # stale the feed by hand (clock injection would need a fake mono)
    w._last_tick = mono_ns() - int(10e9)
    got = await _collect(out, Topic.FEED_RECONNECT, wait=1.4)
    assert len(got) == 1 and "stale" in got[0]
    assert w.reconnects == 1
    # cooldown: reconnect_stale_s (30) blocks immediate repeats
    w._last_tick = mono_ns() - int(10e9)
    assert await _collect(out, Topic.FEED_RECONNECT, wait=1.3) == []
    await w.stop()


async def test_square_off_publishes_watchdog_kill():
    bus, out, w = await _wd(clock="15:20")
    got = await _collect(out, Topic.KILL_SWITCH, wait=1.4)
    assert len(got) == 1 and isinstance(got[0], KillSwitch)
    assert got[0].source == "watchdog" and w.squared_off
    # never twice
    assert await _collect(out, Topic.KILL_SWITCH, wait=1.3) == []
    await w.stop()


async def test_no_square_off_before_time_and_status_stream():
    bus, out, w = await _wd(clock="10:30")
    assert await _collect(out, Topic.KILL_SWITCH, wait=1.3) == []
    st = await _collect(out, Topic.WATCHDOG_STATUS, wait=1.3)
    assert st and st[-1]["square_off_at"] == "15:20"
    assert st[-1]["reconnects"] == 0
    await w.stop()


async def test_kill_marks_watchdog():
    bus, out, w = await _wd()
    bus.publish(Topic.KILL_SWITCH, KillSwitch(source="ui"))
    await asyncio.sleep(0.05)
    assert w.killed
    await w.stop()
