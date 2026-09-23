"""CandleBuilder: closed bars only, exact OHLC, volume deltas, quiet flush.

Next-tick-close tests use FUTURE minute buckets so the wall-clock flush timer
cannot race the publish sequence; the quiet-flush test uses a PAST bucket so
only the timer can close it.
"""
import asyncio
import time

from scapler.agents.candle import CandleBuilderAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.messages import Tick, Topic

KEY = "NSE_INDEX|NIFTY 50"
FUT = (int(time.time()) // 60) * 60 + 120       # 2 minutes ahead
PAST = (int(time.time()) // 60) * 60 - 120      # 2 minutes ago


def tick(base, minute, sec, ltp, vol=0.0):
    return Tick(key=KEY, exch_ts_ns=(base + minute * 60 + sec) * 10**9,
                ltp=ltp, volume=vol)


async def _run(ticks, flush_s=0.05, wait=0.2):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.CANDLE_CLOSED)
    a = CandleBuilderAgent(bus, KEY, flush_s=flush_s)
    await a.start()
    for t in ticks:
        bus.publish(Topic.TICK_RAW, t, key=t.key)
        await asyncio.sleep(0)
    await asyncio.sleep(wait)
    await a.stop()
    return bus, out, a


async def _drain(out, n, timeout=1.0):
    return [(await asyncio.wait_for(out.get(), timeout)).payload
            for _ in range(n)]


async def test_three_bars_two_closes_on_next_tick():
    ticks = [
        tick(FUT, 0, 1, 100.0), tick(FUT, 0, 20, 101.0),
        tick(FUT, 0, 40, 99.0), tick(FUT, 0, 55, 100.5),
        tick(FUT, 1, 1, 100.5), tick(FUT, 1, 30, 102.0),
        tick(FUT, 1, 50, 100.0), tick(FUT, 1, 59, 101.5),
        tick(FUT, 2, 1, 101.5), tick(FUT, 2, 30, 101.0),
    ]
    bus, out, a = await _run(ticks, wait=0.05)
    cs = await _drain(out, 2)
    assert len(cs) == 2 and a.candles_closed == 2
    assert (cs[0].o, cs[0].h, cs[0].l, cs[0].c) == (100.0, 101.0, 99.0, 100.5)
    assert (cs[1].o, cs[1].h, cs[1].l, cs[1].c) == (100.5, 102.0, 100.0, 101.5)
    assert cs[0].close_ts == cs[1].open_ts          # contiguous minutes
    assert cs[0].tf == "1m"


async def test_no_mid_bar_emission():
    ticks = [tick(FUT, 0, s, 100.0 + s / 100.0) for s in (1, 10, 20, 30, 40, 50)]
    bus, out, a = await _run(ticks, wait=0.05)
    assert out.depth() == 0 and a.candles_closed == 0


async def test_quiet_bar_flushed_by_timer():
    ticks = [tick(PAST, 0, 1, 100.0), tick(PAST, 0, 30, 100.5)]
    bus, out, a = await _run(ticks, flush_s=0.05, wait=0.3)
    cs = await _drain(out, 1)
    assert cs[0].c == 100.5 and a.candles_closed == 1


async def test_feed_volume_deltas_sum_per_bar():
    ticks = [
        tick(FUT, 0, 1, 100.0, vol=1000), tick(FUT, 0, 30, 101.0, vol=1400),
        tick(FUT, 0, 55, 100.5, vol=1500),
        tick(FUT, 1, 1, 100.5, vol=1500), tick(FUT, 1, 30, 101.5, vol=1800),
        tick(FUT, 2, 1, 101.5, vol=1800),
    ]
    bus, out, a = await _run(ticks, wait=0.05)
    cs = await _drain(out, 2)
    assert cs[0].volume == 500.0 and cs[1].volume == 300.0


async def test_index_proxy_volume_counts_ticks():
    ticks = [tick(FUT, 0, 1, 100.0), tick(FUT, 0, 20, 101.0),
             tick(FUT, 0, 40, 99.5), tick(FUT, 1, 1, 99.5),
             tick(FUT, 2, 1, 99.0)]
    bus, out, a = await _run(ticks, wait=0.05)
    cs = await _drain(out, 2)
    assert cs[0].volume == 3.0 and cs[1].volume == 1.0


async def test_other_keys_ignored():
    ticks = [tick(FUT, 0, 1, 100.0),
             Tick(key="NSE_FO|9", exch_ts_ns=(FUT + 1) * 10**9, ltp=5.0),
             tick(FUT, 1, 1, 100.2)]
    bus, out, a = await _run(ticks, wait=0.05)
    cs = await _drain(out, 1)
    assert cs[0].key == KEY and cs[0].c == 100.0
