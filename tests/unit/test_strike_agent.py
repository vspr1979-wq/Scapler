"""StrikeAgent: window build, delta-band pick, re-center, lot multiplier."""
import asyncio

from scapler.agents.strike import StrikeAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings
from scapler.core.messages import OrderIntent, OptionType as OT, Tick, Topic
from tests._master import make_master, opt_key, spot_key

SK = spot_key()


def tick_opt(strike, opt, ltp, delta, bid, ask):
    return Tick(key=opt_key(strike, opt), exch_ts_ns=0, ltp=ltp, bid=bid,
                ask=ask, delta=delta)


async def _agent(cfg=None, index="BANKNIFTY", master=None):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.STRIKE_SELECTED, Topic.ORDER_REQUEST,
                  Topic.WINDOW_REBUILT)
    a = StrikeAgent(bus, master or make_master(), cfg or Settings(), index,
                    "2026-09-29")
    await a.start()
    return bus, out, a


async def test_window_build_and_subscription_footprint():
    bus, out, a = await _agent()
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51203.0), key=SK)
    await asyncio.sleep(0.05)
    assert a.window is not None and a.window.center == 51200.0
    assert len(a.sub_keys) == 23                      # spot + 22 options
    rb = (await out.get()).payload                    # first build announced
    assert rb["center"] == 51200.0 and len(rb["keys"]) == 23
    assert rb["strikes"][0] == 50700.0 and rb["step"] == 100.0
    assert ("NSE_FO|51200C" in rb["map"]
            and rb["map"]["NSE_FO|51200C"] == [51200.0, "CE"])
    await a.stop()


async def test_delta_band_pick_and_order_request():
    bus, out, a = await _agent()
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51203.0), key=SK)
    for st, d in ((51100, 0.58), (51200, 0.53), (51300, 0.44)):
        bus.publish(Topic.TICK_RAW,
                    tick_opt(st, "C", 148.2, d, 148.15, 148.20),
                    key=opt_key(st, "C"))
    await asyncio.sleep(0.05)
    bus.publish(Topic.SIGNAL_EXECUTE, OT.CE)
    await asyncio.sleep(0.05)
    envs = [await out.get() for _ in range(3)]     # rebuild + sel + req
    sel = next(e.payload for e in envs if e.topic == Topic.STRIKE_SELECTED)
    req = next(e.payload for e in envs if e.topic == Topic.ORDER_REQUEST)
    assert sel.instrument.strike == 51200.0 and sel.side is OT.CE     # 0.44 out of band
    assert req.intent is OrderIntent.BUY_TO_OPEN
    assert req.qty == 30 and req.instrument.strike == 51200.0
    assert req.client_id.startswith("SCP-") and 8 <= len(req.client_id) <= 20
    await a.stop()


async def test_lot_multiplier_capped_1_10():
    bus, out, a = await _agent(Settings(lot_multiplier=3))
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51203.0), key=SK)
    bus.publish(Topic.TICK_RAW, tick_opt(51200, "C", 148.2, 0.53, 148.15, 148.20),
                key=opt_key(51200, "C"))
    await asyncio.sleep(0.05)
    bus.publish(Topic.SIGNAL_EXECUTE, OT.CE)
    await asyncio.sleep(0.05)
    envs = [await out.get() for _ in range(3)]
    req = next(e.payload for e in envs if e.topic == Topic.ORDER_REQUEST)
    assert req.qty == 90
    await a.stop()


async def test_recenter_on_two_step_drift():
    bus, out, a = await _agent(master=make_master(radius=10))
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51203.0), key=SK)
    await asyncio.sleep(0.05)
    await out.get()                                 # initial build announced
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51399.0), key=SK)
    await asyncio.sleep(0.05)
    assert a.window.center == 51200.0 and out.depth() == 0
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51400.0), key=SK)
    await asyncio.sleep(0.05)
    rb = (await out.get()).payload
    assert rb["center"] == 51400.0 and len(rb["keys"]) == 23
    assert a.window.strikes[0] == 50900.0
    await a.stop()


async def test_no_window_no_order():
    bus, out, a = await _agent()
    bus.publish(Topic.SIGNAL_EXECUTE, OT.CE)      # spot never seen
    await asyncio.sleep(0.05)
    assert out.depth() == 0
    await a.stop()
