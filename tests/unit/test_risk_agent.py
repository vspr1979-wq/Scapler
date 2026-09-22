"""RiskAgent: every veto code, exits always pass, counters."""
import asyncio

from scapler.agents.risk import RiskAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.clock import mono_ns
from scapler.core.config import Settings
from scapler.core.messages import (
    InstrumentKey, OrderFill, OrderIntent, OrderRequest, OptionType,
    PositionUpdate, Tick, Topic,
)

IK = InstrumentKey(exchange="NSE_FO", token="9", symbol="S", strike=51200.0,
                   option_type=OptionType.CE)


def req(intent=OrderIntent.BUY_TO_OPEN, cid="SCP-260929-0001"):
    return OrderRequest(intent=intent, instrument=IK, qty=30, client_id=cid,
                        ts_mono=0)


def fill(intent=OrderIntent.BUY_TO_OPEN, price=148.2, cid="SCP-260929-0001"):
    return OrderFill(client_id=cid, instrument=IK, intent=intent, qty=30,
                     price=price, latency_ms=1.0, ts_mono=0, lot_size=30)


async def _risk(clock="10:00", cfg=None):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.ORDER_APPROVED, Topic.RISK_VETO)
    a = RiskAgent(bus, cfg or Settings(), clock_fn=lambda: clock)
    await a.start()
    return bus, out, a


async def _next(out):
    return (await asyncio.wait_for(out.get(), 1.0)).payload


async def test_entry_passes_in_window():
    bus, out, a = await _risk()
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).client_id == req().client_id
    await a.stop()


async def test_v_window():
    bus, out, a = await _risk(clock="09:10")
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_WINDOW"
    await a.stop()


async def test_v_maxtrades():
    bus, out, a = await _risk()
    for i in range(3):
        bus.publish(Topic.ORDER_FILL, fill(cid=f"C{i}"))
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_MAXTRADES"
    assert a.trades == 3
    await a.stop()


async def test_v_held_then_released():
    bus, out, a = await _risk()
    bus.publish(Topic.POSITION_UPDATE, PositionUpdate("k", 30, 148.2, False))
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_HELD"
    bus.publish(Topic.POSITION_UPDATE, PositionUpdate("k", 0, 148.2, True,
                                                      "T3", 600.0))
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req(cid="SCP-260929-0002"))
    assert (await _next(out)).client_id == "SCP-260929-0002"
    await a.stop()


async def test_v_kill_and_exits_still_pass():
    bus, out, a = await _risk()
    bus.publish(Topic.KILL_SWITCH, "ui")
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_KILL"
    bus.publish(Topic.ORDER_REQUEST, req(OrderIntent.SELL_TO_CLOSE,
                                         "SCP-260929-0009"))
    assert (await _next(out)).client_id == "SCP-260929-0009"   # exits never vetoed
    await a.stop()


async def test_v_slstreak_and_dailyloss():
    bus, out, a = await _risk()
    for i in range(2):
        bus.publish(Topic.POSITION_UPDATE,
                    PositionUpdate("k", 0, 148.2, True, "SL", -240.0))
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_SLSTREAK"
    a.sl_streak = 0
    bus.publish(Topic.POSITION_UPDATE,
                PositionUpdate("k", 0, 148.2, True, "T3", -3000.0))
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_DAILYLOSS"
    assert a.pnl == -3480.0
    await a.stop()


async def test_v_stale_feed():
    bus, out, a = await _risk()
    a._last_tick = mono_ns() - int(10e9)
    bus.publish(Topic.ORDER_REQUEST, req())
    assert (await _next(out)).code == "V_STALE"
    bus.publish(Topic.TICK_RAW, Tick(key="k", exch_ts_ns=0, ltp=1.0), key="k")
    await asyncio.sleep(0.05)
    await asyncio.sleep(0.05)
    bus.publish(Topic.ORDER_REQUEST, req(cid="SCP-260929-0004"))
    assert (await _next(out)).client_id == "SCP-260929-0004"
    await a.stop()
