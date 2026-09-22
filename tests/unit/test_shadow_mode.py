"""Phase 7: SHADOW MODE — real decisions, stubbed adapter edge.

Shadow fills use the REAL prevailing quotes (ask for buys, bid for sells,
LTP fallback), never call the broker adapter, and still enforce SideGuard —
everything downstream (risk, journal, UI) sees ordinary fills.
"""
from __future__ import annotations

import asyncio

from scapler.agents.order import OrderAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.messages import (
    InstrumentKey, OrderIntent, OrderRequest, OptionType, Tick, Topic,
)
from tests._broker import StubBroker
from tests._master import make_master, opt_key

KEY = opt_key(51200, "C")
IK = InstrumentKey(exchange="NSE_FO", token="51200C", symbol="S",
                   strike=51200.0, option_type=OptionType.CE)


def req(intent, qty=30, cid="SCP-260929-0071"):
    return OrderRequest(intent=intent, instrument=IK, qty=qty, client_id=cid,
                        ts_mono=0)


async def _order(shadow=True, tick=Tick(key=KEY, exch_ts_ns=0, ltp=99.0,
                                        bid=98.5, ask=100.5)):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.ORDER_FILL, Topic.ORDER_REJECTED)
    b = StubBroker(make_master())
    a = OrderAgent(bus, b, shadow=shadow)
    await a.start()
    bus.publish(Topic.TICK_RAW, tick, key=KEY)
    await asyncio.sleep(0.05)
    return bus, out, a, b


async def _next(out):
    return (await asyncio.wait_for(out.get(), 1.0)).payload


async def test_shadow_buy_fills_at_ask_and_sell_at_bid():
    bus, out, a, b = await _order()
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.BUY_TO_OPEN))
    f = await _next(out)
    assert f.intent is OrderIntent.BUY_TO_OPEN
    assert f.price == 100.5                       # BUY at the real ASK
    assert a.last_ack.broker_order_id.startswith("SHADOW-")

    bus.publish(Topic.ORDER_APPROVED,
                req(OrderIntent.SELL_TO_CLOSE, cid="SCP-260929-0072"))
    f = await _next(out)
    assert f.intent is OrderIntent.SELL_TO_CLOSE
    assert f.price == 98.5                        # SELL at the real BID
    assert b.orders == []                         # adapter NEVER called
    await a.stop()


async def test_shadow_falls_back_to_ltp_without_book():
    bus, out, a, b = await _order(tick=Tick(key=KEY, exch_ts_ns=0, ltp=77.25))
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.BUY_TO_OPEN))
    f = await _next(out)
    assert f.price == 77.25 and b.orders == []
    await a.stop()


async def test_sideguard_still_enforced_in_shadow():
    bus, out, a, b = await _order()
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.SELL_TO_CLOSE))
    rej = await _next(out)                        # no long position open
    assert rej.reason.startswith("SG_")
    assert b.orders == []
    await a.stop()


async def test_live_mode_still_calls_adapter_at_ltp():
    bus, out, a, b = await _order(shadow=False)
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.BUY_TO_OPEN))
    f = await _next(out)
    assert f.price == 99.0                        # live fills at LTP
    assert len(b.orders) == 1
    assert a.last_ack.broker_order_id == "STUB-SCP-260929-0071"
    await a.stop()
