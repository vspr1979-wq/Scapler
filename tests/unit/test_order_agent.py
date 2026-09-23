"""OrderAgent: SideGuard rejects, idempotency, fill publication, open-qty map."""
import asyncio

from scapler.agents.order import OrderAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings  # noqa: F401  (symmetry)
from scapler.core.messages import (
    InstrumentKey, OrderIntent, OrderRequest, OptionType, Tick, Topic,
)
from tests._broker import StubBroker
from tests._master import make_master, opt_key

KEY = opt_key(51200, "C")
IK = InstrumentKey(exchange="NSE_FO", token="51200C", symbol="S",
                   strike=51200.0, option_type=OptionType.CE)


def req(intent, qty=30, cid="SCP-260929-0001"):
    return OrderRequest(intent=intent, instrument=IK, qty=qty, client_id=cid,
                        ts_mono=0)


async def _order():
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.ORDER_REQ, Topic.ORDER_FILL, Topic.ORDER_REJECTED)
    b = StubBroker(make_master())
    a = OrderAgent(bus, b)
    await a.start()
    bus.publish(Topic.TICK_RAW, Tick(key=KEY, exch_ts_ns=0, ltp=148.2), key=KEY)
    await asyncio.sleep(0.05)
    return bus, out, a, b


async def _next(out):
    return (await asyncio.wait_for(out.get(), 1.0)).payload


async def test_buy_happy_path_fill_at_ltp():
    bus, out, a, b = await _order()
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.BUY_TO_OPEN))
    f = await _next(out)             # order.req
    f = await _next(out)             # order.fill
    assert f.intent is OrderIntent.BUY_TO_OPEN and f.price == 148.2
    assert f.lot_size == 30 and f.qty == 30
    assert a.open[KEY] == 30 and len(b.orders) == 1
    await a.stop()


async def test_idempotency_duplicate_client_id_dropped():
    bus, out, a, b = await _order()
    r = req(OrderIntent.BUY_TO_OPEN)
    bus.publish(Topic.ORDER_APPROVED, r)
    bus.publish(Topic.ORDER_APPROVED, r)
    await asyncio.sleep(0.1)
    assert len(b.orders) == 1
    await a.stop()


async def test_sideguard_sell_without_long():
    bus, out, a, b = await _order()
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.SELL_TO_CLOSE))
    rej = await _next(out)
    assert rej.reason == "SG_SELL_WITHOUT_LONG" and len(b.orders) == 0
    await a.stop()


async def test_sideguard_sell_exceeds_open():
    bus, out, a, b = await _order()
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.BUY_TO_OPEN))
    await _next(out); await _next(out)
    bus.publish(Topic.ORDER_APPROVED,
                req(OrderIntent.SELL_TO_CLOSE, qty=60, cid="SCP-260929-0002"))
    rej = await _next(out)
    assert rej.reason == "SG_SELL_EXCEEDS_QTY"
    assert a.open[KEY] == 30
    await a.stop()


async def test_sell_to_close_round_trip():
    bus, out, a, b = await _order()
    bus.publish(Topic.ORDER_APPROVED, req(OrderIntent.BUY_TO_OPEN))
    await _next(out); await _next(out)
    bus.publish(Topic.ORDER_APPROVED,
                req(OrderIntent.SELL_TO_CLOSE, cid="SCP-260929-0002"))
    await _next(out)
    f = await _next(out)
    assert f.intent is OrderIntent.SELL_TO_CLOSE and f.price == 148.2
    assert a.open[KEY] == 0
    await a.stop()
