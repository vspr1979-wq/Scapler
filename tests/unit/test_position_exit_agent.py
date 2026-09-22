"""PositionExitAgent: trail ladder, SL, T3, time-stop, kill, realized P&L."""
import asyncio

from scapler.agents.position_exit import PositionExitAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings
from scapler.core.messages import (
    CandleClosed, ExitReason, OrderFill, OrderIntent, Tick, Topic,
)
from tests._master import opt_key

KEY = opt_key(51200, "C")
from scapler.core.messages import InstrumentKey, OptionType   # noqa: E402
IK = InstrumentKey(exchange="NSE_FO", token="51200C", symbol="S",
                   strike=51200.0, option_type=OptionType.CE)


def buy(price=148.2, qty=30, lot=30):
    return OrderFill(client_id="C1", instrument=IK,
                     intent=OrderIntent.BUY_TO_OPEN, qty=qty, price=price,
                     latency_ms=1.0, ts_mono=0, lot_size=lot)


def sell(price, qty=30, cid="C2"):
    return OrderFill(client_id=cid, instrument=IK,
                     intent=OrderIntent.SELL_TO_CLOSE, qty=qty, price=price,
                     latency_ms=1.0, ts_mono=0, lot_size=30)


def tick(ltp):
    return Tick(key=KEY, exch_ts_ns=0, ltp=ltp)


async def _px(cfg=None):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.ORDER_REQUEST, Topic.POSITION_UPDATE,
                  Topic.EXIT_TRIGGER)
    a = PositionExitAgent(bus, cfg or Settings())
    await a.start()
    return bus, out, a


async def _drain_reqs(out, n):
    reqs = []
    while len(reqs) < n:
        env = await asyncio.wait_for(out.get(), 1.0)
        if env.topic == Topic.ORDER_REQUEST:
            reqs.append(env.payload)
    return reqs


async def test_open_then_t1_trails_no_partial_for_one_lot():
    bus, out, a = await _px()
    bus.publish(Topic.ORDER_FILL, buy())
    pu = (await asyncio.wait_for(out.get(), 1)).payload
    assert pu.qty_open == 30 and not pu.closed
    bus.publish(Topic.TICK_RAW, tick(153.2))          # T1 hit
    await asyncio.sleep(0.05)
    # no partial sell for N=1 lot — but the trail change is announced so the
    # UI position panel shows T1 ✓ / SL=BE live
    env = await asyncio.wait_for(out.get(), 1)
    assert env.topic == Topic.POSITION_UPDATE
    assert env.payload.t1_hit is True and env.payload.sl_price == 148.2
    assert env.payload.qty_open == 30 and not env.payload.closed
    assert out.depth() == 0                            # nothing else published
    assert a.trackers[KEY].sl_price == 148.2
    await a.stop()


async def test_sl_after_t1_is_breakeven():
    bus, out, a = await _px()
    bus.publish(Topic.ORDER_FILL, buy())
    await asyncio.wait_for(out.get(), 1)
    bus.publish(Topic.TICK_RAW, tick(153.2))
    await asyncio.sleep(0.05)
    bus.publish(Topic.TICK_RAW, tick(148.1))          # dip → BE stop
    r = (await _drain_reqs(out, 1))[0]
    assert r.intent is OrderIntent.SELL_TO_CLOSE and r.qty == 30
    bus.publish(Topic.ORDER_FILL, sell(148.1))
    pu = None
    while True:
        env = await asyncio.wait_for(out.get(), 1)
        if env.topic == Topic.POSITION_UPDATE:
            pu = env.payload
            if pu.closed:
                break
    assert pu.exit_reason == "SL"
    assert round(pu.realized_pnl, 2) == -3.0
    await a.stop()


async def test_t3_full_exit_realized_pnl():
    bus, out, a = await _px()
    bus.publish(Topic.ORDER_FILL, buy())
    await asyncio.wait_for(out.get(), 1)
    for p in (153.2, 160.2, 168.2):
        bus.publish(Topic.TICK_RAW, tick(p))
        await asyncio.sleep(0.03)
    r = (await _drain_reqs(out, 1))[0]
    assert r.qty == 30
    bus.publish(Topic.ORDER_FILL, sell(168.2))
    closed = None
    while True:
        env = await asyncio.wait_for(out.get(), 1)
        if env.topic == Topic.POSITION_UPDATE and env.payload.closed:
            closed = env.payload
            break
    assert closed.exit_reason == "T3"
    assert closed.realized_pnl == 600.0
    assert KEY not in a.trackers
    await a.stop()


async def test_time_stop():
    bus, out, a = await _px(Settings(time_stop_candles=2))
    bus.publish(Topic.ORDER_FILL, buy())
    await asyncio.wait_for(out.get(), 1)
    bus.publish(Topic.TICK_RAW, tick(150.0))          # seed ltp for ref price
    await asyncio.sleep(0.03)
    for i in range(3):
        bus.publish(Topic.CANDLE_CLOSED,
                    CandleClosed(key="spot", tf="1m", open_ts=i,
                                 close_ts=i + 1, o=1, h=1, l=1, c=1, volume=1))
        await asyncio.sleep(0.02)
    r = (await _drain_reqs(out, 1))[0]
    assert r.intent is OrderIntent.SELL_TO_CLOSE
    await a.stop()


async def test_kill_force_close():
    bus, out, a = await _px()
    bus.publish(Topic.ORDER_FILL, buy())
    await asyncio.wait_for(out.get(), 1)
    bus.publish(Topic.TICK_RAW, tick(150.0))
    await asyncio.sleep(0.03)
    bus.publish(Topic.KILL_SWITCH, "ui")
    r = (await _drain_reqs(out, 1))[0]
    assert r.qty == 30
    await a.stop()


async def test_manual_exit_reason_from_ui_trigger():
    """UI EXIT button: ExitTrigger(MANUAL) stamps the close reason; the UI's
    own SELL_TO_CLOSE request produces the fill that closes the tracker."""
    from scapler.core.messages import ExitTrigger
    bus, out, a = await _px()
    bus.publish(Topic.ORDER_FILL, buy())
    await asyncio.wait_for(out.get(), 1)
    bus.publish(Topic.EXIT_TRIGGER, ExitTrigger(
        reason=ExitReason.MANUAL, instrument=IK, qty=30, ref_price=150.0))
    await asyncio.sleep(0.05)
    assert KEY not in a._reason or a._reason[KEY] is ExitReason.MANUAL
    bus.publish(Topic.ORDER_FILL, sell(150.0))
    closed = None
    while True:
        env = await asyncio.wait_for(out.get(), 1)
        if env.topic == Topic.POSITION_UPDATE and env.payload.closed:
            closed = env.payload
            break
    assert closed.exit_reason == "MANUAL"
    assert closed.realized_pnl == (150.0 - 148.2) * 30
    assert KEY not in a.trackers
    await a.stop()
