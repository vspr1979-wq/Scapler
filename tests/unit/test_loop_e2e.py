"""Phase-4 exit gate: the FULL loop on one EventBus.

spot ticks → MarketData → Candle → Indicator → Signal(AUTO) → Strike →
Risk → Order(stub) → PositionExit → exit sells → flat → HELD released →
no second entry (setup never broke).
"""
import asyncio

from scapler.agents.candle import CandleBuilderAgent
from scapler.agents.indicator import IndicatorAgent
from scapler.agents.market_data import MarketDataAgent
from scapler.agents.order import OrderAgent
from scapler.agents.position_exit import PositionExitAgent
from scapler.agents.risk import RiskAgent
from scapler.agents.signal import SignalAgent
from scapler.agents.strike import StrikeAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings, Setup
from scapler.core.messages import OrderIntent, Tick, Topic
from tests._broker import StubBroker
from tests._master import make_master, opt_key, spot_key
from tests.unit.test_chain_integration import crafted_candles, T0

SK = spot_key()
OK = opt_key(51200, "C")


def cfg():
    return Settings(mode="AUTO", signal_ttl_candles=3,
                    setup=Setup(atr_min={k: 0.5 for k in
                                         ("NIFTY", "BANKNIFTY", "SENSEX",
                                          "FINNIFTY", "MIDCPNIFTY")}))


def spot_ticks(rows, base=51203.0):
    import msgspec
    from tests.unit.test_chain_integration import candles_to_ticks
    # The chain fixture lives around px=100; shift OHLC to the BANKNIFTY
    # master's range (constant offset → identical deltas → identical
    # indicators) so the strike window and the signal series agree.
    off = base - rows[0][0]
    rows = [(o + off, h + off, l + off, c + off, v) for o, h, l, c, v in rows]
    # chain fixture ticks are keyed "NSE_INDEX|NIFTY 50"; this loop trades
    # BANKNIFTY, so re-key to the master's spot feed key.
    return [msgspec.structs.replace(t, key=SK) for t in candles_to_ticks(rows)]


def opt_tick(minute, ltp, delta=0.53):
    return Tick(key=OK, exch_ts_ns=(T0 + minute * 60 + 30) * 10**9, ltp=ltp,
                bid=ltp - 0.05, ask=ltp, delta=delta)


async def test_full_loop_one_entry_one_exit_no_second_entry():
    c = cfg()
    master = make_master()
    bus = EventBus()
    log = Inbox()
    bus.subscribe(log, Topic.ORDER_FILL, Topic.POSITION_UPDATE,
                  Topic.RISK_VETO, Topic.SIGNAL_NEW)
    rows = crafted_candles(30)
    ticks = spot_ticks(rows)
    # interleave one option tick per minute so the strike window has quotes
    by_min = {}
    for t in ticks:
        by_min.setdefault(int((t.exch_ts_ns // 10**9 - T0) // 60), []).append(t)
    stream = []
    for m in sorted(by_min):
        stream.extend(by_min[m])
        stream.append(opt_tick(m, 148.2))
    adapter = _FeedAdapter(master, stream)
    md = MarketDataAgent(bus, adapter, [SK])
    cb = CandleBuilderAgent(bus, SK)
    ia = IndicatorAgent(bus)
    sa = SignalAgent(bus, c, "BANKNIFTY")
    st = StrikeAgent(bus, master, c, "BANKNIFTY", "2026-09-29")
    rk = RiskAgent(bus, c, clock_fn=lambda: "10:00")
    ob = StubBroker(master)
    oa = OrderAgent(bus, ob)
    px = PositionExitAgent(bus, c)
    for a in (md, cb, ia, sa, st, rk, oa, px):
        await a.start()
    await asyncio.sleep(0.8)

    # entry happened: one BUY fill
    buys = [o for o in ob.orders if o.intent is OrderIntent.BUY_TO_OPEN]
    assert len(buys) == 1 and buys[0].qty == 30
    assert buys[0].instrument.feed_key == OK

    # drive the exit ladder on the option
    for p in (153.2, 160.2, 168.2):
        bus.publish(Topic.TICK_RAW, Tick(key=OK, exch_ts_ns=0, ltp=p,
                                         bid=p - 0.05, ask=p), key=OK)
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.3)
    sells = [o for o in ob.orders if o.intent is OrderIntent.SELL_TO_CLOSE]
    assert len(sells) == 1 and sells[0].qty == 30        # N=1 → single T3 exit

    # position closed, HELD released, no second entry in remaining candles
    await asyncio.sleep(0.4)
    assert len([o for o in ob.orders
                if o.intent is OrderIntent.BUY_TO_OPEN]) == 1
    assert sa.fsm.held is False
    assert rk.trades == 1
    for a in (md, cb, ia, sa, st, rk, oa, px):
        await a.stop()


class _FeedAdapter:
    name = "stubfeed"

    def __init__(self, master, ticks):
        self.master = master
        self._ticks = ticks

    async def open_feed(self, keys, mode="full"):
        return _H(self._ticks)


class _H:
    def __init__(self, ticks):
        self._t = list(ticks)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._t:
            raise StopAsyncIteration
        # pace the stub feed: bus TICK_RAW coalesces latest-wins per key, so a
        # zero-delay burst would collapse candles before consumers drain.
        await asyncio.sleep(0.001)
        return self._t.pop(0)

    async def close(self):
        self.closed = True


def _handle(ticks):
    return _H(ticks)
