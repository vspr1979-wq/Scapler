"""UIAgent: snapshot content, command → bus relay, counters, audit rows."""
import asyncio

from scapler.agents.ui import UIAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings
from scapler.core.messages import (
    AgentHealth, CandleClosed, IndicatorsReady, InstrumentKey, KillSwitch,
    OrderFill, OrderIntent, OptionType as OT, PositionUpdate, SignalState,
    SignalStateEnum as S, Tick, Topic,
)

SK = "NSE_INDEX|BANKNIFTY"
OK = "NSE_FO|51200C"
IK = InstrumentKey(exchange="NSE_FO", token="51200C",
                   symbol="BANKNIFTY260929C51200", strike=51200.0,
                   expiry="2026-09-29", option_type=OT.CE)

WINDOW = {"index": "BANKNIFTY", "center": 51200.0, "step": 100.0,
          "keys": [SK, OK],
          "strikes": [51100.0, 51200.0, 51300.0],
          "map": {OK: [51200.0, "CE"], "NSE_FO|51200P": [51200.0, "PE"],
                  "NSE_FO|51100C": [51100.0, "CE"],
                  "NSE_FO|51300C": [51300.0, "CE"]}}


async def _ui(mode="MANUAL", hz=50.0):
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.UI_EXECUTE, Topic.KILL_SWITCH, Topic.ORDER_REQUEST,
                  Topic.EXIT_TRIGGER, Topic.UI_SNAPSHOT)
    applied = []
    ui = UIAgent(bus, Settings(mode=mode), "BANKNIFTY", SK,
                 {"BANKNIFTY": SK}, hz=hz,
                 on_command={"set_mode": lambda a: applied.append(a) or {"ok": True}},
                 demo=True)
    ui.broker_name = "DEMO (stub)"
    ui.expiry = "2026-09-29"
    ui.lot = 30
    await ui.start()
    return bus, out, ui, applied


async def _drain(out, topic):
    got = []
    while out.depth():
        e = await out.get()
        if e.topic == topic:
            got.append(e.payload)
    return got


async def test_snapshot_basics_and_push_loop():
    bus, out, ui, _ = await _ui()
    bus.publish(Topic.TICK_RAW, Tick(key=SK, exch_ts_ns=0, ltp=51203.0), key=SK)
    bus.publish(Topic.TICK_RAW,
                Tick(key=OK, exch_ts_ns=0, ltp=148.2, bid=148.15, ask=148.25,
                     delta=0.53, gamma=0.0045), key=OK)
    bus.publish(Topic.WINDOW_REBUILT, WINDOW)
    bus.publish(Topic.CANDLE_CLOSED, CandleClosed(
        key=SK, tf="1m", open_ts=0, close_ts=60 * 10**9,
        o=51188.0, h=51236.0, l=51171.0, c=51203.0, volume=900.0))
    bus.publish(Topic.INDICATORS_READY, IndicatorsReady(
        key=SK, candle_close_ts=60 * 10**9, vwap=51180.2, ema9=51205.4,
        ema21=51160.1, rsi=58.4, atr=26.0, vol_ratio=1.8))
    await asyncio.sleep(0.1)
    snap = ui.snapshot()
    assert snap["demo"] is True and snap["mode"] == "MANUAL"
    assert snap["header"]["ltps"]["BANKNIFTY"]["ltp"] == 51203.0
    assert snap["controls"]["qty"] == 30
    ind = snap["indicators"]
    assert ind["rsi"] == 58.4 and ind["setup_ce"].startswith("VALID")
    assert ind["setup_pe"] == "not valid"
    rows = snap["window"]["rows"]
    assert [r["tag"] for r in rows] == ["ITM1", "ATM", "OTM1"]
    atm = rows[1]
    assert atm["ce_ltp"] == 148.2 and atm["gamma"] == 0.0045
    assert atm["ce_delta"] == 0.53 and atm["pe_ltp"] is None
    # push loop publishes coalesced snapshots
    snaps = await _drain(out, Topic.UI_SNAPSHOT)
    assert snaps and snaps[-1]["ts"]
    await ui.stop()


async def test_signal_states_and_execute_command():
    bus, out, ui, _ = await _ui()
    bus.publish(Topic.SIGNAL_NEW, SignalState(
        side=OT.CE, old=S.ARMED, new=S.SIGNALED, reason="setup valid",
        candle_ts=0, ttl_candles=3))
    await asyncio.sleep(0.05)
    snap = ui.snapshot()
    ce = snap["signals"]["sides"]["CE"]
    assert ce["state"] == "SIGNALED" and ce["can_execute"] is True
    assert snap["signals"]["sides"]["PE"]["can_execute"] is False
    # MANUAL click relays ui.execute with the side
    assert ui.handle_command("execute", {"side": "CE"}) == {"ok": True}
    ex = await _drain(out, Topic.UI_EXECUTE)
    assert ex == [OT.CE]
    # AUTO mode → EXECUTE disabled (auto-fire chip on the JS side)
    ui.cfg = __import__("dataclasses").replace(ui.cfg, mode="AUTO")
    assert ui.snapshot()["signals"]["sides"]["CE"]["can_execute"] is False
    # unknown command never raises
    assert ui.handle_command("nope")["ok"] is False
    await ui.stop()


async def test_kill_and_position_exit_commands():
    bus, out, ui, _ = await _ui()
    # open position from a BUY fill + position update
    bus.publish(Topic.ORDER_FILL, OrderFill(
        client_id="C1", instrument=IK, intent=OrderIntent.BUY_TO_OPEN,
        qty=30, price=148.2, latency_ms=1.0, ts_mono=0, lot_size=30))
    bus.publish(Topic.POSITION_UPDATE, PositionUpdate(
        feed_key=OK, qty_open=30, avg_price=148.2, closed=False,
        t1_hit=True, sl_price=148.2))
    bus.publish(Topic.TICK_RAW, Tick(key=OK, exch_ts_ns=0, ltp=153.4), key=OK)
    await asyncio.sleep(0.05)
    pos = ui.snapshot()["position"]
    assert pos["open"] and pos["qty"] == 30 and pos["lot"] == 30
    assert pos["t1"] is True and pos["sl_price"] == 148.2
    assert pos["instrument"] == "BANKNIFTY260929C51200"
    assert pos["upnl_pts"] == round(153.4 - 148.2, 2)
    assert ui.snapshot()["signals"]["today"]["trades"] == 1
    # EXIT button → EXIT_TRIGGER(MANUAL) + SELL_TO_CLOSE for the full qty
    ui.window = WINDOW
    assert ui.handle_command("exit_position")["ok"] is True
    await asyncio.sleep(0.05)
    envs = []
    while out.depth():
        envs.append(await out.get())
    trig = [e.payload for e in envs if e.topic == Topic.EXIT_TRIGGER]
    reqs = [e.payload for e in envs if e.topic == Topic.ORDER_REQUEST]
    assert len(trig) == 1 and trig[0].reason.value == "MANUAL"
    assert len(reqs) == 1 and reqs[0].intent is OrderIntent.SELL_TO_CLOSE
    assert reqs[0].qty == 30 and reqs[0].instrument.feed_key == OK
    # KILL button → kill.switch on the bus
    assert ui.handle_command("kill")["ok"] is True
    ks = await _drain(out, Topic.KILL_SWITCH)
    assert len(ks) == 1 and isinstance(ks[0], KillSwitch) and ks[0].source == "ui"
    await ui.stop()


async def test_close_updates_counters_and_audit():
    bus, out, ui, _ = await _ui()
    bus.publish(Topic.POSITION_UPDATE, PositionUpdate(
        feed_key=OK, qty_open=0, avg_price=148.2, closed=True,
        exit_reason="SL", realized_pnl=-240.0))
    bus.publish(Topic.POSITION_UPDATE, PositionUpdate(
        feed_key=OK, qty_open=0, avg_price=150.0, closed=True,
        exit_reason="SL", realized_pnl=-300.0))
    bus.publish(Topic.RISK_VETO,
                __import__("scapler.core.messages", fromlist=["RiskVeto"]
                           ).RiskVeto(code="V_SLSTREAK", context="C9"))
    await asyncio.sleep(0.05)
    snap = ui.snapshot()
    today = snap["signals"]["today"]
    assert today["sl_streak"] == 2 and today["pnl"] == -540.0
    topics = [r["topic"] for r in snap["journal"]]
    assert Topic.RISK_VETO in topics and Topic.POSITION_UPDATE in topics
    assert any("V_SLSTREAK" in r["detail"] for r in snap["journal"])
    await ui.stop()


async def test_agent_health_and_command_callback():
    bus, out, ui, applied = await _ui()
    bus.publish(Topic.AGENT_HEALTH, AgentHealth(
        name="risk", state="RUN", restarts=0, inbox_depth=0,
        p99_ms=0.2, beat_age_s=0.1))
    await asyncio.sleep(0.05)
    snap = ui.snapshot()
    row = next(a for a in snap["agents"] if a["name"] == "risk")
    assert row["state"] == "RUN" and row["job"] == "vetoes, breakers, guards"
    assert "connection" in snap["agents_planned"]      # Phase 6 placeholder
    ui.handle_command("set_mode", {"mode": "AUTO"})
    assert applied and applied[0]["mode"] == "AUTO"
    await ui.stop()
