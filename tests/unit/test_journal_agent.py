"""JournalAgent: SQLite WAL durability, per-topic tables, CSV export."""
import asyncio
import csv
import sqlite3

from scapler.agents.journal import JournalAgent
from scapler.core.bus import EventBus
from scapler.core.messages import (
    AgentHealth, ExitReason, ExitTrigger, InstrumentKey, KillSwitch,
    OrderFill, OrderIntent, OrderRejected, OrderRequest, OptionType as OT,
    PositionUpdate, RiskVeto, SignalState, SignalStateEnum as S, Topic,
)

IK = InstrumentKey(exchange="NSE_FO", token="N24500C", symbol="NIFTYC24500",
                   strike=24500.0, expiry="2026-09-29", option_type=OT.CE)


def req(cid="SCP-260929-0001"):
    return OrderRequest(intent=OrderIntent.BUY_TO_OPEN, instrument=IK,
                        qty=65, client_id=cid, ts_mono=0)


def fill(cid="SCP-260929-0001", price=148.2):
    return OrderFill(client_id=cid, instrument=IK,
                     intent=OrderIntent.BUY_TO_OPEN, qty=65, price=price,
                     latency_ms=12.0, ts_mono=0, lot_size=65)


async def _journal(tmp_path):
    bus = EventBus()
    j = JournalAgent(bus, tmp_path / "journal.sqlite", broker="demo",
                     demo=True, mode="MANUAL")
    await j.start()
    await asyncio.sleep(0.05)      # Agent.start() spawns; on_start follows
    return bus, j


async def test_order_path_rows_land_in_dedicated_tables(tmp_path):
    bus, j = await _journal(tmp_path)
    bus.publish(Topic.ORDER_REQUEST, req())
    bus.publish(Topic.ORDER_REQ, req())
    bus.publish(Topic.ORDER_FILL, fill())
    bus.publish(Topic.RISK_VETO, RiskVeto(code="V_HELD", context="C2"))
    bus.publish(Topic.ORDER_REJECTED, OrderRejected(client_id="C3",
                                                    reason="SG_SELL_EXCEEDS_QTY"))
    bus.publish(Topic.SIGNAL_NEW, SignalState(side=OT.CE, old=S.ARMED,
                                              new=S.SIGNALED, reason="ok",
                                              candle_ts=7, ttl_candles=3))
    bus.publish(Topic.POSITION_UPDATE, PositionUpdate(
        feed_key=IK.feed_key, qty_open=65, avg_price=148.2, closed=False,
        t1_hit=True, sl_price=148.2))
    bus.publish(Topic.EXIT_TRIGGER, ExitTrigger(
        reason=ExitReason.T3, instrument=IK, qty=65, ref_price=168.2))
    bus.publish(Topic.KILL_SWITCH, KillSwitch(source="ui"))
    bus.publish(Topic.AGENT_HEALTH, AgentHealth(
        name="risk", state="RUN", restarts=0, inbox_depth=0, p99_ms=0.2,
        beat_age_s=0.5))
    await asyncio.sleep(0.15)
    await j.stop()                 # stop commits pending telemetry rows

    db = sqlite3.connect(tmp_path / "journal.sqlite")
    n = lambda t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    assert n("orders") == 2 and n("fills") == 1
    assert n("risk_veto") == 1 and n("signals") == 1
    assert n("positions") == 1 and n("agent_health") == 1
    assert n("sessions") == 1
    events = n("events")
    assert events >= 9                       # health rows skip the events log
    order_row = db.execute("SELECT intent, symbol, qty FROM orders").fetchone()
    assert order_row == ("BUY_TO_OPEN", "NIFTYC24500", 65)
    veto_row = db.execute("SELECT code, context FROM risk_veto").fetchone()
    assert veto_row == ("V_HELD", "C2")
    ended = db.execute("SELECT ended_at FROM sessions").fetchone()[0]
    assert ended                   # session closed on stop
    db.close()


async def test_wal_mode_and_export_csv(tmp_path):
    bus, j = await _journal(tmp_path)
    db = sqlite3.connect(tmp_path / "journal.sqlite")
    mode = db.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
    db.close()
    for i in range(5):
        bus.publish(Topic.ORDER_REQ, req(f"C{i}"))
    await asyncio.sleep(0.15)
    path, rows = j.export_csv()
    assert rows >= 5
    with open(path, newline="", encoding="utf-8") as f:
        body = list(csv.reader(f))
    assert body[0] == ["seq", "ts_ist", "topic", "agent", "detail"]
    assert any(r[2] == "order.req" for r in body[1:])
    await j.stop()
