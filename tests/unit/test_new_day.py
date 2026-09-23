"""Phase 7: daily rollover — SESSION_NEW_DAY resets breakers and journal."""
from __future__ import annotations

import asyncio
import sqlite3

from scapler.agents.journal import JournalAgent
from scapler.agents.risk import RiskAgent
from scapler.agents.ui import UIAgent
from scapler.agents.watchdog import WatchdogAgent
from scapler.core.bus import EventBus, Inbox
from scapler.core.config import Settings
from scapler.core.messages import Topic

D1, D2 = "2026-09-24", "2026-09-25"
SK = "NSE_INDEX|NIFTY"


async def test_watchdog_publishes_new_day_and_clears_square_off():
    bus = EventBus()
    out = Inbox()
    bus.subscribe(out, Topic.SESSION_NEW_DAY)
    day = {"d": D1}
    wd = WatchdogAgent(bus, Settings(), clock_fn=lambda: "10:00",
                       date_fn=lambda: day["d"])
    await wd.start()
    await asyncio.sleep(1.15)           # watchdog loop is 1 Hz
    assert wd.session_day == D1
    assert out.depth() == 0             # no rollover on the first day
    wd.squared_off = True
    wd.killed = True
    day["d"] = D2                       # IST date flips overnight
    await asyncio.sleep(1.15)
    assert wd.session_day == D2
    assert not wd.squared_off and not wd.killed
    env = await asyncio.wait_for(out.get(), 1.0)
    assert env.payload == D2
    await wd.stop()


async def test_risk_resets_daily_counters_on_new_day():
    bus = EventBus()
    rk = RiskAgent(bus, Settings())
    await rk.start()
    rk.trades = 3
    rk.sl_streak = 2
    rk.pnl = -4999.0
    rk.held = True
    rk.killed = True
    bus.publish(Topic.SESSION_NEW_DAY, D2)
    await asyncio.sleep(0.05)
    assert (rk.trades, rk.sl_streak, rk.pnl) == (0, 0, 0.0)
    assert not rk.held and not rk.killed
    await rk.stop()


async def test_journal_rolls_session_on_new_day(tmp_path):
    bus = EventBus()
    dbp = tmp_path / "j.sqlite"
    jr = JournalAgent(bus, dbp, broker="groww", demo=False,
                      mode="AUTO", shadow=True)
    await jr.start()
    await asyncio.sleep(0.05)
    first = jr.session_id
    bus.publish(Topic.SESSION_NEW_DAY, D2)
    await asyncio.sleep(0.05)
    second = jr.session_id
    assert second != first
    await jr.stop()

    rows = sqlite3.connect(str(dbp)).execute(
        "SELECT id, mode, started_at, ended_at FROM sessions "
        "ORDER BY id").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == "AUTO/SHADOW" and rows[0][3] is not None
    assert rows[1][1] == "AUTO/SHADOW"
    # stop() also closes the second session; rollover ended the first one
    assert rows[0][3] <= rows[1][2]


async def test_ui_clears_kill_and_counters_on_new_day():
    bus = EventBus()
    ui = UIAgent(bus, Settings(), "NIFTY", SK, {"NIFTY": SK}, hz=5.0,
                 demo=False)
    await ui.start()
    ui.killed = True
    ui.trades = 3
    ui.pnl = -1200.0
    ui.sl_streak = 2
    bus.publish(Topic.SESSION_NEW_DAY, D2)
    await asyncio.sleep(0.15)
    assert not ui.killed and ui.trades == 0 and ui.pnl == 0.0
    assert ui.sl_streak == 0
    await ui.stop()
