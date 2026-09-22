"""JournalAgent — SQLite WAL single writer (plan §5-F9 / §9).

Durability model: order-path topics ride the bus PRIORITY lane, so the
journal task writes them before telemetry backlog; WAL + synchronous=FULL
for order/fill/veto rows puts them on disk before the next message is
consumed. Tables: sessions, events, signals, orders, fills, positions,
risk_veto, agent_health. The Journal tab reads the in-memory recent ring
(zero-copy UI); EXPORT CSV streams the full DB.
"""
from __future__ import annotations

import csv
import logging
import sqlite3
import time
from pathlib import Path

from ..core.clock import ist_now, utc_iso
from ..core.messages import OrderIntent, Topic
from .base_imports import Agent

log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS sessions(
  id INTEGER PRIMARY KEY, started_at TEXT, ended_at TEXT,
  broker TEXT, demo INTEGER, mode TEXT);
CREATE TABLE IF NOT EXISTS events(
  seq INTEGER, ts TEXT, topic TEXT, agent TEXT, detail TEXT,
  sess INTEGER);
CREATE TABLE IF NOT EXISTS signals(
  seq INTEGER, ts TEXT, side TEXT, old TEXT, new TEXT, reason TEXT,
  candle_ts INTEGER, ttl INTEGER);
CREATE TABLE IF NOT EXISTS orders(
  seq INTEGER, ts TEXT, client_id TEXT, intent TEXT, symbol TEXT,
  strike REAL, expiry TEXT, qty INTEGER);
CREATE TABLE IF NOT EXISTS fills(
  seq INTEGER, ts TEXT, client_id TEXT, intent TEXT, symbol TEXT,
  qty INTEGER, price REAL, latency_ms REAL);
CREATE TABLE IF NOT EXISTS positions(
  seq INTEGER, ts TEXT, feed_key TEXT, qty_open INTEGER, avg_price REAL,
  closed INTEGER, exit_reason TEXT, realized_pnl REAL);
CREATE TABLE IF NOT EXISTS risk_veto(
  seq INTEGER, ts TEXT, code TEXT, context TEXT);
CREATE TABLE IF NOT EXISTS agent_health(
  ts TEXT, name TEXT, state TEXT, restarts INTEGER, inbox INTEGER,
  p99_ms REAL, beat_s REAL);
"""

AUDIT = (Topic.CANDLE_CLOSED, Topic.INDICATORS_READY, Topic.SIGNAL_NEW,
         Topic.SIGNAL_STATE, Topic.STRIKE_SELECTED, Topic.WINDOW_REBUILT,
         Topic.ORDER_REQUEST, Topic.ORDER_APPROVED, Topic.RISK_VETO,
         Topic.ORDER_REQ, Topic.ORDER_FILL, Topic.ORDER_REJECTED,
         Topic.POSITION_UPDATE, Topic.EXIT_TRIGGER, Topic.KILL_SWITCH,
         Topic.AGENT_HEALTH, Topic.FEED_RECONNECT, Topic.CONNECTION_STATUS,
         Topic.SESSION_NEW_DAY)

SRC = {
    Topic.CANDLE_CLOSED: "candle", Topic.INDICATORS_READY: "indicator",
    Topic.SIGNAL_NEW: "signal", Topic.SIGNAL_STATE: "signal",
    Topic.STRIKE_SELECTED: "strike", Topic.WINDOW_REBUILT: "strike",
    Topic.ORDER_REQUEST: "auto/ui", Topic.ORDER_APPROVED: "risk",
    Topic.RISK_VETO: "risk", Topic.ORDER_REQ: "order",
    Topic.ORDER_FILL: "order", Topic.ORDER_REJECTED: "order",
    Topic.POSITION_UPDATE: "position", Topic.EXIT_TRIGGER: "position",
    Topic.KILL_SWITCH: "ui/any", Topic.AGENT_HEALTH: "supervisor",
    Topic.FEED_RECONNECT: "watchdog", Topic.CONNECTION_STATUS: "connection",
    Topic.SESSION_NEW_DAY: "watchdog",
}


def _ts() -> str:
    return ist_now().strftime("%H:%M:%S.") + f"{ist_now().microsecond // 1000:03d}"


class JournalAgent(Agent):
    name = "journal"
    topics = AUDIT

    def __init__(self, bus, db_path: str | Path, broker: str = "",
                 demo: bool = False, mode: str = "MANUAL",
                 shadow: bool = False) -> None:
        super().__init__(bus)
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.broker, self.demo, self.mode = broker, demo, mode
        self.shadow = shadow
        self.rows = 0
        self.session_id: int | None = None
        self._db: sqlite3.Connection | None = None

    # ── lifecycle ───────────────────────────────────────────────────
    async def on_start(self) -> None:
        self._db = sqlite3.connect(self.db_path, timeout=5.0)
        # fetchone() forces the pragma to execute immediately (python sqlite3
        # can otherwise leave it pending) — WAL is the durability contract
        mode = self._db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            log.error("journal: WAL unavailable (got %s) — durability "
                      "degraded", mode)
        self._db.execute("PRAGMA synchronous=FULL").fetchone()
        self._db.executescript(DDL)
        try:  # migrate pre-Phase-7 DBs: events gained a session id
            self._db.execute("ALTER TABLE events ADD COLUMN sess INTEGER")
        except sqlite3.OperationalError:
            pass                        # column already present
        self._open_session()

    def _open_session(self) -> None:
        mode = self.mode + ("/SHADOW" if self.shadow else "")
        cur = self._db.execute(
            "INSERT INTO sessions(started_at, ended_at, broker, demo, mode) "
            "VALUES(?,?,?,?,?)", (utc_iso(), None, self.broker,
                                 int(self.demo), mode))
        self.session_id = cur.lastrowid
        self._db.commit()

    def _roll_session(self) -> None:
        self._db.execute("UPDATE sessions SET ended_at=? WHERE id=?",
                         (utc_iso(), self.session_id))
        self._open_session()

    async def on_stop(self) -> None:
        if self._db is not None:
            try:
                self._db.execute(
                    "UPDATE sessions SET ended_at=? WHERE id=?",
                    (utc_iso(), self.session_id))
                self._db.commit()
                self._db.close()
            except sqlite3.Error as e:
                log.error("journal close: %s", e)
            self._db = None

    # ── ingest ──────────────────────────────────────────────────────
    async def on_message(self, env) -> None:
        if self._db is None:
            return
        t, p = env.topic, env.payload
        ts = _ts()
        detail = ""
        try:
            if t in (Topic.SIGNAL_NEW, Topic.SIGNAL_STATE):
                detail = (f"{p.side.value} {p.old.value}→{p.new.value} "
                          f"({p.reason})")
                self._db.execute(
                    "INSERT INTO signals VALUES(?,?,?,?,?,?,?,?)",
                    (env.seq, ts, p.side.value, p.old.value, p.new.value,
                     p.reason, p.candle_ts, p.ttl_candles))
            elif t == Topic.ORDER_REQ or t == Topic.ORDER_REQUEST:
                detail = (f"{p.intent.value} {p.instrument.symbol} "
                          f"qty {p.qty} {p.client_id}")
                self._db.execute(
                    "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?)",
                    (env.seq, ts, p.client_id, p.intent.value,
                     p.instrument.symbol, p.instrument.strike,
                     p.instrument.expiry, p.qty))
            elif t == Topic.ORDER_FILL:
                detail = (f"{p.intent.value} {p.qty} @ {p.price:.2f} "
                          f"({p.latency_ms:.0f} ms)")
                self._db.execute(
                    "INSERT INTO fills VALUES(?,?,?,?,?,?,?,?)",
                    (env.seq, ts, p.client_id, p.intent.value,
                     p.instrument.symbol, p.qty, p.price, p.latency_ms))
            elif t == Topic.RISK_VETO:
                detail = f"{p.code} {p.context}"
                self._db.execute("INSERT INTO risk_veto VALUES(?,?,?,?)",
                                 (env.seq, ts, p.code, p.context))
            elif t == Topic.POSITION_UPDATE:
                detail = (f"{p.feed_key} qty {p.qty_open} "
                          f"{'CLOSED ' + p.exit_reason if p.closed else 'open'}"
                          f" realized {p.realized_pnl:.2f}")
                self._db.execute(
                    "INSERT INTO positions VALUES(?,?,?,?,?,?,?,?)",
                    (env.seq, ts, p.feed_key, p.qty_open, p.avg_price,
                     int(p.closed), p.exit_reason, p.realized_pnl))
            elif t == Topic.EXIT_TRIGGER:
                detail = f"{p.reason.value} SELL {p.qty} ref {p.ref_price:.2f}"
            elif t == Topic.ORDER_APPROVED:
                detail = f"approved {p.intent.value} {p.client_id}"
            elif t == Topic.ORDER_REJECTED:
                detail = f"REJECTED {p.client_id}: {p.reason}"
            elif t == Topic.STRIKE_SELECTED:
                detail = (f"{p.instrument.strike:g} {p.side.value} "
                          f"δ{p.delta} spread {p.spread_ticks}t")
            elif t == Topic.WINDOW_REBUILT:
                detail = (f"{p['index']} center {p['center']:g} "
                          f"({len(p['keys'])} keys)")
            elif t == Topic.CANDLE_CLOSED:
                detail = (f"{p.key} O{p.o:g} H{p.h:g} L{p.l:g} C{p.c:g} "
                          f"V{p.volume:g}")
            elif t == Topic.INDICATORS_READY:
                detail = (f"rsi {p.rsi:.1f} atr {p.atr:.1f} "
                          f"vol× {p.vol_ratio:.1f}")
            elif t == Topic.KILL_SWITCH:
                detail = f"kill.switch ({p.source})"
            elif t == Topic.AGENT_HEALTH:
                self._db.execute(
                    "INSERT INTO agent_health VALUES(?,?,?,?,?,?,?)",
                    (ts, p.name, p.state, p.restarts, p.inbox_depth,
                     p.p99_ms, p.beat_age_s))
                return                      # health: table only, no events
            elif t == Topic.SESSION_NEW_DAY:
                detail = f"new session day {p}"
                self._roll_session()
            elif t == Topic.FEED_RECONNECT:
                detail = f"feed.reconnect ({p})"
            elif t == Topic.CONNECTION_STATUS:
                detail = f"{p.get('broker')} {p.get('state')} " \
                         f"{p.get('error') or ''}".strip()
            else:
                detail = str(p)[:160]
            self._db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)",
                             (env.seq, ts, t, SRC.get(t, ""), detail,
                              self.session_id))
            self.rows += 1
            # order-path rows hit the disk before the next message is
            # consumed (synchronous=FULL commit); telemetry batches.
            if t in (Topic.ORDER_REQ, Topic.ORDER_REQUEST, Topic.ORDER_FILL,
                     Topic.RISK_VETO, Topic.KILL_SWITCH, Topic.SIGNAL_NEW):
                self._db.commit()
            elif self.rows % 32 == 0:
                self._db.commit()
        except sqlite3.Error as e:            # journal must never kill the bus
            log.error("journal write failed (%s): %s", t, e)

    # ── export ──────────────────────────────────────────────────────
    def export_csv(self, out_path: str | Path | None = None) -> tuple[str, int]:
        if self._db is not None:
            self._db.commit()
        db = self._db or sqlite3.connect(self.db_path, timeout=5.0)
        path = Path(out_path) if out_path else \
            self.db_path.with_name(
                f"journal-export-{time.strftime('%Y%m%d-%H%M%S')}.csv")
        n = 0
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["seq", "ts_ist", "topic", "agent", "detail"])
            for row in db.execute("SELECT seq, ts, topic, agent, detail "
                                  "FROM events ORDER BY rowid"):
                w.writerow(row)
                n += 1
        if self._db is None:
            db.close()
        return str(path), n
