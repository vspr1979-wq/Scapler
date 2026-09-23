"""UIAgent — coalesced snapshots → transport; JS commands → bus (plan §5-F1/#12).

Read-only consumer of everything the UI shows; never trades on its own except
when relaying explicit operator commands (EXECUTE / EXIT / KILL). Snapshots are
plain dicts (orjson-friendly), rebuilt at ``hz`` (10–20 Hz, never per-tick) and
published on ``ui.snapshot`` (coalesced latest-wins) + handed to ``push_cb``
for the pywebview transport.

Commands (from JS, via runtime/transport):
  execute(side) · kill() · exit_position() · set_mode/set_index/set_lots ·
  save_settings(changes) · broker_save/broker_connect/broker_disconnect
Unknown commands are logged, never raised (UI must not crash the loop).
"""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime

from ..core.clock import IST, mono_ns
from ..core.config import Settings
from ..core.ids import new_client_id
from ..core.messages import (
    ExitReason, ExitTrigger, KillSwitch, OrderIntent, OrderRequest,
    OptionType, Topic,
)
from ..strategy.setup import setup_valid
from .base_imports import Agent

JOBS = {
    "market_data": "WS ingest, LTP cache",
    "candle": "ticks → closed 1m bars",
    "indicator": "VWAP/EMA/RSI/ATR incremental",
    "signal": "CE+PE state machines",
    "strike": "master, expiry, 11-window",
    "risk": "vetoes, breakers, guards",
    "order": "MARKET orders + SideGuard",
    "position_exit": "T1/T2/T3/SL tick-watch",
    "connection": "login/token lifecycle",
    "watchdog": "stale-feed, reconnect, orphans",
    "journal": "SQLite single writer",
    "ui": "snapshots 10–20 Hz → WebView2",
    "supervisor": "health, crash-restart, kill",
}
PLANNED: tuple[str, ...] = ()          # all 13 agents exist as of Phase 6

# journal/log topic → producing agent (display only)
SRC = {
    Topic.CANDLE_CLOSED: "candle", Topic.INDICATORS_READY: "indicator",
    Topic.SIGNAL_NEW: "signal", Topic.SIGNAL_STATE: "signal",
    Topic.STRIKE_SELECTED: "strike", Topic.WINDOW_REBUILT: "strike",
    Topic.ORDER_REQUEST: "auto/ui", Topic.ORDER_APPROVED: "risk",
    Topic.RISK_VETO: "risk", Topic.ORDER_REQ: "order",
    Topic.ORDER_FILL: "order", Topic.ORDER_REJECTED: "order",
    Topic.POSITION_UPDATE: "position", Topic.EXIT_TRIGGER: "position",
    Topic.KILL_SWITCH: "ui/any",
}


def _ts(ns: int = 0) -> str:
    dt = datetime.now(IST)
    return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def _f(x, nd=2):
    return round(float(x), nd) if x is not None else None


class UIAgent(Agent):
    name = "ui"
    topics = (Topic.TICK_RAW, Topic.CANDLE_CLOSED, Topic.INDICATORS_READY,
              Topic.SIGNAL_NEW, Topic.SIGNAL_STATE, Topic.STRIKE_SELECTED,
              Topic.WINDOW_REBUILT, Topic.ORDER_REQUEST, Topic.ORDER_APPROVED,
              Topic.RISK_VETO, Topic.ORDER_REQ, Topic.ORDER_FILL,
              Topic.ORDER_REJECTED, Topic.POSITION_UPDATE, Topic.EXIT_TRIGGER,
              Topic.KILL_SWITCH, Topic.AGENT_HEALTH,
              Topic.WATCHDOG_STATUS, Topic.CONNECTION_STATUS,
              Topic.SESSION_NEW_DAY)

    def __init__(self, bus, settings: Settings, index: str,
                 spot_key: str, index_keys: dict[str, str],
                 hz: float = 10.0, push_cb=None, on_command=None,
                 demo: bool = False) -> None:
        super().__init__(bus)
        self.cfg = settings
        self.index = index
        self.spot_key = spot_key
        self.index_keys = index_keys          # {"NIFTY": feed_key, ...}
        self.hz = hz
        self.push_cb = push_cb                # fn(snapshot dict) — webview
        self.on_command = on_command or {}    # runtime callbacks
        self.demo = demo
        self.lot = 0                          # master lot size (runtime sets)
        # hot caches
        self.ltp: dict[str, float] = {}
        self.greeks: dict[str, tuple] = {}    # key → (delta, gamma)
        self.prev_ltp: dict[str, float] = {}
        self.candles: dict[str, object] = {}
        self.inds: dict[str, object] = {}
        self.setup_text: dict[str, str] = {}
        # fsm / window / position
        self.states = {o.value: "DISARMED" for o in OptionType}
        self.sig_reason = {o.value: "" for o in OptionType}
        self.selected: dict[str, object] = {}  # side.value → StrikeSelected
        self.window: dict | None = None
        self.positions: dict[str, dict] = {}
        self._lot: dict[str, int] = {}          # feed_key → lot size (fills)
        self._sym: dict[str, str] = {}          # feed_key → broker symbol
        # counters (derived from bus events — risk agent owns the truth)
        self.trades = 0
        self.pnl = 0.0
        self.sl_streak = 0
        self.killed = False
        self.agents: dict[str, dict] = {}
        self.watchdog: dict = {}
        self.connection: dict = {"broker": "", "state": "DISCONNECTED",
                                 "error": "", "master_source": "",
                                 "master_options": 0}
        self._first_seen: dict[str, float] = {}
        # telemetry
        self._tick_count = 0
        self._tick_win_start = mono_ns()
        self.ticks_per_s = 0.0
        # audit
        self.log: deque = deque(maxlen=200)
        self.journal: deque = deque(maxlen=500)

    # ── lifecycle ───────────────────────────────────────────────────
    async def on_start(self) -> None:
        self._push = asyncio.get_running_loop().create_task(self._push_loop())

    async def _push_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(1.0 / self.hz)
                snap = self.snapshot()
                self.publish(Topic.UI_SNAPSHOT, snap)
                if self.push_cb is not None:
                    try:
                        self.push_cb(snap)
                    except Exception:
                        pass                  # a dead window must not kill UI
        except asyncio.CancelledError:
            raise

    async def on_stop(self) -> None:
        if getattr(self, "_push", None) is not None:
            self._push.cancel()
            try:
                await self._push
            except asyncio.CancelledError:
                pass

    # ── ingest ──────────────────────────────────────────────────────
    def _audit(self, env, detail: str, cls: str = "") -> None:
        row = {"seq": env.seq, "ts": _ts(), "topic": env.topic,
               "agent": SRC.get(env.topic, ""), "detail": detail, "cls": cls}
        self.journal.append(row)
        if env.topic not in (Topic.CANDLE_CLOSED, Topic.INDICATORS_READY,
                             Topic.POSITION_UPDATE):
            self.log.append(row)

    async def on_message(self, env) -> None:
        t = env.topic
        p = env.payload
        if t == Topic.TICK_RAW:
            self._tick_count += 1
            now = mono_ns()
            dt = (now - self._tick_win_start) / 1e9
            if dt >= 1.0:
                self.ticks_per_s = self._tick_count / dt
                self._tick_count, self._tick_win_start = 0, now
            self.prev_ltp[p.key] = self.ltp.get(p.key, p.ltp)
            self.ltp[p.key] = p.ltp
            if p.delta is not None or p.gamma is not None:
                self.greeks[p.key] = (p.delta, p.gamma)
            return
        if t == Topic.CANDLE_CLOSED:
            self.candles[p.key] = p
            return
        if t == Topic.INDICATORS_READY:
            self.inds[p.key] = p
            c = self.candles.get(p.key)
            if c is not None:
                atr_min = self.cfg.setup.atr_min.get(self.index, 0.0)
                for side in OptionType:
                    ok = setup_valid(side, p, c, self.cfg.setup, atr_min)
                    self.setup_text[side.value] = (
                        "VALID — close>VWAP · EMA9>EMA21 · RSI∈[55,78] · vol≥1.5×"
                        if ok and side is OptionType.CE else
                        "VALID — close<VWAP · EMA9<EMA21 · RSI∈[22,45] · vol≥1.5×"
                        if ok else "not valid")
            self._audit(env, f"vwap={_f(p.vwap)} ema9={_f(p.ema9)} "
                             f"rsi={_f(p.rsi,1)} atr={_f(p.atr,1)} "
                             f"volx={_f(p.vol_ratio,1)}")
            return
        if t in (Topic.SIGNAL_NEW, Topic.SIGNAL_STATE):
            self.states[p.side.value] = p.new.value
            self.sig_reason[p.side.value] = p.reason
            cls = "up" if t == Topic.SIGNAL_NEW else ""
            self._audit(env, f"{p.side.value} {p.old.value}→{p.new.value} "
                             f"({p.reason})", cls)
            return
        if t == Topic.STRIKE_SELECTED:
            self.selected[p.side.value] = p
            self._audit(env, f"{p.instrument.strike:g} {p.side.value} "
                             f"δ{_f(p.delta)} spread={p.spread_ticks}tick", "up")
            return
        if t == Topic.WINDOW_REBUILT:
            self.window = p
            self._audit(env, f"center={p['center']:g} window="
                             f"{p['strikes'][0]:g}..{p['strikes'][-1]:g}")
            return
        if t == Topic.ORDER_APPROVED:
            self._audit(env, f"approved {p.intent.value} {p.client_id}")
            return
        if t == Topic.RISK_VETO:
            self._audit(env, f"{p.code} — {p.context}", "dn")
            return
        if t == Topic.ORDER_REQ:
            self._audit(env, f"MARKET {p.intent.value} "
                             f"{p.instrument.symbol} qty {p.qty}", "up")
            return
        if t == Topic.ORDER_REJECTED:
            self._audit(env, f"REJECTED {p.client_id}: {p.reason}", "dn")
            return
        if t == Topic.ORDER_FILL:
            self._lot[p.instrument.feed_key] = p.lot_size or 1
            self._sym[p.instrument.feed_key] = p.instrument.symbol
            if p.intent is OrderIntent.BUY_TO_OPEN:
                self.trades += 1
            self._audit(env, f"{p.intent.value} {p.qty} @ {_f(p.price)} "
                             f"({p.latency_ms:.0f} ms)", "up")
            return
        if t == Topic.EXIT_TRIGGER:
            self._audit(env, f"{p.reason.value} → SELL_TO_CLOSE {p.qty} "
                             f"ref {_f(p.ref_price)}", "dn")
            return
        if t == Topic.POSITION_UPDATE:
            pos = self.positions.setdefault(p.feed_key, {})
            pos.update(qty=p.qty_open, avg=p.avg_price, closed=p.closed,
                       t1=p.t1_hit, t2=p.t2_hit, sl=p.sl_price,
                       reason=p.exit_reason, realized=p.realized_pnl)
            if p.closed:
                # realized_pnl is per-position (tracker resets on each open)
                self.pnl += p.realized_pnl
                self.sl_streak = self.sl_streak + 1 if p.exit_reason == "SL" \
                    else 0
                self._audit(env, f"CLOSED {p.feed_key} {p.exit_reason} "
                                 f"realized {_f(p.realized_pnl)}",
                            "up" if p.realized_pnl >= 0 else "dn")
            return
        if t == Topic.KILL_SWITCH:
            self.killed = True
            self._audit(env, f"kill.switch ({p.source}) → square off + halt "
                             f"entries", "dn")
            return
        if t == Topic.SESSION_NEW_DAY:
            self.killed = False
            self.trades = 0
            self.pnl = 0.0
            self.sl_streak = 0
            self._audit(env, f"new session day {p} → daily counters reset")
            return
        if t == Topic.WATCHDOG_STATUS:
            self.watchdog = p
            return
        if t == Topic.CONNECTION_STATUS:
            self.connection = {**self.connection, **p}
            self._audit(env, f"{p.get('broker', '?')} → {p.get('state', '?')}"
                             + (f" ({p['error']})" if p.get("error") else ""),
                        "dn" if p.get("error") else "up")
            return
        if t == Topic.AGENT_HEALTH:
            now = mono_ns() / 1e9
            self._first_seen.setdefault(p.name, now)
            self.agents[p.name] = {
                "name": p.name, "job": JOBS.get(p.name, ""),
                "state": p.state, "restarts": p.restarts,
                "inbox": p.inbox_depth, "p99_ms": p.p99_ms,
                "beat_s": p.beat_age_s,
                "up": self._fmt_up(now - self._first_seen[p.name])}
            return

    @staticmethod
    def _fmt_up(s: float) -> str:
        s = int(s)
        return f"{s // 3600}h{(s % 3600) // 60:02d}m" if s >= 3600 \
            else f"{s // 60}m{s % 60:02d}s"

    def switch_index(self, index: str, spot_key: str, expiry: str,
                     lot: int) -> None:
        """Instant index switch: panels rebind to the new spot; window and
        signal state reset (the new SignalAgent FSM starts DISARMED).
        Journal/log persist — the audit trail spans the whole session."""
        self.index = index
        self.spot_key = spot_key
        self.expiry = expiry
        self.lot = lot
        self.window = None
        self.selected = {}
        self.states = {o.value: "DISARMED" for o in OptionType}
        self.sig_reason = {o.value: "" for o in OptionType}
        self.setup_text = {}

    # ── commands (JS → bus) ─────────────────────────────────────────
    async def handle_command(self, name: str,
                             args: dict | None = None) -> dict:
        args = args or {}
        try:
            if name == "execute":
                side = OptionType[args.get("side", "CE")]
                self.publish(Topic.UI_EXECUTE, side, key=side.value)
            elif name == "kill":
                self.publish(Topic.KILL_SWITCH, KillSwitch(source="ui"))
            elif name == "exit_position":
                self._manual_exit()
            elif name in ("set_mode", "set_index", "set_lots",
                          "save_settings", "broker_save", "broker_connect",
                          "broker_disconnect", "journal_export", "rearm"):
                cb = self.on_command.get(name)
                if cb is not None:
                    out = cb(args)
                    if asyncio.iscoroutine(out):
                        out = await out
                    if isinstance(out, dict):
                        return out
            else:
                return {"ok": False, "error": f"unknown command {name}"}
        except Exception as e:                     # UI never crashes the loop
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}
        return {"ok": True}

    def _manual_exit(self) -> None:
        for key, pos in self.positions.items():
            if pos.get("closed") or pos.get("qty", 0) <= 0:
                continue
            meta = self.window or {}
            strike, opt = 0.0, OptionType.CE
            for fk, (s, o) in (meta.get("map") or {}).items():
                if fk == key:
                    strike, opt = s, OptionType[o]
            from ..core.messages import InstrumentKey
            ik = InstrumentKey(exchange=key.split("|")[0], token=key.split("|")[1],
                               symbol=key, strike=strike, option_type=opt)
            self.publish(Topic.EXIT_TRIGGER, ExitTrigger(
                reason=ExitReason.MANUAL, instrument=ik, qty=pos["qty"],
                ref_price=self.ltp.get(key, pos.get("avg", 0.0))))
            self.publish(Topic.ORDER_REQUEST, OrderRequest(
                intent=OrderIntent.SELL_TO_CLOSE, instrument=ik,
                qty=pos["qty"], client_id=new_client_id(), ts_mono=0))

    # ── snapshot ────────────────────────────────────────────────────
    def snapshot(self) -> dict:
        cfg = self.cfg
        ind = self.inds.get(self.spot_key)
        c = self.candles.get(self.spot_key)
        ltp = self.ltp.get(self.spot_key, 0.0)
        prev = self.prev_ltp.get(self.spot_key, ltp)
        # header
        header_ltps = {}
        for name, key in self.index_keys.items():
            v, pv = self.ltp.get(key), self.prev_ltp.get(key)
            header_ltps[name] = {"ltp": _f(v), "dir": (
                "up" if v is not None and pv is not None and v > pv else
                "dn" if v is not None and pv is not None and v < pv else "")}
        # indicators panel
        indicators = None
        if ind is not None and c is not None:
            when = datetime.fromtimestamp(c.close_ts / 1e9, IST)
            indicators = {
                "time": when.strftime("%H:%M"),
                "o": _f(c.o), "h": _f(c.h), "l": _f(c.l), "c": _f(c.c),
                "vwap": _f(ind.vwap), "ema9": _f(ind.ema9),
                "ema21": _f(ind.ema21), "rsi": _f(ind.rsi, 1),
                "atr": _f(ind.atr, 1), "vol_ratio": _f(ind.vol_ratio, 1),
                "setup_ce": self.setup_text.get("CE", "waiting…"),
                "setup_pe": self.setup_text.get("PE", "waiting…"),
                "spot_ltp": _f(ltp), "spot_dir": "up" if ltp > prev else
                "dn" if ltp < prev else ""}
        # signal panel
        held = any(not p.get("closed") and p.get("qty", 0) > 0
                   for p in self.positions.values())
        signals = {"sides": {}, "mode": cfg.mode, "ttl": cfg.signal_ttl_candles,
                   "agents": f"{len(self.agents)}/{len(self.agents)}",
                   "today": {"trades": self.trades,
                             "max": cfg.max_trades_per_day,
                             "sl_streak": self.sl_streak,
                             "pnl": _f(self.pnl)}}
        for side in ("CE", "PE"):
            st = self.states[side]
            sel = self.selected.get(side)
            line = ""
            if sel is not None:
                k = sel.instrument.feed_key
                line = (f"BUY {sel.instrument.symbol} @ "
                        f"{self.ltp.get(k, 0.0):.2f}")
            signals["sides"][side] = {
                "state": st, "reason": self.sig_reason[side], "line": line,
                "can_execute": (st == "SIGNALED" and cfg.mode == "MANUAL"
                                and not held and not self.killed)}
        # strike window rows
        rows, selected = [], None
        if self.window:
            w = self.window
            step = w.get("step", 100.0) or 100.0
            inv = {}
            for fk, (s, o) in (w.get("map") or {}).items():
                inv[(s, o)] = fk
            for s in w["strikes"]:
                n = int(round((w["center"] - s) / step))
                tag = "ATM" if n == 0 else f"ITM{n}" if n > 0 else f"OTM{-n}"
                ce_k, pe_k = inv.get((s, "CE")), inv.get((s, "PE"))
                ce_d = self.greeks.get(ce_k, (None, None)) if ce_k else (None, None)
                pe_d = self.greeks.get(pe_k, (None, None)) if pe_k else (None, None)
                rows.append({
                    "tag": tag, "strike": s,
                    "gamma": _f(ce_d[1], 4) if ce_d[1] is not None else None,
                    "ce_ltp": _f(self.ltp.get(ce_k)) if ce_k else None,
                    "pe_ltp": _f(self.ltp.get(pe_k)) if pe_k else None,
                    "ce_delta": _f(ce_d[0]) if ce_d[0] is not None else None,
                    "pe_delta": _f(pe_d[0]) if pe_d[0] is not None else None})
            for side, sel in self.selected.items():
                if self.window and sel.window_lo <= sel.instrument.strike <= sel.window_hi:
                    selected = {"strike": sel.instrument.strike,
                                "side": side, "delta": _f(sel.delta)}
        # open position
        position = {"open": False}
        for key, p in self.positions.items():
            if p.get("closed") or p.get("qty", 0) <= 0:
                continue
            cur = self.ltp.get(key, p["avg"])
            pts = cur - p["avg"]
            lot = self._lot.get(key, p["qty"]) or p["qty"]
            lots = max(1, p["qty"] // lot)
            position = {
                "open": True, "feed_key": key, "qty": p["qty"],
                "instrument": self._sym.get(key, key), "lot": lot,
                "lots": lots,
                "avg": _f(p["avg"]), "ltp": _f(cur),
                "upnl_pts": _f(pts), "upnl_inr": _f(pts * p["qty"]),
                "t1": p.get("t1", False), "t2": p.get("t2", False),
                "sl_price": _f(p.get("sl", 0.0)),
                "targets": {"t1": cfg.targets.t1, "t2": cfg.targets.t2,
                            "t3": cfg.targets.t3, "sl": cfg.targets.sl},
                "trail": f"T1→SL=BE · T2→SL=+T1 · T3→flat "
                         f"(N={lots} lot{'' if lots == 1 else 's'})",
                "time_stop": f"exit @ {cfg.time_stop_candles} candles · "
                             f"square-off {cfg.square_off}"}
            break
        # agents table
        agents_rows = [self.agents[n] for n in JOBS
                       if n in self.agents]
        planned = [n for n in PLANNED if n not in self.agents]
        return {
            "ts": _ts(), "demo": self.demo, "killed": self.killed,
            "shadow": bool(cfg.shadow),
            "mode": cfg.mode, "index": self.index,
            "header": {"broker": (self.connection.get("broker")
                                  if self.connection.get("state")
                                  == "CONNECTED"
                                  else getattr(self, "broker_name", "—")),
                       "connected": bool(self.ltp) if self.demo
                       else self.connection.get("state") == "CONNECTED",
                       "conn_state": self.connection.get("state", ""),
                       "ltps": header_ltps,
                       "ticks_per_s": round(self.ticks_per_s, 1),
                       "spot_ltp": _f(ltp),
                       "spot_dir": "up" if ltp > prev else
                       "dn" if ltp < prev else ""},
            "controls": {"index": self.index,
                         "indices": list(cfg.steps.keys()),
                         "expiry": getattr(self, "expiry", ""),
                         "lots": cfg.lot_multiplier,
                         "lot": self.lot,
                         "qty": self.lot * max(1, min(10, cfg.lot_multiplier)),
                         "step": cfg.steps.get(self.index)},
            "indicators": indicators,
            "signals": signals,
            "window": {"rows": rows, "selected": selected,
                       "center": self.window["center"] if self.window else None},
            "position": position,
            "log": list(self.log)[-40:],
            "journal": list(self.journal)[-200:],
            "agents": agents_rows, "agents_planned": planned,
            "watchdog": self.watchdog, "connection": self.connection,
            "bus_rates": dict(sorted(self.bus.stats.published.items(),
                                     key=lambda kv: -kv[1])[:8]),
            "settings": self._settings_view(),
        }

    def _settings_view(self) -> dict:
        c = self.cfg
        return {
            "broker_active": c.broker_active,
            "mode": c.mode, "lot_multiplier": c.lot_multiplier,
            "targets": {"t1": c.targets.t1, "t2": c.targets.t2,
                        "t3": c.targets.t3, "sl": c.targets.sl},
            "entry_window": list(c.entry_window), "square_off": c.square_off,
            "max_trades_per_day": c.max_trades_per_day,
            "max_daily_loss_inr": c.max_daily_loss_inr,
            "sl_streak_stop": c.sl_streak_stop,
            "stale_feed_s": c.stale_feed_s,
            "reconnect_stale_s": c.reconnect_stale_s,
            "signal_ttl_candles": c.signal_ttl_candles,
            "time_stop_candles": c.time_stop_candles,
            "orphan_policy": c.orphan_policy,
            "replay": c.replay,
            "shadow": c.shadow,
            "steps": dict(c.steps),
        }
