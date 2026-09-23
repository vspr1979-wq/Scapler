"""ScaplerRuntime — builds bus + all 13 agents + UI for one trading session.

Phase 6 wiring:
  * journal (SQLite WAL under ``data_dir``) and watchdog (stale-feed
    reconnect, square-off, orphan policy) join the stack — 13 agents total
    once the supervisor and UI are counted with the 8 trading agents and
    the connection agent.
  * ``set_index`` is INSTANT (user directive): the dropdown hot-switches
    candle/signal/strike/UI bindings and reconnects the feed against the
    memory-cached master; the choice persists to settings.json (disk).
    Switching is refused while a position is open.
  * broker connect = secrets (DPAPI store) → ConnectionAgent.login →
    master via memory→disk→network cache → live adapter hot-swap
    (refused while a position is open; one active broker).

Entry points: ``run_desktop()`` (pywebview/WebView2, Windows) and
``run_dev()`` (aiohttp preview server; demo mode only until credentials
are saved through the Settings tab).
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from datetime import date
from pathlib import Path

from ..agents.candle import CandleBuilderAgent
from ..agents.connection import ConnectionAgent
from ..agents.indicator import IndicatorAgent
from ..agents.journal import JournalAgent
from ..agents.market_data import MarketDataAgent
from ..agents.order import OrderAgent
from ..agents.position_exit import PositionExitAgent
from ..agents.risk import RiskAgent
from ..agents.signal import SignalAgent
from ..agents.strike import StrikeAgent
from ..agents.supervisor import SupervisorAgent
from ..agents.ui import UIAgent
from ..agents.watchdog import WatchdogAgent
from ..core import config as config_mod
from ..core.config import Settings
from ..core.messages import Topic
from ..core.secrets_store import SecretStore

log = logging.getLogger(__name__)

WEB_DIR = Path(__file__).parent / "web"


class ScaplerRuntime:
    def __init__(self, settings: Settings | None = None, demo: bool = False,
                 settings_path: str | Path | None = None) -> None:
        self.cfg = settings or Settings()
        self.demo = demo
        self.settings_path = Path(settings_path) if settings_path else None
        self.data_dir = Path(self.cfg.data_dir).expanduser()
        self.bus = None
        self.agents: list = []
        self.ui: UIAgent | None = None
        self.store = SecretStore(self.data_dir)

        if demo:
            # labelled synthetic fixtures — imported ONLY in demo mode; the
            # live path never touches this module
            from .demo_feed import DemoAdapter, DemoBroker, demo_master
            # demo days cycle continuously → lift the daily-trade breaker so
            # the scenario stays observable (UI shows the real counters)
            self.cfg = dataclasses.replace(self.cfg, max_trades_per_day=50)
            self.master = demo_master()
            self.adapter = DemoAdapter(self.master)
            self.broker = DemoBroker(self.master)
            self.broker_name = "DEMO (stub)"
        else:
            from ..brokers.base import MasterTable
            # empty placeholder until ConnectionAgent delivers the real
            # master (memory→disk→network cache) on broker connect
            self.master = MasterTable(options={}, indices={},
                                      source="awaiting-connection")
            self.adapter = _NullAdapter()
            self.broker = _NullBroker()
            self.broker_name = "no broker"
        self.index = self._resolve_index(self.cfg.index)
        self.spot_key = self.master.indices.get(self.index, "") \
            if self.master else ""
        self.expiry = self._nearest_expiry(self.index)

    # ── helpers ─────────────────────────────────────────────────────
    def _resolve_index(self, want: str) -> str:
        if not self.master or not self.master.indices:
            return want
        return want if want in self.master.indices \
            else next(iter(self.master.indices))

    def _nearest_expiry(self, index: str) -> str:
        if not self.master or not self.master.indices:
            return ""
        return self.master.nearest_expiry(index, date.today().isoformat()) \
            or ""

    def _lot_of(self, index: str) -> int:
        if not self.master:
            return 0
        lots = {m.lot_size for m in self.master.options.values()
                if m.index_symbol == index}
        return min(lots) if lots else 0

    def _open_position(self) -> bool:
        px = getattr(self, "px", None)
        return bool(px is not None and px.trackers)

    def _persist(self) -> None:
        if self.settings_path:
            config_mod.save(self.cfg, self.settings_path)

    # ── command callbacks (UI → runtime) ────────────────────────────
    def _commands(self) -> dict:
        return {
            "set_mode": self.cmd_set_mode,
            "set_lots": self.cmd_set_lots,
            "set_index": self.cmd_set_index,
            "save_settings": self.cmd_save_settings,
            "broker_save": self.cmd_broker_save,
            "broker_connect": self.cmd_broker_connect,
            "broker_disconnect": self.cmd_broker_disconnect,
            "journal_export": self.cmd_journal_export,
            "rearm": self.cmd_rearm,
        }

    def _replace_cfg(self, **kw) -> None:
        self.cfg = dataclasses.replace(self.cfg, **kw)
        for a in self.agents:                    # propagate to cfg holders
            if hasattr(a, "cfg"):
                a.cfg = dataclasses.replace(a.cfg, **kw)

    def cmd_set_mode(self, args: dict) -> dict:
        mode = args.get("mode", "MANUAL").upper()
        if mode not in ("AUTO", "MANUAL"):
            return {"ok": False, "error": "mode must be AUTO|MANUAL"}
        self._replace_cfg(mode=mode)
        self.sa.set_mode(mode)
        self._persist()
        return {"ok": True}

    def cmd_set_lots(self, args: dict) -> dict:
        n = max(1, min(10, int(args.get("lots", 1))))
        self._replace_cfg(lot_multiplier=n)
        self._persist()
        return {"ok": True}

    def cmd_set_index(self, args: dict) -> dict:
        """INSTANT index switch (user directive) — memory+disk cached master,
        no restart, no network. Refused only while a position is open."""
        index = str(args.get("index", "")).upper()
        if not self.master or index not in self.master.indices:
            return {"ok": False, "error": f"index {index} not in master"}
        if self._open_position():
            return {"ok": False,
                    "error": "close the open position before switching "
                             "index (one index traded at a time)"}
        self.index = index
        self.spot_key = self.master.indices[index]
        self.expiry = self._nearest_expiry(index)
        self._replace_cfg(index=index)
        self._persist()                            # disk cache of the choice
        # hot-rebind the key-bound agents
        self.cb.switch_key(self.spot_key)
        self.sa.switch_index(index)
        self.st.switch_index(index, self.expiry)
        self.ui.switch_index(index, self.spot_key, self.expiry,
                             self._lot_of(index))
        self.md.keys = [self.spot_key]             # window rebuild adds opts
        self.bus.publish(Topic.FEED_RECONNECT, f"index switch → {index}")
        return {"ok": True,
                "note": f"{index} · expiry {self.expiry} · "
                        f"lot {self._lot_of(index)} (instant, cached master)"}

    def cmd_save_settings(self, args: dict) -> dict:
        changes = args.get("changes") or {}
        allowed = {"max_trades_per_day", "max_daily_loss_inr",
                   "sl_streak_stop", "time_stop_candles",
                   "signal_ttl_candles", "stale_feed_s", "reconnect_stale_s",
                   "square_off", "shadow"}
        applied = {k: v for k, v in changes.items() if k in allowed}
        skipped = sorted(set(changes) - allowed - {"index"})
        if "index" in changes:                     # route through instant path
            r = self.cmd_set_index({"index": changes["index"]})
            if not r["ok"]:
                return r
        if applied:
            if "shadow" in applied:
                applied["shadow"] = bool(applied["shadow"])
            self._replace_cfg(**applied)
            note = ""
            if "shadow" in applied:
                # order edge + journal session label follow immediately
                self.oa.shadow = applied["shadow"]
                self.jr.shadow = applied["shadow"]
                note = ("SHADOW ON — orders recorded at real quotes, NOT sent"
                        if applied["shadow"] else
                        "SHADOW OFF — LIVE ORDERS will be sent to the broker")
        self._persist()
        out = {"ok": True, "applied": sorted(applied),
               "restart_required": skipped}
        if note:
            out["note"] = note
        return out

    async def cmd_broker_save(self, args: dict) -> dict:
        broker = args.get("broker", "")
        secrets = {k: args.get(k, "") for k in
                   ("api_key", "api_secret", "redirect_uri", "totp_secret")}
        return await self.conn.save_secrets(broker, secrets)

    async def cmd_broker_connect(self, args: dict) -> dict:
        broker = args.get("broker", "")
        if self._open_position():
            return {"ok": False,
                    "error": "close the open position before switching "
                             "broker (one active broker)"}
        r = await self.conn.connect(broker,
                                    {"code": args.get("code", "")})
        if r["ok"]:
            await self._adopt_live(broker)
        return r

    async def cmd_broker_disconnect(self, args: dict) -> dict:
        broker = args.get("broker", "")
        if self._open_position() and self.conn.active == broker:
            return {"ok": False,
                    "error": "position open on the active broker — "
                             "square off first"}
        return await self.conn.disconnect(broker)

    def cmd_rearm(self, args: dict) -> dict:
        """Clear a kill/halt once flat (new trading intent, same day)."""
        if self._open_position():
            return {"ok": False,
                    "error": "position open — square off before re-arming"}
        self.rk.killed = False
        self.wd.killed = False
        self.sup.killed = False
        self.ui.killed = False
        return {"ok": True, "note": "re-armed — entries allowed again"}

    def cmd_journal_export(self, args: dict) -> dict:
        try:
            path, n = self.jr.export_csv(args.get("path"))
            return {"ok": True, "note": f"exported {n} rows → {path}"}
        except Exception as e:
            return {"ok": False, "error": f"export failed: {e}"}

    async def _adopt_live(self, broker: str) -> None:
        """Hot-swap trading agents onto the live adapter+master (flat only)."""
        adapter = self.conn.adapter(broker)
        master = self.conn.master
        if adapter is None or master is None or not master.indices:
            return                      # nothing real to adopt yet
        self.master, self.adapter, self.broker = master, adapter, adapter
        self.broker_name = broker
        self.index = self._resolve_index(self.cfg.index)
        self.spot_key = master.indices[self.index]
        self.expiry = self._nearest_expiry(self.index)
        self.ui.index_keys = dict(master.indices)
        self.st.master = master
        self.st.switch_index(self.index, self.expiry)
        self.oa.adapter = adapter
        self.md.adapter = adapter
        self.md.keys = [self.spot_key]
        self.ui.switch_index(self.index, self.spot_key, self.expiry,
                             self._lot_of(self.index))
        self.ui.broker_name = broker
        self.bus.publish(Topic.FEED_RECONNECT, f"broker switch → {broker}")

    # ── wiring ──────────────────────────────────────────────────────
    def build(self, push_cb=None, hz: float = 10.0) -> None:
        from ..core.bus import EventBus
        bus = self.bus = EventBus()
        c = self.cfg
        self.md = MarketDataAgent(bus, self.adapter, [self.spot_key])
        self.cb = CandleBuilderAgent(bus, self.spot_key)
        self.ia = IndicatorAgent(bus)
        self.sa = SignalAgent(bus, c, self.index)
        self.st = StrikeAgent(bus, self.master, c, self.index, self.expiry)
        # demo days run at any wall-clock hour → pin the session clock inside
        # the entry window (DEMO only; live mode uses real IST)
        self.rk = RiskAgent(bus, c,
                            clock_fn=(lambda: "10:30") if self.demo else None)
        self.oa = OrderAgent(bus, self.broker, shadow=c.shadow)
        self.px = PositionExitAgent(bus, c)
        self.conn = ConnectionAgent(bus, c, self.store)
        self.jr = JournalAgent(bus, self.data_dir / "journal.sqlite",
                               broker=self.broker_name, demo=self.demo,
                               mode=c.mode, shadow=c.shadow)
        self.wd = WatchdogAgent(
            bus, c, clock_fn=(lambda: "10:30") if self.demo else None,
            orphan_check=self.conn.positions)
        index_keys = dict(self.master.indices) if self.master else {}
        self.ui = UIAgent(bus, c, self.index, self.spot_key, index_keys,
                          hz=hz, push_cb=push_cb, on_command=self._commands(),
                          demo=self.demo)
        self.ui.broker_name = self.broker_name
        self.ui.expiry = self.expiry
        self.ui.lot = self._lot_of(self.index)
        self.sup = SupervisorAgent(
            bus, agents=(self.md, self.cb, self.ia, self.sa, self.st,
                         self.rk, self.oa, self.px, self.conn, self.jr,
                         self.wd, self.ui), beat_s=1.0)
        self.agents = [self.md, self.cb, self.ia, self.sa, self.st, self.rk,
                       self.oa, self.px, self.conn, self.jr, self.wd,
                       self.ui, self.sup]

    async def start(self) -> None:
        for a in self.agents:
            await a.start()

    async def stop(self) -> None:
        for a in reversed(self.agents):
            await a.stop()


class _NullAdapter:
    """Pre-connect placeholder: opens an idle feed; orders are refused."""
    name = "none"
    master = None

    async def open_feed(self, keys, mode="full"):
        return _IdleHandle()

    async def update_subs(self, keys):
        pass


class _IdleHandle:
    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(3600)
        raise StopAsyncIteration

    async def close(self):
        pass


class _NullBroker:
    name = "none"
    master = None

    async def place_market_order(self, req):
        raise RuntimeError("no broker connected — Settings → broker connect")


# ── entry points ────────────────────────────────────────────────────
async def run_dev(host: str = "0.0.0.0", port: int = 8787,
                  demo: bool = True) -> None:
    from .transport import start_dev_server
    rt = ScaplerRuntime(demo=demo)
    rt.build()
    await rt.start()
    await start_dev_server(rt, host, port)


def desktop_runtime(demo: bool = False, settings_path: str | Path | None = None):
    """Live desktop runtime with settings LOADED FROM DISK (and persisted on
    every save). Default path: {data_dir}/settings.json."""
    path = Path(settings_path) if settings_path else \
        Path(Settings().data_dir).expanduser() / "settings.json"
    rt = ScaplerRuntime(settings=config_mod.load(path), demo=demo,
                        settings_path=path)
    return rt, path


def run_desktop(demo: bool = False,
                settings_path: str | Path | None = None) -> None:
    from .transport import run_webview                # pragma: no cover (Win)
    rt, _ = desktop_runtime(demo=demo, settings_path=settings_path)
    run_webview(rt)
