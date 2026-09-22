"""ScaplerRuntime — builds bus + agents + UI for one trading session.

Two entry points:
  * ``run_desktop()`` — Windows: pywebview/WebView2 window, js_api commands,
    evaluate_js snapshots (plan §7). No HTTP server.
  * ``run_dev()`` — development/preview: aiohttp serves the SAME frontend
    plus a WebSocket snapshot stream; only for operator machines/sandbox,
    never part of the packaged desktop path.

Live mode requires a broker adapter (Phase 1/2 classes) + credentials; until
packaging (Phase 6) the runnable configuration is ``demo=True``: synthetic
labelled fixtures + DemoBroker (no real orders possible).
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from pathlib import Path

from ..agents.candle import CandleBuilderAgent
from ..agents.indicator import IndicatorAgent
from ..agents.market_data import MarketDataAgent
from ..agents.order import OrderAgent
from ..agents.position_exit import PositionExitAgent
from ..agents.risk import RiskAgent
from ..agents.signal import SignalAgent
from ..agents.strike import StrikeAgent
from ..agents.supervisor import SupervisorAgent
from ..agents.ui import UIAgent
from ..core import config as config_mod
from ..core.config import Settings
from ..core.messages import Topic
from .demo_feed import DemoAdapter, DemoBroker, demo_master

log = logging.getLogger(__name__)

WEB_DIR = Path(__file__).parent / "web"


class ScaplerRuntime:
    def __init__(self, settings: Settings | None = None, demo: bool = False,
                 settings_path: str | Path | None = None) -> None:
        self.cfg = settings or Settings()
        self.demo = demo
        self.settings_path = Path(settings_path) if settings_path else None
        self.bus = None
        self.agents: list = []
        self.ui: UIAgent | None = None
        self.secret_note = "secrets live in memory only (DPAPI store: Phase 6)"

        if demo:
            # demo days cycle continuously → lift the daily-trade breaker so
            # the scenario stays observable (UI shows the real counters)
            self.cfg = dataclasses.replace(self.cfg, max_trades_per_day=50)
            self.master = demo_master()
            self.adapter = DemoAdapter(self.master)
            self.broker = DemoBroker(self.master)
            self.broker_name = "DEMO (stub)"
        else:
            raise RuntimeError(
                "live broker wiring (credentials + adapters) lands with "
                "Phase 6 packaging; run with demo=True for now")
        self.index = self.cfg.index if self.cfg.index in self.master.indices \
            else next(iter(self.master.indices))
        self.spot_key = self.master.indices[self.index]
        self.expiry = self.master.nearest_expiry(self.index, "2026-09-22") \
            or ""

    # ── command callbacks (UI → runtime) ────────────────────────────
    def _commands(self) -> dict:
        return {
            "set_mode": self.cmd_set_mode,
            "set_lots": self.cmd_set_lots,
            "save_settings": self.cmd_save_settings,
            "broker_connect": lambda a: {
                "ok": False,
                "error": "live broker connect: Phase 6 (demo mode)"},
            "broker_disconnect": lambda a: {"ok": True},
            "broker_save": lambda a: {"ok": True, "note": self.secret_note},
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
        sig = next((a for a in self.agents if a.name == "signal"), None)
        if sig is not None:
            sig.set_mode(mode)
        return {"ok": True}

    def cmd_set_lots(self, args: dict) -> dict:
        n = max(1, min(10, int(args.get("lots", 1))))
        self._replace_cfg(lot_multiplier=n)
        return {"ok": True}

    def cmd_set_index(self, args: dict) -> dict:
        # window/candle/signal agents are key-bound → needs a session restart
        return {"ok": False,
                "error": "index change applies on restart (saved to "
                         "settings.json)"}

    def cmd_save_settings(self, args: dict) -> dict:
        changes = args.get("changes") or {}
        allowed = {"max_trades_per_day", "max_daily_loss_inr",
                   "sl_streak_stop", "time_stop_candles",
                   "signal_ttl_candles", "stale_feed_s", "reconnect_stale_s"}
        bad = set(changes) - allowed
        applied = {k: v for k, v in changes.items() if k in allowed}
        if applied:
            self._replace_cfg(**applied)
        if self.settings_path:
            config_mod.save(self.cfg, self.settings_path)
        return {"ok": True,
                "applied": sorted(applied),
                "restart_required": sorted(bad | ({"index"} if "index" in
                                                  changes else set()))}

    # ── wiring ──────────────────────────────────────────────────────
    def build(self, push_cb=None, hz: float = 10.0) -> None:
        from ..core.bus import EventBus
        bus = self.bus = EventBus()
        c = self.cfg
        md = MarketDataAgent(bus, self.adapter, [self.spot_key])
        cb = CandleBuilderAgent(bus, self.spot_key)
        ia = IndicatorAgent(bus)
        sa = SignalAgent(bus, c, self.index)
        st = StrikeAgent(bus, self.master, c, self.index, self.expiry)
        # demo days run at any wall-clock hour → pin the session clock inside
        # the entry window (DEMO only; live mode uses real IST)
        rk = RiskAgent(bus, c, clock_fn=(lambda: "10:30") if self.demo else None)
        oa = OrderAgent(bus, self.broker)
        px = PositionExitAgent(bus, c)
        index_keys = dict(self.master.indices)
        ui = UIAgent(bus, c, self.index, self.spot_key, index_keys,
                     hz=hz, push_cb=push_cb, on_command=self._commands(),
                     demo=self.demo)
        ui.broker_name = self.broker_name
        ui.expiry = self.expiry
        lots = {m.lot_size for m in self.master.options.values()
                if m.index_symbol == self.index}
        ui.lot = min(lots) if lots else 0
        sup = SupervisorAgent(bus, agents=(md, cb, ia, sa, st, rk, oa, px, ui),
                              beat_s=1.0)
        self.agents = [md, cb, ia, sa, st, rk, oa, px, ui, sup]
        self.ui = ui

    async def start(self) -> None:
        for a in self.agents:
            await a.start()

    async def stop(self) -> None:
        for a in reversed(self.agents):
            await a.stop()


# ── entry points ────────────────────────────────────────────────────
async def run_dev(host: str = "0.0.0.0", port: int = 8787,
                  demo: bool = True) -> None:
    from .transport import start_dev_server
    rt = ScaplerRuntime(demo=demo)
    rt.build()
    await rt.start()
    await start_dev_server(rt, host, port)


def run_desktop(demo: bool = False) -> None:      # pragma: no cover (Win)
    from .transport import run_webview
    run_webview(ScaplerRuntime(demo=demo))
