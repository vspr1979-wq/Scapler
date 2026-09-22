"""PositionExitAgent — T1/T2/T3/SL tick-watch, sell-to-close only (plan §6).

One tracker per open long. Exits bypass entry vetoes (Risk auto-approves
SELL_TO_CLOSE). Time-stop counts closed spot candles while a position is
open. Kill switch force-closes the residual at market.
"""
from __future__ import annotations

from ..core.config import Settings
from ..core.ids import new_client_id
from ..core.messages import (
    ExitReason, ExitTrigger, OrderIntent, OrderRequest, PositionUpdate, Topic,
)
from ..strategy.exits import PositionExitTracker
from .base_imports import Agent


class PositionExitAgent(Agent):
    name = "position_exit"
    topics = (Topic.ORDER_FILL, Topic.TICK_RAW, Topic.KILL_SWITCH,
              Topic.CANDLE_CLOSED, Topic.EXIT_TRIGGER)

    def __init__(self, bus, settings: Settings) -> None:
        super().__init__(bus)
        self.cfg = settings
        self.trackers: dict[str, PositionExitTracker] = {}
        self.realized: dict[str, float] = {}
        self._reason: dict[str, ExitReason] = {}
        self._ui_state: dict[str, tuple] = {}
        self._candles_open = 0

    # ── helpers ─────────────────────────────────────────────────────
    def _exit_requests(self, key, tracker, acts) -> None:
        for a in acts:
            self._reason[key] = a.reason
            self.publish(Topic.EXIT_TRIGGER, ExitTrigger(
                reason=a.reason, instrument=tracker.instrument,
                qty=a.qty, ref_price=a.ref_price))
            self.publish(Topic.ORDER_REQUEST, OrderRequest(
                intent=OrderIntent.SELL_TO_CLOSE,
                instrument=tracker.instrument, qty=a.qty,
                client_id=new_client_id(), ts_mono=0))

    def _update(self, key, tracker, closed=False) -> None:
        self.publish(Topic.POSITION_UPDATE, PositionUpdate(
            feed_key=key, qty_open=tracker.qty, avg_price=tracker.avg,
            closed=closed or tracker.closed,
            exit_reason=self._reason.get(key, ExitReason.T3).value
            if tracker.closed else "",
            realized_pnl=self.realized.get(key, 0.0),
            t1_hit=tracker.t1_hit, t2_hit=tracker.t2_hit,
            sl_price=tracker.sl_price))

    # ── messages ────────────────────────────────────────────────────
    async def on_message(self, env) -> None:
        t = env.topic
        if t == Topic.EXIT_TRIGGER:
            # echo of our own triggers except MANUAL exits raised by the UI —
            # those close the tracker here; the UI sends the SELL request
            # itself (Risk auto-approves, SideGuard validates the long).
            p = env.payload
            if p.reason is ExitReason.MANUAL:
                key = p.instrument.feed_key
                tr = self.trackers.get(key)
                if tr is not None:
                    self._reason[key] = ExitReason.MANUAL
                    tr.force_close(ExitReason.MANUAL, p.ref_price)
            return
        if t == Topic.CANDLE_CLOSED:
            if self.trackers:
                self._candles_open += 1
                if self._candles_open > self.cfg.time_stop_candles:
                    for key, tr in list(self.trackers.items()):
                        ltp = getattr(self, "_ltp", {}).get(key, tr.avg)
                        act = tr.force_close(ExitReason.TIME_STOP, ltp)
                        if act:
                            self._exit_requests(key, tr, [act])
            return
        if t == Topic.KILL_SWITCH:
            # watchdog square-off books as SQUARE_OFF; manual/other kills as
            # KILL — the journal and journal-tab reasons stay distinguishable
            reason = ExitReason.SQUARE_OFF \
                if getattr(env.payload, "source", "") == "watchdog" \
                else ExitReason.KILL
            for key, tr in list(self.trackers.items()):
                ltp = getattr(self, "_ltp", {}).get(key, tr.avg)
                act = tr.force_close(reason, ltp)
                if act:
                    self._exit_requests(key, tr, [act])
            return
        if t == Topic.TICK_RAW:
            tick = env.payload
            tr = self.trackers.get(tick.key)
            if tr is None:
                return
            if not hasattr(self, "_ltp"):
                self._ltp = {}
            self._ltp[tick.key] = tick.ltp
            self._exit_requests(tick.key, tr, tr.on_tick(tick.ltp))
            # trail-state change (T1/T2 hit, SL ratcheted) → announce so the
            # UI position panel shows live ladder state between fills
            sig = (tr.t1_hit, tr.t2_hit, tr.sl_price)
            if not tr.closed and self._ui_state.get(tick.key) != sig:
                self._ui_state[tick.key] = sig
                self._update(tick.key, tr)
            return
        # ORDER_FILL
        f = env.payload
        key = f.instrument.feed_key
        if f.intent is OrderIntent.BUY_TO_OPEN:
            lot = f.lot_size or 1
            tr = PositionExitTracker(f.instrument, lot, f.qty // lot,
                                     f.price, self.cfg.targets)
            self.trackers[key] = tr
            self.realized[key] = 0.0
            self._ui_state[key] = (tr.t1_hit, tr.t2_hit, tr.sl_price)
            self._candles_open = 0
            self._update(key, tr)
        else:
            tr = self.trackers.get(key)
            if tr is None:
                return
            self.realized[key] = self.realized.get(key, 0.0) + \
                (f.price - tr.avg) * f.qty
            if tr.closed:
                self._update(key, tr, closed=True)
                del self.trackers[key]
                self._ui_state.pop(key, None)
            else:
                self._update(key, tr)
