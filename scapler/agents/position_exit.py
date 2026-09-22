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
              Topic.CANDLE_CLOSED)

    def __init__(self, bus, settings: Settings) -> None:
        super().__init__(bus)
        self.cfg = settings
        self.trackers: dict[str, PositionExitTracker] = {}
        self.realized: dict[str, float] = {}
        self._reason: dict[str, ExitReason] = {}
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
            realized_pnl=self.realized.get(key, 0.0)))

    # ── messages ────────────────────────────────────────────────────
    async def on_message(self, env) -> None:
        t = env.topic
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
            for key, tr in list(self.trackers.items()):
                ltp = getattr(self, "_ltp", {}).get(key, tr.avg)
                act = tr.force_close(ExitReason.KILL, ltp)
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
            else:
                self._update(key, tr)
