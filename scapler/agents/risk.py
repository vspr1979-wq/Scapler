"""RiskAgent — veto power over every ENTRY; exits always pass (plan §5-F8).

Veto codes: V_KILL V_HELD V_WINDOW V_MAXTRADES V_DAILYLOSS V_SLSTREAK V_STALE.
Counters feed the Signal Panel (trades x/N, SL-streak, P&L).
"""
from __future__ import annotations

from ..core.clock import ist_hhmm, mono_ns
from ..core.config import Settings
from ..core.messages import OrderIntent, RiskVeto, Topic
from .base_imports import Agent


class RiskAgent(Agent):
    name = "risk"
    topics = (Topic.ORDER_REQUEST, Topic.ORDER_FILL, Topic.POSITION_UPDATE,
              Topic.KILL_SWITCH, Topic.TICK_RAW, Topic.SESSION_NEW_DAY)

    def __init__(self, bus, settings: Settings, clock_fn=None) -> None:
        super().__init__(bus)
        self.cfg = settings
        self._clock = clock_fn or ist_hhmm
        self.trades = 0
        self.sl_streak = 0
        self.pnl = 0.0
        self.killed = False
        self.held = False
        self._last_tick = 0

    def _veto(self) -> str | None:
        c = self.cfg
        if self.killed:
            return "V_KILL"
        if self.held:
            return "V_HELD"
        if not (c.entry_window[0] <= self._clock() <= c.entry_window[1]):
            return "V_WINDOW"
        if self.trades >= c.max_trades_per_day:
            return "V_MAXTRADES"
        if self.pnl <= -abs(c.max_daily_loss_inr):
            return "V_DAILYLOSS"
        if self.sl_streak >= c.sl_streak_stop:
            return "V_SLSTREAK"
        if self._last_tick and \
                (mono_ns() - self._last_tick) / 1e9 > c.stale_feed_s:
            return "V_STALE"
        return None

    async def on_message(self, env) -> None:
        t = env.topic
        if t == Topic.TICK_RAW:
            self._last_tick = mono_ns()
            return
        if t == Topic.KILL_SWITCH:
            self.killed = True
            return
        if t == Topic.SESSION_NEW_DAY:
            # daily breakers reset for the new session (journal rolls the
            # session too); kill state clears — a new day starts unarmed
            self.trades = 0
            self.sl_streak = 0
            self.pnl = 0.0
            self.held = False
            self.killed = False
            return
        if t == Topic.ORDER_FILL:
            if env.payload.intent is OrderIntent.BUY_TO_OPEN:
                self.trades += 1
            return
        if t == Topic.POSITION_UPDATE:
            p = env.payload
            self.held = p.qty_open > 0
            if p.closed:
                self.pnl += p.realized_pnl
                self.sl_streak = self.sl_streak + 1 if p.exit_reason == "SL" \
                    else 0
            return
        # ORDER_REQUEST
        req = env.payload
        if req.intent is OrderIntent.SELL_TO_CLOSE:
            self.publish(Topic.ORDER_APPROVED, req)      # exits never vetoed
            return
        code = self._veto()
        if code:
            self.publish(Topic.RISK_VETO, RiskVeto(code=code,
                                                   context=req.client_id))
        else:
            self.publish(Topic.ORDER_APPROVED, req)
