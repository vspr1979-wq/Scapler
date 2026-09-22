"""StrikeAgent — 11-strike window owner + strike pick + entry order request.

Subscriptions footprint (plan §5-F5): spot + 22 window options, nothing else.
Re-centers on ±2-step spot drift (window.rebuilt carries the new sub keys so
MarketDataAgent can resubscribe). Strike pick = |δ|∈band + liquid, nearest
ATM; ATM fallback when the feed has no greeks (Groww poll mode).
"""
from __future__ import annotations

from ..core.config import Settings
from ..core.ids import new_client_id
from ..core.messages import (
    InstrumentKey, OrderIntent, OrderRequest, OptionType, StrikeSelected,
    Topic,
)
from ..strategy.strikes import (
    StrikeQuote, build_window, needs_recenter, select_strike,
)
from .base_imports import Agent

TICK_SIZE = 0.05          # index options tick; master tick_size lands Phase 6


class StrikeAgent(Agent):
    name = "strike"
    topics = (Topic.TICK_RAW, Topic.SIGNAL_EXECUTE)

    def __init__(self, bus, master, settings: Settings, index: str,
                 expiry: str) -> None:
        super().__init__(bus)
        self.master = master
        self.cfg = settings
        self.index = index
        self.expiry = expiry
        self.spot_key = master.indices[index]
        self.spot = 0.0
        self.window = None
        self.key_map: dict[str, tuple[float, OptionType]] = {}
        self.quotes: dict[tuple[float, OptionType], StrikeQuote] = {}
        self.sub_keys: list[str] = [self.spot_key]

    # ── window ──────────────────────────────────────────────────────
    def _rebuild_if_needed(self, force: bool = False) -> bool:
        if not force and (self.window is None or
                          not needs_recenter(self.window, self.spot,
                                             self.cfg.recenter_steps)):
            return False
        self.window = build_window(self.spot, self._step())
        self.key_map, self.quotes = {}, {}
        keys = [self.spot_key]
        for strike in self.window.strikes:
            for opt in OptionType:
                meta = self.master.meta(self.index, self.expiry, strike, opt)
                if meta is not None:
                    self.key_map[meta.instrument.feed_key] = (strike, opt)
                    keys.append(meta.instrument.feed_key)
        self.sub_keys = keys
        # always announce (first build included): MarketData resubscribes and
        # the UI strike-window panel renders from this payload.
        self.publish(Topic.WINDOW_REBUILT,
                     {"index": self.index, "center": self.window.center,
                      "keys": keys, "step": self._step(),
                      "strikes": list(self.window.strikes),
                      "map": {fk: [s, o.value]
                              for fk, (s, o) in self.key_map.items()}})
        return True

    def _step(self) -> float:
        # strike STEP is a Settings value (plan §1); the master validates
        # lots/tokens/expiries and Phase-6 reconciles step drift warnings
        return self.cfg.steps.get(self.index, 100.0)

    # ── messages ────────────────────────────────────────────────────
    async def on_message(self, env) -> None:
        tick = env.payload if env.topic == Topic.TICK_RAW else None
        if tick is not None:
            if tick.key == self.spot_key:
                self.spot = tick.ltp
                if self.window is None:
                    self._rebuild_if_needed(force=True)
                else:
                    self._rebuild_if_needed()
                return
            so = self.key_map.get(tick.key)
            if so is not None:
                strike, opt = so
                self.quotes[(strike, opt)] = StrikeQuote(
                    strike=strike, delta=tick.delta, bid=tick.bid,
                    ask=tick.ask or tick.ltp, tick=TICK_SIZE,
                    volume_1m=1.0 if tick.ltp > 0 else 0.0)
            return
        # SIGNAL_EXECUTE(side)
        side: OptionType = env.payload
        if self.window is None:
            return
        strike, delta, spread = select_strike(
            side, self.window, list(self.quotes.values()),
            self.cfg.setup.delta_band, self.cfg.setup.max_spread_ticks)
        meta = self.master.meta(self.index, self.expiry, strike, side)
        if meta is None:
            return
        self.publish(Topic.STRIKE_SELECTED, StrikeSelected(
            side=side, instrument=meta.instrument, delta=delta,
            spread_ticks=spread, window_lo=self.window.strikes[0],
            window_hi=self.window.strikes[-1]))
        lot = meta.lot_size
        qty = lot * max(1, min(10, self.cfg.lot_multiplier))
        self.publish(Topic.ORDER_REQUEST, OrderRequest(
            intent=OrderIntent.BUY_TO_OPEN, instrument=meta.instrument,
            qty=qty, client_id=new_client_id(), ts_mono=env.ts_mono))
