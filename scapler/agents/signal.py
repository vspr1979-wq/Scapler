"""SignalAgent — FSM on closed candles; AUTO fires, MANUAL waits for the UI.

Publishes signal.new / signal.state for every transition; emits
signal.execute(side) when AUTO arms, or when the UI clicks EXECUTE while the
side is SIGNALED (MANUAL). HELD coupling comes from position.update.
"""
from __future__ import annotations

from dataclasses import replace

from ..core.config import Settings
from ..core.messages import OptionType, Topic
from ..strategy.setup import setup_broken, setup_valid
from ..strategy.signal_fsm import SignalEngine, S
from .base_imports import Agent


class SignalAgent(Agent):
    name = "signal"
    topics = (Topic.CANDLE_CLOSED, Topic.INDICATORS_READY,
              Topic.UI_EXECUTE, Topic.POSITION_UPDATE)

    def __init__(self, bus, settings: Settings, index: str) -> None:
        super().__init__(bus)
        self.cfg = settings
        self.index = index
        self.atr_min = settings.setup.atr_min.get(index, 0.0)
        self.fsm = SignalEngine(settings)
        self._candles: dict[str, object] = {}

    def set_mode(self, mode: str) -> None:
        self.cfg = replace(self.cfg, mode=mode)

    def switch_index(self, index: str) -> None:
        """Instant index switch: fresh FSMs (DISARMED both sides), new
        atr_min, candle cache dropped — signals only from the new index's
        CLOSED candles. HELD stays False: switching is refused while a
        position is open (runtime guard)."""
        self.index = index
        self.atr_min = self.cfg.setup.atr_min.get(index, 0.0)
        self.fsm = SignalEngine(self.cfg)
        self._candles.clear()

    async def on_message(self, env) -> None:
        if env.topic == Topic.CANDLE_CLOSED:
            self._candles[env.payload.key] = env.payload
            return
        if env.topic == Topic.POSITION_UPDATE:
            p = env.payload
            for st in self.fsm.set_held(p.qty_open > 0):
                self.publish(Topic.SIGNAL_STATE, st)
            return
        if env.topic == Topic.UI_EXECUTE:
            side = env.payload
            if self.fsm.fsm[side].state is S.SIGNALED:
                self.publish(Topic.SIGNAL_EXECUTE, side,
                             key=self.fsm.fsm[side].side.value)
            return
        # INDICATORS_READY
        ind = env.payload
        c = self._candles.get(ind.key)
        if c is None:
            return
        atr_min = self.atr_min
        valid, broken = {}, {}
        for side in OptionType:
            valid[side] = setup_valid(side, ind, c, self.cfg.setup, atr_min)
            broken[side] = setup_broken(side, ind, c)
        for st in self.fsm.on_candle(c.close_ts, valid, broken):
            topic = Topic.SIGNAL_NEW if st.new is S.SIGNALED else Topic.SIGNAL_STATE
            self.publish(topic, st)
            if st.new is S.SIGNALED and self.cfg.mode == "AUTO":
                self.publish(Topic.SIGNAL_EXECUTE, st.side)

