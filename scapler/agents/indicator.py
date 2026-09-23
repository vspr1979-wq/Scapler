"""IndicatorAgent — closed candles → indicators.ready (incremental, O(1))."""
from __future__ import annotations

from ..core.messages import Topic
from ..strategy.indicators import IndicatorEngine
from .base_imports import Agent


class IndicatorAgent(Agent):
    name = "indicator"
    topics = (Topic.CANDLE_CLOSED,)

    def __init__(self, bus) -> None:
        super().__init__(bus)
        self.engines: dict[str, IndicatorEngine] = {}

    def engine(self, key: str) -> IndicatorEngine:
        e = self.engines.get(key)
        if e is None:
            e = self.engines[key] = IndicatorEngine(key)
        return e

    async def on_message(self, env) -> None:
        candle = env.payload
        ready = self.engine(candle.key).on_candle(candle)
        self.publish(Topic.INDICATORS_READY, ready, key=candle.key)
