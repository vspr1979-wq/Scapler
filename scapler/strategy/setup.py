"""Entry setup evaluation on CLOSED 1-min candles (plan.md §5-F7).

valid  = all trend clauses true  → may arm a signal (FSM decides)
broken = a trend clause violated → re-arm prerequisite (plan.md §5-F6)
Volume/RSI-band/ATR clauses gate entries but do NOT define the break:
the break is the trend condition flipping (close vs VWAP, EMA9 vs EMA21).
"""
from __future__ import annotations

from ..core.config import Setup
from ..core.messages import CandleClosed, IndicatorsReady, OptionType


def setup_valid(side: OptionType, ind: IndicatorsReady, candle: CandleClosed,
                s: Setup, atr_min: float) -> bool:
    if ind.atr < atr_min or ind.vol_ratio < s.vol_mult:
        return False
    if side is OptionType.CE:
        return (candle.c > ind.vwap
                and ind.ema9 > ind.ema21
                and s.rsi_ce[0] <= ind.rsi <= s.rsi_ce[1])
    return (candle.c < ind.vwap
            and ind.ema9 < ind.ema21
            and s.rsi_pe[0] <= ind.rsi <= s.rsi_pe[1])


def setup_broken(side: OptionType, ind: IndicatorsReady, candle: CandleClosed) -> bool:
    if side is OptionType.CE:
        return candle.c < ind.vwap or ind.ema9 <= ind.ema21
    return candle.c > ind.vwap or ind.ema9 >= ind.ema21
