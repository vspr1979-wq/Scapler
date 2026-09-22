"""11-strike window math: 5 ITM + ATM + 5 OTM, re-centering, strike pick.

Never a full-chain scan (plan.md §5-F5): the window is 11 strikes around the
ATM; subscriptions are 22 options (CE+PE per strike) + spot feeds only.
Moneyness labels are per side — for CE, ITM = below spot; for PE, mirrored.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..core.messages import OptionType

WINDOW_RADIUS = 5   # 5 ITM + ATM + 5 OTM


def atm_strike(spot: float, step: float) -> float:
    return round(spot / step) * step


@dataclass(frozen=True)
class Window:
    center: float
    step: float
    strikes: tuple[float, ...]      # ascending, len == 11

    def __post_init__(self):
        if len(self.strikes) != 2 * WINDOW_RADIUS + 1:
            raise ValueError("window must be 5 ITM + ATM + 5 OTM")


def build_window(spot: float, step: float) -> Window:
    center = atm_strike(spot, step)
    strikes = tuple(center + (i - WINDOW_RADIUS) * step
                    for i in range(2 * WINDOW_RADIUS + 1))
    return Window(center=center, step=step, strikes=strikes)


def needs_recenter(w: Window, spot: float, recenter_steps: int = 2) -> bool:
    return abs(spot - w.center) >= recenter_steps * w.step


def moneyness_label(strike: float, w: Window, side: OptionType) -> str:
    off = round((w.center - strike) / w.step)      # CE view: below center = ITM
    if side is OptionType.PE:
        off = -off
    if off == 0:
        return "ATM"
    return f"{'ITM' if off > 0 else 'OTM'}{abs(off)}"


@dataclass(frozen=True)
class StrikeQuote:
    strike: float
    delta: float | None
    bid: float
    ask: float
    tick: float
    volume_1m: float


def spread_ticks(q: StrikeQuote) -> int:
    if q.tick <= 0:
        return 10**9
    return round((q.ask - q.bid) / q.tick)


def select_strike(side: OptionType, w: Window, quotes: list[StrikeQuote],
                  delta_band: tuple[float, float], max_spread_ticks: int,
                  ) -> tuple[float, float | None, int]:
    """Pick inside the window: |delta| in band + liquid; nearest ATM wins.
    Fallback = ATM (delta may be None when the feed has no greeks)."""
    cands = []
    for q in quotes:
        if q.delta is None or q.volume_1m <= 0:
            continue
        d = q.delta if side is OptionType.CE else -q.delta
        if delta_band[0] <= d <= delta_band[1] and spread_ticks(q) <= max_spread_ticks:
            cands.append(q)
    if cands:
        q = min(cands, key=lambda x: abs(x.strike - w.center))
        return q.strike, q.delta, spread_ticks(q)
    atm_q = next((q for q in quotes if q.strike == w.center), None)
    if atm_q is not None:
        return atm_q.strike, atm_q.delta, spread_ticks(atm_q)
    return w.center, None, 0
