"""T1/T2/T3/SL exit ladder — sell-to-close only, whole lots only (plan.md §6).

N ≥ 2 lots : scale out  floor(N/2) at T1, floor(rem/2) at T2, residual at T3
N = 1 lot  : no partials — trail ladder  T1 → SL=BE,  T2 → SL=entry+T1,
             T3 → flat at market
After any T1 hit the residual SL trails to breakeven; after T2 to entry+T1.
SL / time-stop / square-off / kill always sell the FULL residual.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..core.config import Targets
from ..core.messages import ExitReason, InstrumentKey


@dataclass(frozen=True)
class ExitAction:
    reason: ExitReason
    qty: int
    ref_price: float


class PositionExitTracker:
    def __init__(self, instrument: InstrumentKey, lot_size: int, lots: int,
                 avg_price: float, targets: Targets) -> None:
        if lots < 1 or lot_size < 1:
            raise ValueError("lots and lot_size must be ≥ 1")
        self.instrument = instrument
        self.lot_size = lot_size
        self.lots = lots
        self.avg = avg_price
        self.t = targets
        self.qty = lot_size * lots
        self.initial_qty = self.qty
        self.t1_qty = (lots // 2) * lot_size if lots >= 2 else 0
        rem_lots = lots - (lots // 2 if lots >= 2 else 0)
        self.t2_qty = (rem_lots // 2) * lot_size
        self.sl_price = avg_price - targets.sl
        self.t1_hit = False
        self.t2_hit = False
        self.closed = False

    # ── tick-driven ─────────────────────────────────────────────────
    def on_tick(self, ltp: float) -> list[ExitAction]:
        if self.closed or self.qty <= 0:
            return []
        acts: list[ExitAction] = []
        if ltp <= self.sl_price:                       # full residual, first
            acts.append(self._sell(ExitReason.SL, self.qty, self.sl_price))
            return acts
        if not self.t1_hit and ltp >= self.avg + self.t.t1:
            self.t1_hit = True
            if self.t1_qty:
                acts.append(self._sell(ExitReason.T1, self.t1_qty, self.avg + self.t.t1))
            self.sl_price = self.avg                   # breakeven trail
        if self.t1_hit and not self.t2_hit and ltp >= self.avg + self.t.t2:
            self.t2_hit = True
            if self.t2_qty:
                acts.append(self._sell(ExitReason.T2, self.t2_qty, self.avg + self.t.t2))
            self.sl_price = self.avg + self.t.t1       # lock T1 on residual
        if ltp >= self.avg + self.t.t3:
            acts.append(self._sell(ExitReason.T3, self.qty, self.avg + self.t.t3))
        return acts

    def force_close(self, reason: ExitReason, ref_price: float) -> ExitAction | None:
        if self.closed or self.qty <= 0:
            return None
        return self._sell(reason, self.qty, ref_price)

    # ── internals ───────────────────────────────────────────────────
    def _sell(self, reason: ExitReason, qty: int, ref: float) -> ExitAction:
        qty = min(qty, self.qty)
        self.qty -= qty
        if self.qty == 0:
            self.closed = True
        return ExitAction(reason=reason, qty=qty, ref_price=ref)
