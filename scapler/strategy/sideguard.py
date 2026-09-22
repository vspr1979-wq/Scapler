"""SideGuard — hard structural guard, last line before any adapter call
(plan.md §5-F2). Belt to the ``OrderIntent`` enum's braces.

Rejects, with journal-able reason codes:
  SG_NON_OPTION        instrument is not an option (stock/future/index)
  SG_SELL_WITHOUT_LONG SELL with no matching open long (sell-to-open)
  SG_SELL_EXCEEDS_QTY  SELL qty > open long qty (would flip into a short)
  SG_BAD_QTY           non-positive or non-whole-lot quantity
"""
from __future__ import annotations

from ..core.messages import OrderIntent, OrderRequest


class SideGuardError(Exception):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


def check(req: OrderRequest, open_qty: int, lot_size: int) -> None:
    if req.instrument.option_type is None:
        raise SideGuardError("SG_NON_OPTION", req.instrument.symbol)
    if req.qty <= 0 or req.qty % lot_size != 0:
        raise SideGuardError("SG_BAD_QTY", f"qty={req.qty} lot={lot_size}")
    if req.intent is OrderIntent.SELL_TO_CLOSE:
        if open_qty <= 0:
            raise SideGuardError("SG_SELL_WITHOUT_LONG", req.instrument.symbol)
        if req.qty > open_qty:
            raise SideGuardError("SG_SELL_EXCEEDS_QTY",
                                 f"sell={req.qty} open={open_qty}")
