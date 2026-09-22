"""OrderAgent — SideGuard gate + MARKET dispatch + fill publication.

v1 fill model: MARKET orders are reported filled at the last seen LTP for the
instrument (order-update WS reconciliation lands with the Watchdog, Phase 6);
the journal records the assumption. Idempotency: client_id seen once.
"""
from __future__ import annotations

from ..core.clock import mono_ns
from ..core.messages import OrderFill, OrderIntent, OrderRejected, Topic
from ..strategy.sideguard import SideGuardError, check as sideguard_check
from .base_imports import Agent


class OrderAgent(Agent):
    name = "order"
    topics = (Topic.ORDER_APPROVED, Topic.TICK_RAW)

    def __init__(self, bus, adapter) -> None:
        super().__init__(bus)
        self.adapter = adapter
        self.ltp: dict[str, float] = {}
        self.open: dict[str, int] = {}
        self._seen: set[str] = set()

    def _lot_size(self, feed_key: str) -> int:
        master = getattr(self.adapter, "master", None)
        meta = master.options.get(feed_key) if master else None
        return meta.lot_size if meta else 1

    async def on_message(self, env) -> None:
        if env.topic == Topic.TICK_RAW:
            self.ltp[env.payload.key] = env.payload.ltp
            return
        req = env.payload
        if req.client_id in self._seen:
            return
        self._seen.add(req.client_id)
        key = req.instrument.feed_key
        try:
            sideguard_check(req, self.open.get(key, 0), self._lot_size(key))
        except SideGuardError as e:
            self.publish(Topic.ORDER_REJECTED,
                         OrderRejected(client_id=req.client_id, reason=e.code))
            return
        self.publish(Topic.ORDER_REQ, req)
        t0 = mono_ns()
        ack = await self.adapter.place_market_order(req)
        lat = (mono_ns() - t0) / 1e6
        price = self.ltp.get(key, 0.0)
        if req.intent is OrderIntent.BUY_TO_OPEN:
            self.open[key] = self.open.get(key, 0) + req.qty
        else:
            self.open[key] = max(0, self.open.get(key, 0) - req.qty)
        self.publish(Topic.ORDER_FILL, OrderFill(
            client_id=req.client_id, instrument=req.instrument,
            intent=req.intent, qty=req.qty, price=price, latency_ms=lat,
            ts_mono=mono_ns(), lot_size=self._lot_size(key)),
            key=key)
        self.last_ack = ack
