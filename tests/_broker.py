"""Duck-typed stub broker for OrderAgent tests (place only)."""
from __future__ import annotations

from scapler.brokers.base import OrderAck


class StubBroker:
    name = "stub"

    def __init__(self, master=None) -> None:
        self.master = master
        self.orders = []
        self.fail = False

    async def place_market_order(self, req):
        self.orders.append(req)
        if self.fail:
            raise RuntimeError("stub: exchange reject")
        return OrderAck(client_id=req.client_id,
                        broker_order_id="STUB-" + req.client_id,
                        status="FILLED")
