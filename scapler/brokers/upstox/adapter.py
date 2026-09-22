"""Upstox BrokerAdapter — composes REST v2 + master parser + Streamer v3."""
from __future__ import annotations

import dataclasses
from datetime import date

from ...core.messages import OrderRequest
from ..base import BrokerAdapter, MasterTable, OrderAck, Position
from .feed_v3 import UpstoxFeedV3
from .master import parse_master
from .rest import UpstoxCreds, UpstoxRest


class UpstoxAdapter(BrokerAdapter):
    name = "upstox"

    def __init__(self, creds: UpstoxCreds, http=None, ws_factory=None) -> None:
        self.rest = UpstoxRest(creds, http=http)
        self.feed = UpstoxFeedV3(self.rest, ws_factory=ws_factory)
        self.master: MasterTable | None = None

    def auth_url(self, state: str) -> str:
        return self.rest.auth_url(state)

    async def login(self, code: str) -> None:
        await self.rest.exchange_token(code)

    async def load_master(self, today: str | None = None) -> MasterTable:
        blobs, source = await self.rest.master_bytes()
        m = parse_master(blobs, today or date.today().isoformat())
        self.master = dataclasses.replace(m, source=f"upstox:{source}")
        return self.master

    async def open_feed(self, keys: list[str], mode: str = "full"):
        return await self.feed.open(keys, mode)

    async def place_market_order(self, req: OrderRequest) -> OrderAck:
        return await self.rest.place_order(req)

    async def cancel_order(self, broker_order_id: str) -> None:
        await self.rest.cancel_order(broker_order_id)

    async def positions(self) -> list[Position]:
        return await self.rest.positions()

    async def disconnect(self) -> None:
        self.rest.access_token = None            # block further authed calls
        close = getattr(self.rest.http, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass
