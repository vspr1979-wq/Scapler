"""Groww BrokerAdapter — composes REST v1 + master parser + poll feed."""
from __future__ import annotations

import dataclasses
from datetime import date

from ...core.messages import OrderRequest
from ..base import BrokerAdapter, MasterTable, OrderAck, Position
from .feed import GrowwPollFeed
from .master import parse_master
from .rest import GrowwCreds, GrowwRest


class GrowwAdapter(BrokerAdapter):
    name = "groww"

    def __init__(self, creds: GrowwCreds, http=None,
                 poll_interval: float = 0.25) -> None:
        self.rest = GrowwRest(creds, http=http)
        self.feed = GrowwPollFeed(self.rest, interval=poll_interval)
        self.master: MasterTable | None = None

    def auth_url(self, state: str = "") -> str:
        return self.rest.auth_url(state)

    async def login(self, code: str = "") -> None:
        if self.rest.access_token:
            return
        totp = None
        if self.rest.creds.totp_secret:
            import pyotp                       # optional dep, TOTP flow only
            totp = pyotp.TOTP(self.rest.creds.totp_secret).now()
        await self.rest.get_access_token(totp=totp)

    async def load_master(self, today: str | None = None) -> MasterTable:
        body = await self.rest.master_bytes()
        m = parse_master(body, today or date.today().isoformat())
        self.master = dataclasses.replace(m, source="groww:instrument.csv")
        return self.master

    async def open_feed(self, keys: list[str], mode: str = "full"):
        if self.master is None:
            raise RuntimeError("groww: load_master() before open_feed()")
        subs: dict[str, tuple[str, str, str]] = {}
        for key in keys:
            meta = self.master.options.get(key)
            if meta is not None:
                ik = meta.instrument
                subs[key] = (ik.exchange.split("_")[0], "FNO", ik.symbol)
            elif key in self.master.indices.values():
                ex_seg, token = key.split("|", 1)
                subs[key] = (ex_seg.split("_")[0], "CASH", token)
        return await self.feed.open(subs)

    async def place_market_order(self, req: OrderRequest) -> OrderAck:
        return await self.rest.place_order(req)

    async def cancel_order(self, broker_order_id: str) -> None:
        await self.rest.cancel_order(broker_order_id)

    async def positions(self) -> list[Position]:
        raw = await self.rest.positions()
        if self.master is None:
            return raw
        by_symbol = {m.instrument.symbol: k for k, m in self.master.options.items()}
        out = []
        for p in raw:
            ex_seg, symbol = p.feed_key.split("|", 1)
            key = by_symbol.get(symbol)
            out.append(dataclasses.replace(p, feed_key=key) if key else p)
        return out

    async def disconnect(self) -> None:
        self.rest.access_token = None
        close = getattr(self.rest.http, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass
