"""Upstox REST v2 + feed-v3 authorize, behind an injectable Http seam.

Endpoints (official v2/v3):
  POST /v2/login/authorization/token            OAuth2 code → access_token
  GET  /v3/feed/market-data-feed/authorize      → one-time wss:// redirect URI
  GET  assets…/exchange/{EX}.csv.gz             instrument master (per exchange)
  POST /v2/order/place                          MARKET order
  DELETE /v2/order/cancel/{order_id}
  GET  /v2/positions
"""
from __future__ import annotations

import asyncio
import dataclasses
from typing import Protocol
from urllib.parse import quote

import orjson

from ...core.messages import OrderIntent, OrderRequest
from ..base import OrderAck, Position

BASE = "https://api.upstox.com"
MASTER_URL = ("https://assets.upstox.com/market-quote/instruments/"
              "exchange/{ex}.csv.gz")
MASTER_EXCHANGES = ("NSE_FO", "BSE_FO", "NSE_INDEX", "BSE_INDEX")
MASTER_FALLBACK = ("https://assets.upstox.com/market-quote/instruments/"
                   "exchange/complete.csv.gz")


class Http(Protocol):
    async def request(self, method: str, url: str, *, headers: dict | None = None,
                      params: dict | None = None, json: dict | None = None,
                      data: dict | None = None) -> tuple[int, bytes]: ...


class AioHttp:
    """Production Http. aiohttp imported lazily so unit tests stay dep-free."""

    def __init__(self) -> None:
        self._session = None

    async def _sess(self):
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session

    async def request(self, method, url, *, headers=None, params=None,
                      json=None, data=None):
        s = await self._sess()
        async with s.request(method, url, headers=headers, params=params,
                             json=json, data=data) as r:
            return r.status, await r.read()

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None


@dataclasses.dataclass(frozen=True)
class UpstoxCreds:
    api_key: str
    api_secret: str
    redirect_uri: str


class UpstoxRest:
    def __init__(self, creds: UpstoxCreds, http: Http | None = None) -> None:
        self.creds = creds
        self.http = http or AioHttp()
        self.access_token: str | None = None

    # ── auth ────────────────────────────────────────────────────────
    def auth_url(self, state: str) -> str:
        return (f"{BASE}/v2/login/authorization/dialog?client_id={self.creds.api_key}"
                f"&response_type=code&redirect_uri={quote(self.creds.redirect_uri, safe='')}"
                f"&state={state}")

    async def exchange_token(self, code: str) -> str:
        status, body = await self.http.request(
            "POST", f"{BASE}/v2/login/authorization/token",
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "Accept": "application/json"},
            data={"code": code, "client_id": self.creds.api_key,
                  "client_secret": self.creds.api_secret,
                  "grant_type": "authorization_code",
                  "redirect_uri": self.creds.redirect_uri})
        if status != 200:
            raise RuntimeError(f"upstox token exchange failed: {status} {body[:200]!r}")
        self.access_token = orjson.loads(body)["access_token"]
        return self.access_token

    def set_token(self, token: str) -> None:
        self.access_token = token

    async def _authed(self, method: str, path: str, **kw) -> tuple[int, bytes]:
        if not self.access_token:
            raise RuntimeError("upstox: not logged in")
        headers = kw.pop("headers", {})
        headers.update({"Authorization": f"Bearer {self.access_token}",
                        "Accept": "application/json"})
        return await self.http.request(method, f"{BASE}{path}",
                                       headers=headers, **kw)

    # ── feed authorize (v3) ─────────────────────────────────────────
    async def authorize_feed_url(self) -> str:
        for attempt in range(3):
            status, body = await self._authed(
                "GET", "/v3/feed/market-data-feed/authorize")
            if status == 200:
                return orjson.loads(body)["data"]["authorized_redirect_uri"]
            if status == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"upstox feed authorize failed: {status}")
        raise RuntimeError("upstox feed authorize: max retries exceeded")

    # ── instrument master ───────────────────────────────────────────
    async def master_bytes(self) -> tuple[dict[str, bytes], str]:
        out: dict[str, bytes] = {}
        for ex in MASTER_EXCHANGES:
            status, body = await self.http.request("GET", MASTER_URL.format(ex=ex))
            if status == 200:
                out[ex] = body
        if out:
            return out, "per-exchange"
        status, body = await self.http.request("GET", MASTER_FALLBACK)
        if status != 200:
            raise RuntimeError(f"upstox master download failed: {status}")
        return {"COMPLETE": body}, "complete"

    # ── orders ──────────────────────────────────────────────────────
    async def place_order(self, req: OrderRequest) -> OrderAck:
        body = {
            "quantity": req.qty,
            "product": "MIS",                      # intraday, v1
            "validity": "DAY",
            "price": 0.0,
            "order_type": "MARKET",
            "transaction_type": ("BUY" if req.intent is OrderIntent.BUY_TO_OPEN
                                 else "SELL"),
            "exchange": req.instrument.exchange,
            "instrument_token": req.instrument.token,
            "tag": "scapler",
        }
        status, raw = await self._authed("POST", "/v2/order/place", json=body)
        parsed = orjson.loads(raw)
        if status != 200 or parsed.get("status") != "success":
            raise RuntimeError(f"upstox place_order failed: {status} {raw[:300]!r}")
        return OrderAck(client_id=req.client_id,
                        broker_order_id=parsed["data"]["order_id"],
                        status="PUT")

    async def cancel_order(self, broker_order_id: str) -> None:
        status, raw = await self._authed(
            "DELETE", f"/v2/order/cancel/{broker_order_id}")
        if status != 200:
            raise RuntimeError(f"upstox cancel failed: {status} {raw[:200]!r}")

    async def positions(self) -> list[Position]:
        status, raw = await self._authed("GET", "/v2/positions")
        if status != 200:
            raise RuntimeError(f"upstox positions failed: {status}")
        out = []
        for p in orjson.loads(raw).get("data", []):
            qty = int(p.get("quantity", 0))
            if qty:
                out.append(Position(
                    feed_key=f"{p['exchange']}|{p['instrument_token']}",
                    qty=qty, avg_price=float(p.get("average_price", 0.0))))
        return out
