"""Groww REST v1 behind the injectable Http seam (official docs, 2026-09):

  POST /v1/token/api/access            api_key+secret | api_key+totp → token
                                       (150 req / 24 h — ConnectionAgent caches)
  GET  /v1/live-data/ltp               batch ≤50 exchange_symbols per segment
  GET  /v1/live-data/quote             single symbol: bid/ask/OI/IV/last time
  POST /v1/order/create                MARKET order, order_reference_id=idempotency
  POST /v1/order/cancel                {segment, groww_order_id}
  GET  /v1/positions/user?segment=FNO  net qty + net_price
  GET  growwapi-assets…/instrument.csv instrument master
Headers: Authorization Bearer + X-API-VERSION: 1.0.
Rate limits (Orders 10/s·250/min, Live 10/s·300/min) are enforced later by the
OrderAgent/MarketDataAgent token buckets (plan §4.3).
"""
from __future__ import annotations

import dataclasses

import orjson

from ...core.messages import OrderIntent, OrderRequest
from ..base import OrderAck, Position
from ..upstox.rest import AioHttp, Http

BASE = "https://api.groww.in"
INSTRUMENT_CSV = "https://growwapi-assets.groww.in/instruments/instrument.csv"

# canonical exchange suffix → groww segment
_SEGMENT = {"FO": "FNO", "INDEX": "CASH", "CASH": "CASH"}
# groww segment → canonical exchange suffix
_SUFFIX = {"FNO": "FO", "CASH": "INDEX"}


@dataclasses.dataclass(frozen=True)
class GrowwCreds:
    api_key: str
    api_secret: str = ""        # key+secret flow (daily approval)
    totp_secret: str = ""       # TOTP flow (no expiry); base32 secret
    access_token: str = ""      # pre-generated token (console)


class GrowwRest:
    def __init__(self, creds: GrowwCreds, http: Http | None = None) -> None:
        self.creds = creds
        self.http = http or AioHttp()
        self.access_token = creds.access_token or None

    # ── auth ────────────────────────────────────────────────────────
    def auth_url(self, state: str = "") -> str:
        """Groww issues keys/tokens from the API console (no redirect flow)."""
        return "https://groww.in/trade-api/api-keys"

    async def get_access_token(self, totp: str | None = None) -> str:
        body: dict = {"api_key": self.creds.api_key}
        if totp:
            body["totp"] = totp
        elif self.creds.api_secret:
            body["secret"] = self.creds.api_secret
        else:
            raise RuntimeError("groww: need api_secret, totp or access_token")
        status, raw = await self.http.request(
            "POST", f"{BASE}/v1/token/api/access",
            headers={"Content-Type": "application/json",
                     "Accept": "application/json"}, json=body)
        if status != 200:
            raise RuntimeError(f"groww token failed: {status} {raw[:200]!r}")
        j = orjson.loads(raw)
        tok = j.get("access_token") or j.get("payload", {}).get("access_token")
        if not tok:
            raise RuntimeError(f"groww token missing in response: {raw[:200]!r}")
        self.access_token = tok
        return tok

    async def _authed(self, method: str, path: str, **kw) -> tuple[int, bytes]:
        if not self.access_token:
            raise RuntimeError("groww: not logged in")
        headers = kw.pop("headers", {})
        headers.update({"Authorization": f"Bearer {self.access_token}",
                        "Accept": "application/json",
                        "X-API-VERSION": "1.0"})
        return await self.http.request(method, f"{BASE}{path}",
                                       headers=headers, **kw)

    @staticmethod
    def _payload(j: dict) -> dict:
        return j.get("payload") or j.get("data") or j

    # ── market data ─────────────────────────────────────────────────
    async def ltp_batch(self, segment: str,
                        exchange_symbols: list[str]) -> dict[str, float]:
        status, raw = await self._authed(
            "GET", "/v1/live-data/ltp",
            params={"segment": segment,
                    "exchange_symbols": ",".join(exchange_symbols)})
        if status != 200:
            raise RuntimeError(f"groww ltp failed: {status}")
        return {k: float(v) for k, v in self._payload(orjson.loads(raw)).items()}

    async def quote(self, exchange: str, segment: str,
                    trading_symbol: str) -> dict:
        status, raw = await self._authed(
            "GET", "/v1/live-data/quote",
            params={"exchange": exchange, "segment": segment,
                    "trading_symbol": trading_symbol})
        if status != 200:
            raise RuntimeError(f"groww quote failed: {status}")
        return self._payload(orjson.loads(raw))

    # ── master ──────────────────────────────────────────────────────
    async def master_bytes(self) -> bytes:
        status, body = await self.http.request("GET", INSTRUMENT_CSV)
        if status != 200:
            raise RuntimeError(f"groww master download failed: {status}")
        return body

    # ── orders ──────────────────────────────────────────────────────
    async def place_order(self, req: OrderRequest) -> OrderAck:
        exch, seg = req.instrument.exchange.split("_")[:2]       # NSE_FO → NSE, FO
        segment = _SEGMENT.get(seg, seg)                         # FO → FNO
        body = {
            "trading_symbol": req.instrument.symbol,
            "quantity": req.qty,
            "validity": "DAY",
            "exchange": exch,                             # NSE | BSE
            "segment": segment,                           # FNO
            "product": "MIS",                             # intraday, v1
            "order_type": "MARKET",
            "transaction_type": ("BUY" if req.intent is OrderIntent.BUY_TO_OPEN
                                 else "SELL"),
            "order_reference_id": req.client_id,          # 8-20 alnum, ≤2 hyphens
        }
        status, raw = await self._authed("POST", "/v1/order/create", json=body)
        j = orjson.loads(raw)
        if status != 200 or j.get("status") != "SUCCESS":
            raise RuntimeError(f"groww place_order failed: {status} {raw[:300]!r}")
        p = self._payload(j)
        return OrderAck(client_id=req.client_id,
                        broker_order_id=p["groww_order_id"],
                        status=p.get("order_status", "OPEN"))

    async def cancel_order(self, broker_order_id: str,
                           segment: str = "FNO") -> None:
        status, raw = await self._authed(
            "POST", "/v1/order/cancel",
            json={"segment": segment, "groww_order_id": broker_order_id})
        if status != 200:
            raise RuntimeError(f"groww cancel failed: {status} {raw[:200]!r}")

    async def positions(self, segment: str = "FNO") -> list[Position]:
        status, raw = await self._authed("GET", "/v1/positions/user",
                                         params={"segment": segment})
        if status != 200:
            raise RuntimeError(f"groww positions failed: {status}")
        out = []
        for p in self._payload(orjson.loads(raw)).get("positions", []):
            qty = int(p.get("quantity", 0))
            if qty:
                # provisional key: "<EX>_FO|<trading_symbol>"; the adapter
                # translates symbol → exchange_token feed_key via the master
                out.append(Position(
                    feed_key=f"{p['exchange']}_{_SUFFIX.get(segment, segment)}"
                             f"|{p['trading_symbol']}",
                    qty=qty, avg_price=float(p.get("net_price", 0.0))))
        return out
