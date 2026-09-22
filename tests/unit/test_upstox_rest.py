"""Upstox REST v2 against a recorded-shape FakeHttp."""
import orjson
import pytest

from scapler.brokers.upstox.rest import UpstoxCreds, UpstoxRest
from scapler.core.messages import (
    InstrumentKey, OrderIntent, OrderRequest, OptionType,
)

CREDS = UpstoxCreds(api_key="KEY", api_secret="SEC",
                    redirect_uri="https://localhost/oauth")
CE = InstrumentKey(exchange="NSE_FO", token="45450",
                   symbol="BANKNIFTY29SEP26C51200", strike=51200.0,
                   option_type=OptionType.CE)


class FakeHttp:
    def __init__(self, routes: dict):
        self.routes = routes
        self.calls = []

    async def request(self, method, url, *, headers=None, params=None,
                      json=None, data=None):
        self.calls.append({"method": method, "url": url, "headers": headers,
                           "json": json, "data": data})
        for path, (status, body) in self.routes.items():
            if path in url:
                return status, (body if isinstance(body, bytes)
                                else orjson.dumps(body))
        return 404, orjson.dumps({"status": "failure"})


@pytest.fixture
def rest():
    http = FakeHttp({
        "/v2/login/authorization/token": (200, {"access_token": "TOK123"}),
        "/v3/feed/market-data-feed/authorize": (
            200, {"status": "success",
                  "data": {"authorized_redirect_uri": "wss://feeder/x?code=1"}}),
        "/v2/order/place": (200, {"status": "success",
                                  "data": {"order_id": "ORD1"}}),
        "/v2/order/cancel/ORD1": (200, {"status": "success"}),
        "/v2/positions": (200, {"data": [
            {"exchange": "NSE_FO", "instrument_token": "45450",
             "quantity": 30, "average_price": 148.2},
            {"exchange": "NSE_FO", "instrument_token": "777",
             "quantity": 0, "average_price": 0},
        ]}),
    })
    return UpstoxRest(CREDS, http=http), http


async def test_auth_url_contains_client_and_redirect(rest):
    r, _ = rest
    u = r.auth_url("st1")
    assert "client_id=KEY" in u and "response_type=code" in u
    assert "redirect_uri=https%3A%2F%2Flocalhost%2Foauth" in u and "state=st1" in u


async def test_token_exchange(rest):
    r, http = rest
    tok = await r.exchange_token("CODE9")
    assert tok == "TOK123" and r.access_token == "TOK123"
    call = http.calls[0]
    assert call["data"]["grant_type"] == "authorization_code"
    assert call["data"]["code"] == "CODE9"
    assert call["data"]["client_id"] == "KEY"


async def test_authed_calls_require_token(rest):
    r, _ = rest
    with pytest.raises(RuntimeError):
        await r.authorize_feed_url()


async def test_feed_authorize_returns_wss(rest):
    r, _ = rest
    r.set_token("TOK123")
    url = await r.authorize_feed_url()
    assert url.startswith("wss://")


async def test_place_order_buy_to_open_body(rest):
    r, http = rest
    r.set_token("TOK123")
    req = OrderRequest(intent=OrderIntent.BUY_TO_OPEN, instrument=CE, qty=30,
                       client_id="SCP-260929-0001", ts_mono=0)
    ack = await r.place_order(req)
    assert ack.broker_order_id == "ORD1" and ack.client_id == req.client_id
    body = http.calls[-1]["json"]
    assert body["transaction_type"] == "BUY"
    assert body["order_type"] == "MARKET" and body["product"] == "MIS"
    assert body["validity"] == "DAY" and body["quantity"] == 30
    assert body["exchange"] == "NSE_FO" and body["instrument_token"] == "45450"
    assert body["tag"] == "scapler"
    assert http.calls[-1]["headers"]["Authorization"] == "Bearer TOK123"


async def test_place_order_sell_to_close_body(rest):
    r, http = rest
    r.set_token("TOK123")
    req = OrderRequest(intent=OrderIntent.SELL_TO_CLOSE, instrument=CE, qty=30,
                       client_id="SCP-260929-0002", ts_mono=0)
    await r.place_order(req)
    assert http.calls[-1]["json"]["transaction_type"] == "SELL"


async def test_place_order_failure_raises(rest):
    r, http = rest
    r.set_token("TOK123")
    http.routes["/v2/order/place"] = (400, {"status": "failure",
                                            "message": "insufficient margin"})
    with pytest.raises(RuntimeError):
        await r.place_order(OrderRequest(intent=OrderIntent.BUY_TO_OPEN,
                                         instrument=CE, qty=30,
                                         client_id="X", ts_mono=0))


async def test_cancel_and_positions(rest):
    r, _ = rest
    r.set_token("TOK123")
    await r.cancel_order("ORD1")
    pos = await r.positions()
    assert len(pos) == 1                      # zero-qty filtered
    assert pos[0].feed_key == "NSE_FO|45450"
    assert pos[0].qty == 30 and pos[0].avg_price == 148.2
