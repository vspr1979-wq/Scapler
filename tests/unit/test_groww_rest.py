"""Groww REST v1 against recorded-shape FakeHttp."""
import pytest

from scapler.brokers.groww.rest import GrowwCreds, GrowwRest
from scapler.core.messages import (
    InstrumentKey, OrderIntent, OrderRequest, OptionType,
)
from tests._http import FakeHttp

CREDS = GrowwCreds(api_key="KEY", api_secret="SEC")
CE = InstrumentKey(exchange="NSE_FO", token="60123",
                   symbol="BANKNIFTY29SEP26C51200", strike=51200.0,
                   expiry="2026-09-29", option_type=OptionType.CE)


@pytest.fixture
def rest():
    http = FakeHttp({
        "/v1/token/api/access": (200, {"access_token": "GTOK"}),
        "/v1/live-data/ltp": (200, {"status": "SUCCESS", "payload": {
            "NSE_BANKNIFTY29SEP26C51200": 148.2, "NSE_NIFTY": 24512.3}}),
        "/v1/live-data/quote": (200, {"status": "SUCCESS", "payload": {
            "last_price": 148.2, "bid_price": 148.15, "offer_price": 148.25,
            "open_interest": 256800, "last_trade_time": 1740729368660}}),
        "/v1/order/create": (200, {"status": "SUCCESS", "payload": {
            "groww_order_id": "GORD1", "order_status": "OPEN",
            "order_reference_id": "SCP-260929-0001"}}),
        "/v1/order/cancel": (200, {"status": "SUCCESS", "payload": {
            "groww_order_id": "GORD1", "order_status": "CANCELLED"}}),
        "/v1/positions/user": (200, {"status": "SUCCESS", "payload": {
            "positions": [
                {"exchange": "NSE", "trading_symbol": "BANKNIFTY29SEP26C51200",
                 "quantity": 30, "net_price": 148.2, "product": "MIS"},
                {"exchange": "NSE", "trading_symbol": "OTHER",
                 "quantity": 0, "net_price": 0, "product": "MIS"}]}}),
    })
    return GrowwRest(CREDS, http=http), http


async def test_token_exchange_key_secret(rest):
    r, http = rest
    tok = await r.get_access_token()
    assert tok == "GTOK"
    call = http.last("/v1/token/api/access")
    assert call["json"] == {"api_key": "KEY", "secret": "SEC"}


async def test_token_exchange_totp(rest):
    r, http = rest
    await r.get_access_token(totp="123456")
    call = http.last("/v1/token/api/access")
    assert call["json"] == {"api_key": "KEY", "totp": "123456"}


async def test_authed_headers(rest):
    r, _ = rest
    r.access_token = "GTOK"
    await r.ltp_batch("FNO", ["NSE_BANKNIFTY29SEP26C51200"])
    call = r.http.last("/v1/live-data/ltp")
    assert call["headers"]["Authorization"] == "Bearer GTOK"
    assert call["headers"]["X-API-VERSION"] == "1.0"


async def test_ltp_batch_parse(rest):
    r, _ = rest
    r.access_token = "GTOK"
    out = await r.ltp_batch("FNO", ["NSE_BANKNIFTY29SEP26C51200", "NSE_NIFTY"])
    assert out == {"NSE_BANKNIFTY29SEP26C51200": 148.2, "NSE_NIFTY": 24512.3}


async def test_requires_login(rest):
    r, _ = rest
    with pytest.raises(RuntimeError):
        await r.ltp_batch("FNO", ["X"])


async def test_place_order_body_and_ack(rest):
    r, http = rest
    r.access_token = "GTOK"
    req = OrderRequest(intent=OrderIntent.BUY_TO_OPEN, instrument=CE, qty=30,
                       client_id="SCP-260929-0001", ts_mono=0)
    ack = await r.place_order(req)
    assert ack.broker_order_id == "GORD1" and ack.client_id == req.client_id
    body = http.last("/v1/order/create")["json"]
    assert body == {
        "trading_symbol": "BANKNIFTY29SEP26C51200", "quantity": 30,
        "validity": "DAY", "exchange": "NSE", "segment": "FNO",
        "product": "MIS", "order_type": "MARKET", "transaction_type": "BUY",
        "order_reference_id": "SCP-260929-0001"}


async def test_place_order_sell_intent(rest):
    r, http = rest
    r.access_token = "GTOK"
    await r.place_order(OrderRequest(intent=OrderIntent.SELL_TO_CLOSE,
                                     instrument=CE, qty=30,
                                     client_id="SCP-260929-0002", ts_mono=0))
    assert http.last("/v1/order/create")["json"]["transaction_type"] == "SELL"


async def test_place_failure_raises(rest):
    r, http = rest
    r.access_token = "GTOK"
    http.routes["/v1/order/create"] = (200, {"status": "FAILURE",
                                             "remark": "insufficient margin"})
    with pytest.raises(RuntimeError):
        await r.place_order(OrderRequest(intent=OrderIntent.BUY_TO_OPEN,
                                         instrument=CE, qty=30,
                                         client_id="X8", ts_mono=0))


async def test_cancel_body(rest):
    r, http = rest
    r.access_token = "GTOK"
    await r.cancel_order("GORD1")
    assert http.last("/v1/order/cancel")["json"] == {
        "segment": "FNO", "groww_order_id": "GORD1"}


async def test_positions_mapping(rest):
    r, _ = rest
    r.access_token = "GTOK"
    pos = await r.positions()
    assert len(pos) == 1
    assert pos[0].feed_key == "NSE_FO|BANKNIFTY29SEP26C51200"
    assert pos[0].qty == 30 and pos[0].avg_price == 148.2
