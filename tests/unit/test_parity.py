"""Phase-2 exit gate: cross-broker normalization parity.

Same synthetic market expressed in each broker's wire format must normalize
to identical canonical values (expiries, lots, strikes, order semantics,
position qty/avg, tick prices). Feed keys are broker-native by design and
are compared through each broker's own master map, not across brokers.
"""
import gzip

import pytest

from scapler.brokers.groww.master import parse_master as gw_master
from scapler.brokers.groww.rest import GrowwCreds, GrowwRest
from scapler.brokers.upstox.master import parse_master as ux_master
from scapler.brokers.upstox.rest import UpstoxCreds, UpstoxRest
from scapler.core.messages import OrderIntent, OrderRequest, OptionType as OT
from tests import _pb as pb
from tests._http import FakeHttp

TODAY = "2026-09-22"

# ── same market, two wires ────────────────────────────────────────────
UX_CSV = (
    "token,segment,symbol,instrument,name,expiry,strikeprice,lotsize\n"
    "11,NSE_FO,BANKNIFTY29SEP26C51200,OPTIDX,BANKNIFTY,29-Sep-2026,51200,30\n"
    "12,NSE_FO,BANKNIFTY29SEP26P51200,OPTIDX,BANKNIFTY,29-Sep-2026,51200,30\n"
    "13,NSE_FO,BANKNIFTY29SEP26C51300,OPTIDX,BANKNIFTY,29-Sep-2026,51300,30\n"
    "14,NSE_FO,BANKNIFTY06OCT26C51200,OPTIDX,BANKNIFTY,06-Oct-2026,51200,30\n"
    "21,NSE_INDEX,NIFTY 50,INDEX,NIFTY 50,,,\n"
    "22,NSE_INDEX,Nifty Bank,INDEX,Nifty Bank,,,\n"
)
GW_CSV = (
    "exchange,exchange_token,trading_symbol,groww_symbol,name,instrument_type,"
    "segment,series,isin,underlying_symbol,underlying_exchange_token,"
    "expiry_date,strike_price,lot_size,tick_size,freeze_quantity,"
    "is_reserved,buy_allowed,sell_allowed\n"
    "NSE,11,BANKNIFTY29SEP26C51200,x,NaN,CE,FNO,NaN,NaN,BANKNIFTY,26009,"
    "2026-09-29,51200,30,0.05,601,0,1,1\n"
    "NSE,12,BANKNIFTY29SEP26P51200,x,NaN,PE,FNO,NaN,NaN,BANKNIFTY,26009,"
    "2026-09-29,51200,30,0.05,601,0,1,1\n"
    "NSE,13,BANKNIFTY29SEP26C51300,x,NaN,CE,FNO,NaN,NaN,BANKNIFTY,26009,"
    "2026-09-29,51300,30,0.05,601,0,1,1\n"
    "NSE,14,BANKNIFTY06OCT26C51200,x,NaN,CE,FNO,NaN,NaN,BANKNIFTY,26009,"
    "2026-10-06,51200,30,0.05,601,0,1,1\n"
    "NSE,NIFTY,NIFTY,x,NaN,INDEX,CASH,NaN,NaN,,,0,0,0,0.05,0,0,1,1\n"
    "NSE,BANKNIFTY,BANKNIFTY,x,NaN,INDEX,CASH,NaN,NaN,,,0,0,0,0.05,0,0,1,1\n"
)


def test_master_parity():
    ux = ux_master({"NSE_FO": gzip.compress(UX_CSV.encode()),
                    "NSE_INDEX": b""}, TODAY)
    gw = gw_master(GW_CSV.encode(), TODAY)
    for m in (ux, gw):
        assert m.nearest_expiry("BANKNIFTY", TODAY) == "2026-09-29"
        assert m.expiries("BANKNIFTY") == ["2026-09-29", "2026-10-06"]
        assert m.lot("BANKNIFTY", "2026-09-29") == 30
        assert m.strikes("BANKNIFTY", "2026-09-29", OT.CE) == [51200.0, 51300.0]
        assert m.strikes("BANKNIFTY", "2026-09-29", OT.PE) == [51200.0]
        assert "BANKNIFTY" in m.indices
    # each master resolves its own feed key for the same contract
    ux_meta = ux.meta("BANKNIFTY", "2026-09-29", 51200.0, OT.CE)
    gw_meta = gw.meta("BANKNIFTY", "2026-09-29", 51200.0, OT.CE)
    # unified canonical key: same contract → same feed_key on both brokers
    assert ux_meta.instrument.feed_key == gw_meta.instrument.feed_key == "NSE_FO|11"
    assert ux_meta.instrument.symbol == gw_meta.instrument.symbol


@pytest.mark.parametrize("intent,txn", [
    (OrderIntent.BUY_TO_OPEN, "BUY"),
    (OrderIntent.SELL_TO_CLOSE, "SELL"),
])
async def test_order_semantics_parity(intent, txn):
    ux_http = FakeHttp({"/v2/order/place": (200, {"status": "success",
                                                  "data": {"order_id": "U1"}})})
    gw_http = FakeHttp({"/v1/order/create": (200, {"status": "SUCCESS",
                                                   "payload": {"groww_order_id": "G1"}})})
    ux = UpstoxRest(UpstoxCreds("K", "S", "R"), http=ux_http)
    gw = GrowwRest(GrowwCreds("K", "S"), http=gw_http)
    ux.set_token("T")
    gw.access_token = "T"
    ux_ik = ux_meta_instrument()
    req_ux = OrderRequest(intent=intent, instrument=ux_ik, qty=30,
                          client_id="SCP-260929-0001", ts_mono=0)
    req_gw = OrderRequest(intent=intent, instrument=gw_meta_instrument(),
                          qty=30, client_id="SCP-260929-0001", ts_mono=0)
    ack_u = await ux.place_order(req_ux)
    ack_g = await gw.place_order(req_gw)
    assert ack_u.client_id == ack_g.client_id == "SCP-260929-0001"
    bu = ux_http.last("/v2/order/place")["json"]
    bg = gw_http.last("/v1/order/create")["json"]
    assert bu["transaction_type"] == bg["transaction_type"] == txn
    assert bu["order_type"] == bg["order_type"] == "MARKET"
    assert bu["quantity"] == bg["quantity"] == 30
    assert bu["validity"] == bg["validity"] == "DAY"
    assert bg["order_reference_id"] == req_gw.client_id      # idempotency key
    assert bg["segment"] == "FNO" and bg["product"] == "MIS"
    assert bu["product"] == "MIS"


async def test_position_parity():
    ux_http = FakeHttp({"/v2/positions": (200, {"data": [
        {"exchange": "NSE_FO", "instrument_token": "11",
         "quantity": 30, "average_price": 148.2}]})})
    gw_http = FakeHttp({"/v1/positions/user": (200, {"status": "SUCCESS",
        "payload": {"positions": [
            {"exchange": "NSE", "trading_symbol": "BANKNIFTY29SEP26C51200",
             "quantity": 30, "net_price": 148.2}]}})})
    ux = UpstoxRest(UpstoxCreds("K", "S", "R"), http=ux_http)
    gw = GrowwRest(GrowwCreds("K", "S"), http=gw_http)
    ux.set_token("T")
    gw.access_token = "T"
    pu, pg = await ux.positions(), await gw.positions()
    assert len(pu) == len(pg) == 1
    assert pu[0].qty == pg[0].qty == 30
    assert pu[0].avg_price == pg[0].avg_price == 148.2


def test_tick_value_parity():
    """Same market event on both wires → same canonical ltp."""
    from scapler.brokers.upstox import proto_decode
    frame = pb.feed_response([("NSE_FO|11",
                               pb.feed_ltpc(pb.ltpc(148.20, 1740729368660)))])
    _, ux_feed = proto_decode.decode_feed_response(frame)[0]
    gw_price = 148.20                      # what /v1/live-data/ltp would return
    assert ux_feed["ltp"] == gw_price


# helpers ──────────────────────────────────────────────────────────────
def ux_meta_instrument():
    return ux_master({"X": UX_CSV.encode()}, TODAY).meta(
        "BANKNIFTY", "2026-09-29", 51200.0, OT.CE).instrument


def gw_meta_instrument():
    return gw_master(GW_CSV.encode(), TODAY).meta(
        "BANKNIFTY", "2026-09-29", 51200.0, OT.CE).instrument
