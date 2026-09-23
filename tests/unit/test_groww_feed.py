"""Groww poll feed: batch LTP → Ticks on change only; close stops pump."""
import asyncio

from scapler.brokers.groww.feed import GrowwPollFeed
from scapler.brokers.groww.rest import GrowwCreds, GrowwRest
from tests._http import FakeHttp

SUBS = {
    "NSE|FNO|BANKNIFTY29SEP26C51200": ("NSE", "FNO", "BANKNIFTY29SEP26C51200"),
    "NSE|CASH|NIFTY": ("NSE", "CASH", "NIFTY"),
}


def _http(payload):
    return FakeHttp({"/v1/live-data/ltp": (200, {"status": "SUCCESS",
                                                 "payload": payload})})


async def _open(http, interval=0.01):
    rest = GrowwRest(GrowwCreds("K", "S"), http=http)
    rest.access_token = "T"
    handle = await GrowwPollFeed(rest, interval=interval).open(SUBS)
    return handle


async def test_first_cycle_emits_all_keys():
    handle = await _open(_http({"NSE_BANKNIFTY29SEP26C51200": 148.2,
                                "NSE_NIFTY": 24512.3}))
    seen = {}
    for _ in range(2):
        t = await asyncio.wait_for(handle.__anext__(), 2)
        seen[t.key] = t.ltp
    assert seen == {"NSE|FNO|BANKNIFTY29SEP26C51200": 148.2,
                    "NSE|CASH|NIFTY": 24512.3}
    await handle.close()


async def test_unchanged_prices_not_re_emitted_then_change_flows():
    http = _http({"NSE_BANKNIFTY29SEP26C51200": 148.2, "NSE_NIFTY": 24512.3})
    handle = await _open(http)
    for _ in range(2):
        await asyncio.wait_for(handle.__anext__(), 2)
    # several idle cycles → nothing new
    await asyncio.sleep(0.05)
    http.routes["/v1/live-data/ltp"] = (
        200, {"status": "SUCCESS",
              "payload": {"NSE_BANKNIFTY29SEP26C51200": 149.05,
                          "NSE_NIFTY": 24512.3}})
    t = await asyncio.wait_for(handle.__anext__(), 2)
    assert t.key == "NSE|FNO|BANKNIFTY29SEP26C51200" and t.ltp == 149.05
    assert t.exch_ts_ns > 0
    await handle.close()


async def test_rest_error_cycle_survives():
    http = FakeHttp({"/v1/live-data/ltp": (429, {"status": "FAILURE"})})
    handle = await _open(http)
    await asyncio.sleep(0.05)                       # rate-limited cycles skipped
    http.routes["/v1/live-data/ltp"] = (
        200, {"status": "SUCCESS", "payload": {"NSE_NIFTY": 1.0}})
    t = await asyncio.wait_for(handle.__anext__(), 2)
    assert t.ltp == 1.0
    await handle.close()


async def test_close_terminates_iterator():
    handle = await _open(_http({"NSE_NIFTY": 1.0}))
    await handle.close()
    try:
        await asyncio.wait_for(handle.__anext__(), 1)
        raised = False
    except (StopAsyncIteration, asyncio.TimeoutError):
        raised = True
    assert raised
