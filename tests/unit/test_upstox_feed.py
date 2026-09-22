"""Streamer v3 handle: subscribe payload, frame → Tick pipeline, clean close,
bad frames never kill the pump."""
import asyncio

import orjson

from scapler.brokers.upstox.feed_v3 import UpstoxFeedV3
from scapler.brokers.upstox.rest import UpstoxCreds, UpstoxRest
from tests import _pb as pb


class FakeWS:
    def __init__(self, frames):
        self.frames = list(frames)
        self.sent = []
        self.closed = False

    async def send(self, payload):
        self.sent.append(payload)

    async def recv(self):
        while not self.frames:
            await asyncio.sleep(3600)          # parked until close()
        return self.frames.pop(0)

    async def close(self):
        self.closed = True


class FakeHttp:
    async def request(self, method, url, **kw):
        return 200, orjson.dumps({"data": {"authorized_redirect_uri":
                                           "wss://feeder/test"}})


async def _open(frames, mode="full", keys=("NSE_FO|45450",)):
    rest = UpstoxRest(UpstoxCreds("K", "S", "R"), http=FakeHttp())
    rest.set_token("T")
    ws = FakeWS(frames)
    feed = UpstoxFeedV3(rest, ws_factory=lambda url: _awaitable(ws))
    handle = await feed.open(list(keys), mode=mode)
    return handle, ws


async def _awaitable(ws):
    return ws


async def test_subscribe_payload_shape():
    handle, ws = await _open([])
    msg = orjson.loads(ws.sent[0])
    assert msg["method"] == "sub"
    assert msg["data"]["mode"] == "full"
    assert msg["data"]["instrumentKeys"] == ["NSE_FO|45450"]
    assert msg["guid"]
    await handle.close()


async def test_frames_become_ticks_in_order():
    f1 = pb.feed_response([("NSE_FO|45450", pb.feed_ltpc(pb.ltpc(148.20, 1000)))])
    f2 = pb.feed_response([("NSE_FO|45450", pb.feed_ltpc(pb.ltpc(149.05, 2000)))])
    handle, _ = await _open([f1, f2])
    t1 = await asyncio.wait_for(handle.__anext__(), 2)
    t2 = await asyncio.wait_for(handle.__anext__(), 2)
    assert (t1.ltp, t1.exch_ts_ns) == (148.20, 1000 * 1_000_000)
    assert (t2.ltp, t2.exch_ts_ns) == (149.05, 2000 * 1_000_000)
    assert t1.key == "NSE_FO|45450"
    await handle.close()


async def test_bad_frame_skipped_pump_survives():
    good1 = pb.feed_response([("NSE_FO|1", pb.feed_ltpc(pb.ltpc(1.0, 1)))])
    good2 = pb.feed_response([("NSE_FO|1", pb.feed_ltpc(pb.ltpc(2.0, 2)))])
    handle, _ = await _open([good1, b"\xde\xad\xbe\xef\xff", good2])
    t1 = await asyncio.wait_for(handle.__anext__(), 2)
    t2 = await asyncio.wait_for(handle.__anext__(), 2)
    assert (t1.ltp, t2.ltp) == (1.0, 2.0)
    await handle.close()


async def test_greeks_flow_into_tick():
    fl = pb.first_level(pb.ltpc(148.20, 5), depth=pb.quote(1, 148.15, 1, 148.25),
                        greeks_b=pb.greeks(0.53, 0.0045), vtt=10, oi=99.0)
    frame = pb.feed_response([("NSE_FO|9", pb.feed_first_level(fl))])
    handle, _ = await _open([frame], keys=("NSE_FO|9",))
    t = await asyncio.wait_for(handle.__anext__(), 2)
    assert t.bid == 148.15 and t.ask == 148.25
    assert t.delta == 0.53 and t.gamma == 0.0045 and t.oi == 99.0
    await handle.close()


async def test_close_stops_pump_and_socket():
    handle, ws = await _open([])
    await handle.close()
    assert ws.closed
    # iterator terminates after close
    with_raised = False
    try:
        await asyncio.wait_for(handle.__anext__(), 1)
    except (StopAsyncIteration, asyncio.TimeoutError):
        with_raised = True
    assert with_raised
