"""Market Data Streamer v3 client: authorize → wss → subscribe → Ticks.

Known SDK pitfall handled (plan §4.2): the authorized redirect URI is fetched
via REST first and the socket is opened directly against it, avoiding the
307-redirect handshake problem. Subscribe payload is JSON-as-bytes:
{"guid":…, "method":"sub", "data":{"mode":…, "instrumentKeys":[…]}}.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import AsyncIterator

import orjson

from ...core.messages import Tick
from . import proto_decode
from .rest import UpstoxRest


async def _default_ws_factory(url: str):
    import websockets
    return await websockets.connect(url, max_size=2 ** 22, ping_interval=20)


class _Handle:
    def __init__(self, ws, loop: asyncio.AbstractEventLoop) -> None:
        self._ws = ws
        self._q: asyncio.Queue[Tick | None] = asyncio.Queue(maxsize=50_000)
        self._task = loop.create_task(self._pump())
        self.frames_decoded = 0
        self.frames_bad = 0

    async def _pump(self) -> None:
        try:
            while True:
                frame = await self._ws.recv()
                if isinstance(frame, str):
                    frame = frame.encode()
                for key, f in proto_decode.decode_feed_response(frame):
                    self.frames_decoded += 1
                    if self._q.full():            # drop-oldest: never bloat
                        try:
                            self._q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    self._q.put_nowait(Tick(
                        key=key,
                        exch_ts_ns=int(f["ltt"]) * 1_000_000,
                        ltp=f["ltp"], bid=f["bid"], ask=f["ask"],
                        oi=f["oi"], volume=float(f["vtt"]),
                        delta=f["delta"], gamma=f["gamma"]))
        except asyncio.CancelledError:
            raise
        except Exception:
            pass                                 # socket dead → iterator ends
        finally:
            self._q.put_nowait(None)

    def __aiter__(self) -> AsyncIterator[Tick]:
        return self

    async def __anext__(self) -> Tick:
        item = await self._q.get()
        if item is None:
            raise StopAsyncIteration
        return item

    async def close(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            await self._ws.close()
        except Exception:
            pass


class UpstoxFeedV3:
    def __init__(self, rest: UpstoxRest, ws_factory=None) -> None:
        self._rest = rest
        self._ws_factory = ws_factory or _default_ws_factory

    async def open(self, keys: list[str], mode: str = "full") -> _Handle:
        url = await self._rest.authorize_feed_url()
        ws = await self._ws_factory(url)
        sub = orjson.dumps({"guid": str(uuid.uuid4()), "method": "sub",
                            "data": {"mode": mode, "instrumentKeys": list(keys)}})
        await ws.send(sub)
        return _Handle(ws, asyncio.get_running_loop())
