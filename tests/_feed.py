"""Scripted feed handle + fake adapter for agent-chain tests."""
from __future__ import annotations


class ScriptHandle:
    def __init__(self, ticks):
        self._ticks = list(ticks)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        import asyncio
        await asyncio.sleep(0)          # let the loop interleave
        if not self._ticks:
            raise StopAsyncIteration
        return self._ticks.pop(0)

    async def close(self):
        self.closed = True


class FakeAdapter:
    name = "fake"

    def __init__(self, ticks):
        self.ticks = ticks
        self.handle = None

    async def open_feed(self, keys, mode="full"):
        self.handle = ScriptHandle(self.ticks)
        return self.handle

    async def disconnect(self):
        pass
