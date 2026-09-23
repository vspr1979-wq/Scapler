"""Shared FakeHttp for broker adapter tests."""
from __future__ import annotations

import orjson


class FakeHttp:
    """Routes matched by substring; bodies may be static or callable(method, url)."""

    def __init__(self, routes: dict):
        self.routes = routes
        self.calls: list[dict] = []

    async def request(self, method, url, *, headers=None, params=None,
                      json=None, data=None):
        self.calls.append({"method": method, "url": url, "headers": headers,
                           "params": params, "json": json, "data": data})
        for path, body in self.routes.items():
            if path in url:
                if callable(body):
                    status, payload = body(method, url)
                else:
                    status, payload = body
                return status, (payload if isinstance(payload, bytes)
                                else orjson.dumps(payload))
        return 404, orjson.dumps({"status": "FAILURE"})

    def last(self, substr: str) -> dict:
        for c in reversed(self.calls):
            if substr in c["url"]:
                return c
        raise AssertionError(f"no call matched {substr}")
