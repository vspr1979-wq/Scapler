"""Real-tick recorder / replayer (plan §0.3 replay note).

Records LIVE ticks to JSONL for offline regression tests. Replay is always
labelled REPLAY by the UI and can never arm the Signal agent in live mode.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import AsyncIterator

import orjson

from .messages import Tick


class TickRecorder:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("ab", buffering=1 << 16)
        self.n = 0

    def write(self, tick: Tick, ts_iso: str) -> None:
        self._fh.write(orjson.dumps({"ts": ts_iso, "k": tick.key,
                                     "t": tick.exch_ts_ns, "p": tick.ltp,
                                     "b": tick.bid, "a": tick.ask,
                                     "oi": tick.oi, "v": tick.volume,
                                     "d": tick.delta, "g": tick.gamma}) + b"\n")
        self.n += 1

    def close(self) -> None:
        self._fh.close()


async def replay(path: str | Path, rate: float = 0.0) -> AsyncIterator[Tick]:
    """rate=0 → as fast as possible; else real-time pacing by exch_ts."""
    prev_ts = None
    import asyncio
    with Path(path).open("rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if rate and prev_ts is not None:
                gap = (r["t"] - prev_ts) / 1e9 * rate
                if gap > 0:
                    await asyncio.sleep(gap)
            prev_ts = r["t"]
            yield Tick(key=r["k"], exch_ts_ns=r["t"], ltp=r["p"], bid=r["b"],
                       ask=r["a"], oi=r["oi"], volume=r["v"],
                       delta=r.get("d"), gamma=r.get("g"))
