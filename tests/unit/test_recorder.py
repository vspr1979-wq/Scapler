"""Recorder / replayer round-trip (real ticks only, labelled REPLAY)."""
import asyncio

from scapler.core.messages import Tick
from scapler.core.recorder import TickRecorder, replay


async def test_roundtrip(tmp_path):
    p = tmp_path / "session.jsonl"
    rec = TickRecorder(p)
    ticks = [Tick(key="NSE_FO|1", exch_ts_ns=i * 10**6, ltp=100.0 + i,
                  bid=99.5, ask=100.5, oi=10.0, volume=5.0,
                  delta=0.5, gamma=0.004) for i in range(50)]
    for t in ticks:
        rec.write(t, "2026-09-22T10:31:00")
    rec.close()
    assert rec.n == 50

    out = [t async for t in replay(p)]
    assert len(out) == 50
    assert out[0] == ticks[0] and out[-1] == ticks[-1]


async def test_replay_pacing(tmp_path):
    p = tmp_path / "s2.jsonl"
    rec = TickRecorder(p)
    rec.write(Tick(key="K", exch_ts_ns=0, ltp=1.0), "t")
    rec.write(Tick(key="K", exch_ts_ns=120 * 10**6, ltp=2.0), "t")
    rec.close()
    import time
    t0 = time.monotonic()
    n = 0
    async for _ in replay(p, rate=1.0):
        n += 1
    dt = time.monotonic() - t0
    assert n == 2 and dt >= 0.10            # 120 ms gap honoured (±)
