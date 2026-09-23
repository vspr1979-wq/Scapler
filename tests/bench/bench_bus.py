"""Phase-0 exit gate (plan.md §12 Phase 0):
bus throughput ≥ 100k msg/s and publish→consume p99 < 1 ms;
coalesced burst keeps inbox depth bounded.

Run:  python3 tests/bench/bench_bus.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scapler.core.agent import Agent            # noqa: E402
from scapler.core.bus import EventBus           # noqa: E402
from scapler.core.messages import Tick, Topic   # noqa: E402

N = 200_000
# Producer yields every CHUNK msgs so the consumer interleaves; queue-wait is
# bounded by CHUNK publish-calls, keeping p99 a steady-state number, not a
# batching artifact.
CHUNK = 100


class Sink(Agent):
    name = "sink"
    topics = ("bench.tick",)

    def __init__(self, bus):
        super().__init__(bus)
        self.deltas: list[int] = []
        self.done = asyncio.Event()

    async def on_message(self, env):
        self.deltas.append(time.monotonic_ns() - env.ts_mono)
        if len(self.deltas) >= N:
            self.done.set()


class SlowSink(Agent):
    name = "slow"
    topics = (Topic.TICK_RAW,)

    def __init__(self, bus):
        super().__init__(bus)
        self.n = 0
        self.max_depth = 0

    async def on_message(self, env):
        self.n += 1
        self.max_depth = max(self.max_depth, self.inbox.depth())
        await asyncio.sleep(0.001)   # deliberately slow consumer


async def throughput_bench() -> tuple[float, float, float]:
    bus = EventBus()
    sink = Sink(bus)
    await sink.start()
    t0 = time.monotonic_ns()
    for i in range(N):
        bus.publish("bench.tick", Tick(key="K", exch_ts_ns=i, ltp=float(i)))
        if i % CHUNK == CHUNK - 1:
            await asyncio.sleep(0)          # let the consumer interleave
    await asyncio.wait_for(sink.done.wait(), 30)
    dt_s = (time.monotonic_ns() - t0) / 1e9
    d = sorted(sink.deltas)
    p50 = d[len(d) // 2] / 1e6
    p99 = d[int(len(d) * 0.99)] / 1e6
    await sink.stop()
    return N / dt_s, p50, p99


async def coalescing_bench() -> tuple[int, int]:
    bus = EventBus()
    slow = SlowSink(bus)
    await slow.start()
    for i in range(50_000):
        bus.publish(Topic.TICK_RAW, Tick(key="K", exch_ts_ns=i, ltp=float(i)), key="K")
    await asyncio.sleep(0.05)
    depth = slow.inbox.depth()
    await slow.stop()
    return slow.n, depth


async def main() -> int:
    thr, p50, p99 = await throughput_bench()
    n, depth = await coalescing_bench()
    print(f"throughput      : {thr:,.0f} msg/s   (gate ≥ 100,000)")
    print(f"hop latency     : p50 {p50:.3f} ms · p99 {p99:.3f} ms   (gate p99 < 1.0 ms)")
    print(f"coalesced burst : 50,000 ticks → {n} delivered, inbox depth now {depth} (bounded)")
    ok = thr >= 100_000 and p99 < 1.0 and depth <= 2
    print("PHASE-0 BUS BENCH:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
