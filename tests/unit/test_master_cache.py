"""Master cache: memory → disk → network precedence + JSON round-trip."""
import asyncio
import json

from scapler.brokers import master_cache as mc
from scapler.brokers.base import InstrumentMeta, MasterTable
from scapler.core.messages import InstrumentKey, OptionType as OT
from tests._master import make_master


class FakeAdapter:
    name = "fake"

    def __init__(self, master=None, fail=False):
        self.master = master
        self.fail = fail
        self.net_calls = 0

    async def load_master(self, today=None):
        self.net_calls += 1
        if self.fail:
            raise RuntimeError("network down")
        self.master = make_master()
        return self.master


async def test_roundtrip_preserves_everything(tmp_path):
    m = make_master()
    p = mc.save_disk(m, tmp_path, "groww", "2026-09-23")
    assert p.exists()
    back = mc.load_disk(tmp_path, "groww", "2026-09-23")
    assert back is not None
    assert set(back.options) == set(m.options)
    assert back.indices == m.indices
    meta = back.meta("BANKNIFTY", "2026-09-29", 51200.0, OT.CE)
    assert meta is not None and meta.lot_size == 30
    assert meta.instrument.option_type is OT.CE
    assert meta.instrument.strike == 51200.0


async def test_memory_first_then_disk_then_network(tmp_path):
    # 1) memory: adapter.master set → no disk read, no network
    mem = make_master(center=51300.0)
    a = FakeAdapter(master=mem)
    assert await mc.get_master(a, tmp_path) is mem
    assert a.net_calls == 0
    # 2) disk: same-day file exists → network untouched
    mc.save_disk(make_master(), tmp_path, "fake", "2026-09-23")
    b = FakeAdapter()
    got = await mc.get_master(b, tmp_path, "2026-09-23")
    assert got is not None and b.net_calls == 0 and b.master is got
    # 3) network: nothing cached → fetch + persist
    c = FakeAdapter()
    got = await mc.get_master(c, tmp_path, "2026-09-24")
    assert c.net_calls == 1 and got is not None
    assert mc.cache_path(tmp_path, "fake", "2026-09-24").exists()


async def test_corrupt_disk_cache_refetches(tmp_path):
    p = mc.cache_path(tmp_path, "fake", "2026-09-23")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{corrupt", encoding="utf-8")
    a = FakeAdapter()
    got = await mc.get_master(a, tmp_path, "2026-09-23")
    assert a.net_calls == 1 and got is not None
    # the refetched master overwrote the corrupt file
    assert json.loads(p.read_text())["indices"]


async def test_stale_day_files_pruned(tmp_path):
    for day in range(20, 26):
        mc.save_disk(make_master(), tmp_path, "fake", f"2026-09-{day}")
    left = sorted(p.name for p in tmp_path.glob("master-fake-*.json"))
    assert len(left) == 3 and left[-1] == "master-fake-2026-09-25.json"
