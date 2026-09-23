"""User directives (Phase 6): NIFTY default; instant index switch from the
dropdown using memory+disk cache — no restart, persisted to disk."""
import asyncio
import json

from scapler.core.config import Settings, load
from scapler.ui.runtime import ScaplerRuntime


async def _runtime(tmp_path):
    cfg = Settings(data_dir=str(tmp_path / "data"))
    rt = ScaplerRuntime(settings=cfg, demo=True,
                        settings_path=tmp_path / "settings.json")
    rt.build(hz=50)
    await rt.start()
    await asyncio.sleep(0.4)          # let the demo feed run
    return rt


async def test_nifty_is_default_and_order_is_nifty_sensex_banknifty(tmp_path):
    rt = await _runtime(tmp_path)
    assert rt.index == "NIFTY" and rt.cfg.index == "NIFTY"
    snap = rt.ui.snapshot()
    assert snap["controls"]["indices"] == [
        "NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
    assert snap["controls"]["index"] == "NIFTY"
    assert snap["controls"]["lot"] == 65           # NIFTY lot from master
    assert rt.spot_key == "NSE_INDEX|NIFTY"
    # header strip always carries the three flagship LTP keys
    assert set(snap["header"]["ltps"]) >= {"NIFTY", "BANKNIFTY", "SENSEX"}
    await rt.stop()


async def test_instant_switch_updates_everything_and_persists(tmp_path):
    rt = await _runtime(tmp_path)
    r = await rt.ui.handle_command("set_index", {"index": "SENSEX"})
    assert r["ok"] is True, r
    await asyncio.sleep(0.5)                       # feed reconnect + ticks
    # every key-bound agent flipped instantly
    assert rt.index == "SENSEX" and rt.ui.index == "SENSEX"
    assert rt.ui.spot_key == "BSE_INDEX|SENSEX"
    assert rt.cb.key == "BSE_INDEX|SENSEX"
    assert rt.sa.index == "SENSEX" and rt.st.index == "SENSEX"
    assert rt.md.keys[0] == "BSE_INDEX|SENSEX"
    assert rt.md.reconnects >= 1                   # feed re-opened live
    snap = rt.ui.snapshot()
    assert snap["controls"]["index"] == "SENSEX"
    assert snap["controls"]["lot"] == 20           # SENSEX lot 20 (BSE)
    assert snap["controls"]["step"] == 100.0
    assert rt.cfg.index == "SENSEX"
    # choice persisted to disk (settings.json)
    on_disk = json.loads((tmp_path / "settings.json").read_text())
    assert on_disk["index"] == "SENSEX"
    assert load(tmp_path / "settings.json").index == "SENSEX"
    # the SENSEX window builds from the streamed BSE spot — no restart
    for _ in range(40):
        await asyncio.sleep(0.1)
        if rt.ui.window is not None:
            break
    assert rt.ui.window is not None
    # demo SENSEX spot oscillates around 80100 → ATM is 80000/80100/80200
    assert abs(rt.ui.window["center"] - 80100.0) <= 200
    # switch back to BANKNIFTY, third priority
    r = await rt.ui.handle_command("set_index", {"index": "BANKNIFTY"})
    assert r["ok"] is True
    assert rt.ui.index == "BANKNIFTY" and rt.ui.lot == 30
    await rt.stop()


async def test_switch_refused_while_position_open(tmp_path):
    rt = await _runtime(tmp_path)
    class _FakeTracker:
        pass
    rt.px.trackers["NSE_FO|N24500C"] = _FakeTracker()
    r = await rt.ui.handle_command("set_index", {"index": "SENSEX"})
    assert r["ok"] is False and "close the open position" in r["error"]
    assert rt.index == "NIFTY"                     # unchanged
    del rt.px.trackers["NSE_FO|N24500C"]
    r = await rt.ui.handle_command("set_index", {"index": "SENSEX"})
    assert r["ok"] is True
    await rt.stop()


async def test_unknown_index_rejected(tmp_path):
    rt = await _runtime(tmp_path)
    r = await rt.ui.handle_command("set_index", {"index": "DOGECOIN"})
    assert r["ok"] is False and "not in master" in r["error"]
    await rt.stop()
