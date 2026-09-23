"""Phase 7: RE-ARM command + shadow toggle through the runtime."""
import asyncio
import json

from scapler.core.config import Settings
from scapler.ui.runtime import ScaplerRuntime


async def _runtime(tmp_path):
    cfg = Settings(data_dir=str(tmp_path / "data"))
    rt = ScaplerRuntime(settings=cfg, demo=True,
                        settings_path=tmp_path / "settings.json")
    rt.build(hz=50)
    await rt.start()
    await asyncio.sleep(0.4)
    return rt


async def test_snapshot_and_order_edge_default_to_shadow(tmp_path):
    rt = await _runtime(tmp_path)
    snap = rt.ui.snapshot()
    assert snap["shadow"] is True                 # default ON until go-live
    assert snap["settings"]["shadow"] is True
    assert rt.oa.shadow is True and rt.jr.shadow is True
    await rt.stop()


async def test_shadow_toggle_flips_edge_and_persists(tmp_path):
    rt = await _runtime(tmp_path)
    r = await rt.ui.handle_command(
        "save_settings", {"changes": {"shadow": False}})
    assert r["ok"] is True and "SHADOW OFF" in r["note"]
    assert rt.oa.shadow is False and rt.jr.shadow is False
    assert rt.cfg.shadow is False
    saved = json.loads((tmp_path / "settings.json").read_text())
    assert saved["shadow"] is False               # persists across restarts
    assert rt.ui.snapshot()["shadow"] is False

    r = await rt.ui.handle_command(
        "save_settings", {"changes": {"shadow": True}})
    assert "SHADOW ON" in r["note"] and rt.oa.shadow is True
    await rt.stop()


async def test_rearm_clears_kill_when_flat(tmp_path):
    rt = await _runtime(tmp_path)
    rt.rk.killed = rt.wd.killed = rt.sup.killed = rt.ui.killed = True
    r = await rt.ui.handle_command("rearm", {})
    assert r["ok"] is True
    assert not (rt.rk.killed or rt.wd.killed or rt.sup.killed
                or rt.ui.killed)
    await rt.stop()


async def test_rearm_refused_with_open_position(tmp_path):
    rt = await _runtime(tmp_path)
    rt.rk.killed = True
    rt.px.trackers["NSE_FO|N25000825000C"] = object()   # pretend a long
    r = await rt.ui.handle_command("rearm", {})
    assert r["ok"] is False and "position open" in r["error"]
    assert rt.rk.killed                           # still halted
    await rt.stop()


def test_desktop_runtime_loads_settings_from_disk(tmp_path):
    from scapler.core.config import Settings, save
    from scapler.ui.runtime import desktop_runtime
    p = tmp_path / "settings.json"
    save(Settings(shadow=False, broker_active="groww", index="SENSEX"), p)
    rt, path = desktop_runtime(settings_path=p)
    assert rt.cfg.shadow is False and rt.cfg.index == "SENSEX"
    assert rt.cfg.broker_active == "groww"
    assert rt.settings_path == path == p
    assert rt.demo is False
    assert getattr(rt, "oa", None) is None         # agents built by run_webview


def test_desktop_runtime_default_path_is_data_dir():
    from scapler.ui.runtime import desktop_runtime
    rt, path = desktop_runtime()
    assert path.name == "settings.json" and ".scapler" in str(path)
    assert rt.settings_path == path
