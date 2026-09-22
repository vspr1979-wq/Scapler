"""Phase 7 tools: shadow_report.summarize + go_live_check.run_checks."""
from __future__ import annotations

import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

from go_live_check import (FAIL, PASS, SKIP, WARN,             # noqa: E402
                           run_checks)
from shadow_report import summarize                           # noqa: E402

from scapler.agents.journal import DDL                        # noqa: E402
from scapler.core.config import Settings, save                # noqa: E402
from scapler.core.secrets_store import SecretStore            # noqa: E402
from scapler.brokers.master_cache import save_disk            # noqa: E402
from tests._master import make_master                         # noqa: E402



def _seed_db(path: Path, sessions: int, dirty_last: bool) -> Path:
    db = sqlite3.connect(str(path))
    db.executescript(DDL)
    t0 = date(2026, 9, 21)
    for i in range(sessions):
        start = f"{(t0 + timedelta(days=i)).isoformat()}T04:00:00+00:00"
        end = f"{(t0 + timedelta(days=i)).isoformat()}T10:30:00+00:00"
        db.execute("INSERT INTO sessions(started_at, ended_at, broker, demo,"
                   " mode) VALUES(?,?,?,?,?)",
                   (start, end, "groww", 0, "AUTO/SHADOW"))
        seq = i * 100
        db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)",
                   (seq, start, "signal.new", "signal", "BULL CE", i + 1))
        db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)",
                   (seq + 1, start, "order.fill", "order", "BUY 65 @100.5",
                    i + 1))
        db.execute("INSERT INTO fills VALUES(?,?,?,?,?,?,?,?)",
                   (seq, start, f"cid{i}", "BUY_TO_OPEN",
                    "NIFTY25000825000CE", 65, 100.5, 3.0 + i))
        db.execute("INSERT INTO positions VALUES(?,?,?,?,?,?,?,?)",
                   (seq, start, "NSE_FO|N25000825000C", 0, 100.5,
                    1, "T1", 325.0))
        if dirty_last and i == sessions - 1:
            db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)",
                       (seq + 2, start, "order.rejected", "order",
                        "REJECTED cidX: SG_NO_LONG_POSITION", i + 1))
    db.commit()
    db.close()
    return path


def test_summarize_counts_sessions_and_cleanliness(tmp_path):
    dbp = _seed_db(tmp_path / "j.sqlite", 3, dirty_last=False)
    rep = summarize(dbp)
    assert rep["clean_sessions"] == 3
    assert rep["go_live_ready"] is True
    assert rep["pnl_by_reason"]["T1"] == {"n": 3, "pnl": 975.0}
    assert rep["fill_latency_ms"]["p50"] >= 3.0

    dbp2 = _seed_db(tmp_path / "dirty.sqlite", 3, dirty_last=True)
    rep2 = summarize(dbp2)
    assert rep2["clean_sessions"] == 2
    assert rep2["go_live_ready"] is False
    assert rep2["sessions"][-1]["sideguard_rejects"] == 1


def test_run_checks_fails_on_empty_dir(tmp_path):
    res = run_checks(data_dir=tmp_path)
    d = {item: st for st, item, _ in res}
    assert d["broker secrets"] == FAIL
    assert d["clean shadow sessions"] == FAIL
    assert any(st == SKIP for st, _, _ in res)      # manual items listed


def test_run_checks_passes_on_ready_dir(tmp_path):
    save(Settings(broker_active="groww"), tmp_path / "settings.json")
    SecretStore(tmp_path).save("groww", {"api_key": "K", "api_secret": "S"})
    save_disk(make_master(index="NIFTY", lot=65, center=25000.0, step=50.0,
                          exch="NSE_FO"), tmp_path, "groww")
    _seed_db(tmp_path / "journal.sqlite", 3, dirty_last=False)
    res = run_checks(data_dir=tmp_path)
    d = {item: st for st, item, _ in res}
    assert d["settings.json"] == PASS
    assert d["risk breakers"] == PASS
    assert d["broker secrets"] == PASS
    assert d["instrument master"] == PASS           # NIFTY matches table
    assert d["journal sessions"] == PASS
    assert d["clean shadow sessions"] == PASS
    assert not [v for v in d.values() if v == FAIL]


def test_run_checks_warns_on_lot_mismatch(tmp_path):
    save(Settings(broker_active="groww"), tmp_path / "settings.json")
    SecretStore(tmp_path).save("groww", {"api_key": "K", "api_secret": "S"})
    # BANKNIFTY master says lot 35 (live Groww CSV sample) vs table 30
    save_disk(make_master(index="BANKNIFTY", lot=35, center=51200.0,
                          step=100.0), tmp_path, "groww")
    _seed_db(tmp_path / "journal.sqlite", 3, dirty_last=False)
    res = run_checks(data_dir=tmp_path)
    d = {item: st for st, item, _ in res}
    assert d["instrument master"] == WARN
    det = [dt for st, it, dt in res if it == "instrument master"][0]
    assert "lot master=35 vs table=30" in det
