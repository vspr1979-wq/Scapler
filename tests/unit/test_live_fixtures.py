"""Offline regression on LIVE-recorded fixtures.

Fixtures are produced on a machine that can reach Groww by:
    GROWW_API_KEY=… GROWW_API_SECRET=… bash tools/run_groww_validation.sh
(read-only: token, master, LTPs, historical candles, positions).
Until then these tests SKIP — they are not fake-data substitutes.
"""
import json
from pathlib import Path

import pytest

from scapler.brokers.groww.master import parse_master
from scapler.core.messages import CandleClosed
from scapler.strategy.indicators import IndicatorEngine
from tests.unit.test_indicators import reference

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
CSV = FIX / "groww_master_sample.csv"
REP = FIX / "groww_live_report.json"
CAND = FIX / "groww_banknifty_1m_latest.json"

needs_fix = pytest.mark.skipif(
    not (CSV.exists() and REP.exists()),
    reason="run tools/run_groww_validation.sh on a machine with Groww access")


@needs_fix
def test_live_master_sample_parses():
    report = json.loads(REP.read_text())
    m = parse_master(CSV.read_bytes(), report["date"])
    for row in report["master"]:
        idx = row["index"]
        if row["expiry"]:
            assert m.nearest_expiry(idx, report["date"]) == row["expiry"]
            assert m.lot(idx, row["expiry"]) == row["lot_master"]
    assert "BANKNIFTY" in m.indices


@needs_fix
@pytest.mark.skipif(not CAND.exists(), reason="candle fixture missing")
def test_live_candles_engine_matches_reference():
    raw = json.loads(CAND.read_text())
    cs = [CandleClosed(key="NSE_INDEX|BANKNIFTY", tf="1m", open_ts=c[0],
                       close_ts=c[0] + 60, o=c[1], h=c[2], l=c[3], c=c[4],
                       volume=float(c[5])) for c in raw]
    eng = IndicatorEngine("NSE_INDEX|BANKNIFTY")
    for c, r in zip(cs, reference(cs)):
        g = eng.on_candle(c)
        for a, b in zip((g.vwap, g.ema9, g.ema21, g.rsi, g.atr, g.vol_ratio), r):
            assert a == pytest.approx(b, rel=1e-9, abs=1e-12)
