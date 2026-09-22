"""Indicator engine vs an independently written textbook reference +
pandas cross-check for EMA/VWAP (rel tol 1e-9)."""
import math

import pytest

from scapler.core.messages import CandleClosed
from scapler.strategy.indicators import IndicatorEngine

pandas = pytest.importorskip("pandas")


def candles(series_vol):
    out = []
    ts = 0
    for (o, h, l, c, v) in series_vol:
        ts += 60
        out.append(CandleClosed(key="K", tf="1m", open_ts=ts - 60, close_ts=ts,
                                o=o, h=h, l=l, c=c, volume=v))
    return out


def synthetic(n=120, seed=7):
    import random
    rng = random.Random(seed)
    px = 100.0
    rows = []
    for i in range(n):
        o = px
        c = px + rng.uniform(-0.8, 0.9)
        h = max(o, c) + rng.random() * 0.4
        l = min(o, c) - rng.random() * 0.4
        v = rng.randint(60, 400)
        rows.append((o, h, l, c, v))
        px = c
    return rows


def reference(cs):
    """Plain textbook loop, written independently of the engine."""
    res = []
    vwap_pv = vwap_v = 0.0
    ema9 = ema21 = None
    prev = None
    gains, losses = [], []
    avg_g = avg_l = 0.0
    trs = []
    atr = 0.0
    vols = []
    for c in cs:
        v = c.volume if c.volume > 0 else 1.0
        tp = (c.h + c.l + c.c) / 3.0
        vwap_pv += tp * v
        vwap_v += v
        vwap = vwap_pv / vwap_v
        if ema9 is None:
            ema9 = ema21 = c.c
        else:
            ema9 = ema9 + (c.c - ema9) * (2 / 10)
            ema21 = ema21 + (c.c - ema21) * (2 / 22)
        rsi = 50.0
        if prev is not None:
            ch = c.c - prev
            g, l = (ch if ch > 0 else 0.0), (-ch if ch < 0 else 0.0)
            if len(gains) < 14:
                gains.append(g)
                losses.append(l)
                if len(gains) == 14:
                    avg_g = sum(gains) / 14
                    avg_l = sum(losses) / 14
            else:
                avg_g = (avg_g * 13 + g) / 14
                avg_l = (avg_l * 13 + l) / 14
            if len(gains) == 14:
                rsi = (100.0 if avg_g > 0 else 50.0) if avg_l == 0 else \
                      100.0 - 100.0 / (1.0 + avg_g / avg_l)
            tr = max(c.h - c.l, abs(c.h - prev), abs(c.l - prev))
        else:
            tr = c.h - c.l
        if len(trs) < 14:
            trs.append(tr)
            if len(trs) == 14:
                atr = sum(trs) / 14
                atr_out = atr
            else:
                atr_out = 0.0
        else:
            atr = (atr * 13 + tr) / 14
            atr_out = atr
        ratio = (v / (sum(vols) / len(vols))) if vols else 0.0
        vols.append(v)
        if len(vols) > 21:
            vols = vols[-21:]
        prev = c.c
        res.append((vwap, ema9, ema21, rsi, atr_out, ratio))
    return res


def test_engine_matches_reference_1e9():
    cs = candles(synthetic())
    eng = IndicatorEngine("K")
    ref = reference(cs)
    for c, r in zip(cs, ref):
        got = eng.on_candle(c)
        for a, b in zip((got.vwap, got.ema9, got.ema21, got.rsi, got.atr,
                         got.vol_ratio), r):
            assert a == pytest.approx(b, rel=1e-9, abs=1e-12)


def test_ema_vwap_match_pandas():
    rows = synthetic(200)
    cs = candles(rows)
    eng = IndicatorEngine("K")
    gots = [eng.on_candle(c) for c in cs]
    df = pandas.DataFrame(rows, columns=["o", "h", "l", "c", "v"])
    ema9 = df.c.ewm(span=9, adjust=False).mean()
    ema21 = df.c.ewm(span=21, adjust=False).mean()
    tp = (df.h + df.l + df.c) / 3.0
    vwap = (tp * df.v).cumsum() / df.v.cumsum()
    for i, g in enumerate(gots):
        assert g.ema9 == pytest.approx(ema9.iloc[i], rel=1e-9)
        assert g.ema21 == pytest.approx(ema21.iloc[i], rel=1e-9)
        assert g.vwap == pytest.approx(vwap.iloc[i], rel=1e-9)


def test_warmup_is_neutral_not_invented():
    cs = candles(synthetic(10))
    eng = IndicatorEngine("K")
    first = eng.on_candle(cs[0])
    assert first.rsi == 50.0 and first.atr == 0.0 and first.vol_ratio == 0.0
    assert not eng.warmed_up
    for c in cs[1:]:
        eng.on_candle(c)
    assert not eng.warmed_up            # 10 candles < 15
    eng2 = IndicatorEngine("K")
    for c in candles(synthetic(30)):
        r = eng2.on_candle(c)
    assert eng2.warmed_up
    assert 0.0 <= r.rsi <= 100.0 and r.atr > 0.0


def test_session_reset():
    eng = IndicatorEngine("K")
    for c in candles(synthetic(40)):
        eng.on_candle(c)
    eng.reset_session()
    assert eng.n == 0 and eng.rsi == 50.0 and eng.atr == 0.0
    assert math.isfinite(eng.on_candle(candles(synthetic(1))[0]).vwap)
