"""Read-only LIVE validation of the Groww adapter (plan §12 Phase 2/3 exit).

Run via:  GROWW_API_KEY=… GROWW_API_SECRET=… python3 tools/validate_groww_live.py

Does ONLY safe reads: token, instrument master, LTP batches, historical 1-min
candles, positions. NEVER places/modifies/cancels orders.
Writes small public-metadata fixtures (instrument sample + candles + report)
under tests/fixtures/ for offline regression. No credentials are stored.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scapler.brokers.groww.master import parse_master          # noqa: E402
from scapler.brokers.groww.rest import GrowwCreds, GrowwRest   # noqa: E402
from scapler.core.messages import CandleClosed, OptionType     # noqa: E402
from scapler.strategy.indicators import IndicatorEngine        # noqa: E402
from tests._http import FakeHttp                               # noqa: E402,F401

FIX = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
TABLE = {"NIFTY": (50, 65), "BANKNIFTY": (100, 30), "SENSEX": (100, 20),
         "FINNIFTY": (50, 60), "MIDCPNIFTY": (25, 120)}
INDEX_SYM = {"NIFTY": ("NSE", "NIFTY"), "BANKNIFTY": ("NSE", "BANKNIFTY"),
             "SENSEX": ("BSE", "SENSEX"), "FINNIFTY": ("NSE", "FINNIFTY"),
             "MIDCPNIFTY": ("NSE", "MIDCPNIFTY")}


def reference(cs):
    """Textbook loop (independent of the engine) for parity reporting."""
    out = []
    vpv = vv = 0.0
    e9 = e21 = None
    prev = None
    gs, ls = [], []
    ag = al = 0.0
    trs = []
    atr = 0.0
    vols = []
    for c in cs:
        v = c.volume or 1.0
        tp = (c.h + c.l + c.c) / 3
        vpv += tp * v
        vv += v
        vwap = vpv / vv
        if e9 is None:
            e9 = e21 = c.c
        else:
            e9 += (c.c - e9) * 2 / 10
            e21 += (c.c - e21) * 2 / 22
        rsi = 50.0
        if prev is not None:
            ch = c.c - prev
            g, l = max(ch, 0.0), max(-ch, 0.0)
            if len(gs) < 14:
                gs.append(g); ls.append(l)
                if len(gs) == 14:
                    ag, al = sum(gs) / 14, sum(ls) / 14
            else:
                ag = (ag * 13 + g) / 14
                al = (al * 13 + l) / 14
            if len(gs) == 14:
                rsi = (100.0 if ag > 0 else 50.0) if al == 0 else \
                    100.0 - 100.0 / (1 + ag / al)
            tr = max(c.h - c.l, abs(c.h - prev), abs(c.l - prev))
        else:
            tr = c.h - c.l
        if len(trs) < 14:
            trs.append(tr)
            atr = sum(trs) / 14 if len(trs) == 14 else 0.0
        else:
            atr = (atr * 13 + tr) / 14
        ratio = (v / (sum(vols) / len(vols))) if vols else 0.0
        vols.append(v)
        if len(vols) > 21:
            vols = vols[-21:]
        prev = c.c
        out.append((vwap, e9, e21, rsi, atr, ratio))
    return out


async def main() -> int:
    key = os.environ["GROWW_API_KEY"]
    secret = os.environ["GROWW_API_SECRET"]
    rest = GrowwRest(GrowwCreds(api_key=key, api_secret=secret))
    today = date.today().isoformat()
    report: dict = {"date": today, "broker": "groww", "read_only": True}

    print("== token")
    tok = await rest.get_access_token()
    print(f"   access_token acquired: {tok[:8]}…{tok[-4:]}")

    print("== instrument master")
    body = await rest.master_bytes()
    master = parse_master(body, today)
    print(f"   parsed {len(master.options)} live index-option contracts, "
          f"{len(master.indices)} indices")
    rows = []
    for idx, (step, lot) in TABLE.items():
        exp = master.nearest_expiry(idx, today)
        mlot = master.lot(idx, exp) if exp else None
        nstr = len(master.strikes(idx, exp)) if exp else 0
        flag = "OK " if mlot == lot else "WARN"
        print(f"   {flag} {idx:<10} nearest expiry {exp}  lot master={mlot} "
              f"table={lot}  strikes={nstr}  step(table)={step}")
        rows.append({"index": idx, "expiry": exp, "lot_master": mlot,
                     "lot_table": lot, "strikes": nstr})
    report["master"] = rows

    print("== index LTPs (header strip)")
    syms = [f"{ex}_{sym}" for ex, sym in INDEX_SYM.values()]
    ltps = await rest.ltp_batch("CASH", syms)
    for s, p in ltps.items():
        print(f"   {s:<14} {p:>12,.2f}")
    report["index_ltps"] = ltps

    print("== 11-strike window LTPs (BANKNIFTY)")
    exp = master.nearest_expiry("BANKNIFTY", today)
    spot = ltps.get("NSE_BANKNIFTY", 0.0)
    step = TABLE["BANKNIFTY"][0]
    atm = round(spot / step) * step
    from scapler.strategy.strikes import build_window
    w = build_window(spot, step)
    keys = []
    for st in w.strikes:
        for opt in ("CE", "PE"):
            m = master.meta("BANKNIFTY", exp, st, OptionType[opt])
            if m:
                keys.append(m.instrument.symbol)
    wl = await rest.ltp_batch("FNO", keys)
    print(f"   spot {spot:,.2f} → ATM {atm:.0f}; subscribed {len(keys)} contracts")
    for k in keys[:6]:
        print(f"   {k:<26} {wl.get(k, float('nan')):>10.2f}")
    report["window"] = {"spot": spot, "atm": atm, "expiry": exp,
                        "contracts": len(keys)}

    print("== historical 1-min candles (BANKNIFTY index, today)")
    candles = await rest.candles("NSE", "CASH", "BANKNIFTY",
                                 f"{today} 09:15:00", f"{today} 15:30:00", 1)
    print(f"   received {len(candles)} candles")
    cs = [CandleClosed(key="NSE_INDEX|BANKNIFTY", tf="1m", open_ts=c[0],
                       close_ts=c[0] + 60, o=c[1], h=c[2], l=c[3], c=c[4],
                       volume=float(c[5])) for c in candles]
    eng = IndicatorEngine("NSE_INDEX|BANKNIFTY")
    got = [eng.on_candle(c) for c in cs]
    ref = reference(cs)
    maxdiff = 0.0
    for g, r in zip(got, ref):
        for a, b in zip((g.vwap, g.ema9, g.ema21, g.rsi, g.atr, g.vol_ratio), r):
            maxdiff = max(maxdiff, abs(a - b))
    print(f"   engine vs reference max abs diff: {maxdiff:.3e}")
    if cs:
        last = got[-1]
        print(f"   last closed candle indicators: vwap={last.vwap:,.2f} "
              f"ema9={last.ema9:,.2f} ema21={last.ema21:,.2f} "
              f"rsi={last.rsi:.1f} atr={last.atr:.2f} volx={last.vol_ratio:.2f}")
    report["candles"] = {"count": len(cs), "engine_vs_ref_max_diff": maxdiff}

    print("== positions (read-only)")
    pos = await rest.positions()
    print(f"   open FNO positions: {len(pos)}")
    report["positions"] = len(pos)

    # fixtures (public metadata only)
    FIX.mkdir(parents=True, exist_ok=True)
    (FIX / "groww_banknifty_1m_latest.json").write_text(
        json.dumps(candles[:400]))
    sample = ["exchange,exchange_token,trading_symbol,groww_symbol,name,"
              "instrument_type,segment,series,isin,underlying_symbol,"
              "underlying_exchange_token,expiry_date,strike_price,lot_size,"
              "tick_size,freeze_quantity,is_reserved,buy_allowed,sell_allowed"]
    for idx, (ex, sym) in INDEX_SYM.items():
        fk = master.indices.get(idx)
        if fk:
            tok_i = fk.split("|", 1)[1]
            sample.append(f"{ex},{tok_i},{sym},x,NaN,INDEX,CASH,NaN,NaN,,,"
                          f"0,0,0,0.05,0,0,1,1")
    for st in w.strikes:
        for opt in ("CE", "PE"):
            m = master.meta("BANKNIFTY", exp, st, OptionType[opt])
            if m:
                ik = m.instrument
                sample.append(f"{ik.exchange.split('_')[0]},{ik.token},"
                              f"{ik.symbol},x,NaN,{opt},FNO,NaN,NaN,BANKNIFTY,"
                              f"0,{ik.expiry},{ik.strike:.0f},{m.lot_size},"
                              f"0.05,0,0,1,1")
    (FIX / "groww_master_sample.csv").write_text("\n".join(sample) + "\n")
    (FIX / "groww_live_report.json").write_text(json.dumps(report, indent=2))
    print("== fixtures written:", ", ".join(
        p.name for p in FIX.glob("groww_*")))
    print("VALIDATION COMPLETE (read-only)")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
