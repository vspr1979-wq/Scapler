"""GO-LIVE CHECKLIST (plan §13) — automate what can be automated, list the rest.

Usage:  python tools/go_live_check.py [--settings ~/.scapler/settings.json]

Each item → PASS / WARN / FAIL / SKIP + detail. Overall verdict:
  GO       — no FAIL and ≥3 clean shadow sessions recorded
  NO-GO    — any FAIL
  MANUAL   — automated checks pass but human items still need sign-off.

Automated: secrets present, master cached today + lots/steps vs plan table
(master is source of truth; mismatch = WARN), journal DB + WAL + sessions,
shadow cleanliness via shadow_report, risk breakers configured, square-off
15:20, journal export works. Manual (printed): feed latency < 50 ms (status
bar during shadow), kill-switch drill, UPS/power plan, Windows sleep off,
1-lot first-live policy.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# plan §6 instrument table — used for SANITY only; master is source of truth
TABLE_LOTS = {"NIFTY": 65, "SENSEX": 20, "BANKNIFTY": 30, "FINNIFTY": 60,
              "MIDCPNIFTY": 120}
TABLE_STEPS = {"NIFTY": 50.0, "SENSEX": 100.0, "BANKNIFTY": 100.0,
               "FINNIFTY": 50.0, "MIDCPNIFTY": 25.0}

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"


def run_checks(settings_path: str | Path | None = None,
               data_dir: str | Path | None = None) -> list[tuple]:
    """Return [(status, item, detail)]."""
    out: list[tuple] = []
    data_dir = Path(data_dir or Path.home() / ".scapler").expanduser()
    settings_path = Path(settings_path or data_dir / "settings.json")

    # 1. settings
    try:
        cfg = json.loads(settings_path.read_text(encoding="utf-8")) \
            if settings_path.exists() else {}
    except Exception as e:  # noqa: BLE001
        out.append((FAIL, "settings.json", f"unreadable: {e}"))
        cfg = {}
    if cfg:
        out.append((PASS, "settings.json", f"loaded {len(cfg)} keys"))
        bad = []
        if cfg.get("max_trades_per_day") is None or \
                not (1 <= int(cfg.get("max_trades_per_day", 0)) <= 10):
            bad.append("max_trades_per_day (1..10)")
        if not float(cfg.get("max_daily_loss_inr", 0)) > 0:
            bad.append("max_daily_loss_inr>0")
        if int(cfg.get("sl_streak_stop", 0)) < 1:
            bad.append("sl_streak_stop>=1")
        if cfg.get("square_off") != "15:20":
            bad.append(f"square_off must be 15:20 (got {cfg.get('square_off')})")
        if tuple(cfg.get("entry_window", [])) != ("09:17", "15:15"):
            bad.append("entry_window 09:17–15:15")
        out.append((FAIL if bad else PASS, "risk breakers",
                    "; ".join(bad) if bad else "configured per plan"))
        out.append((PASS if cfg.get("broker_active") in
                    ("groww", "upstox") else WARN, "broker selected",
                    f"broker_active={cfg.get('broker_active')}"))
    else:
        out.append((WARN, "settings.json", "not created yet — defaults apply "
                    "until the operator saves once"))

    # 2. secrets for the active broker (never print values)
    try:
        from scapler.core.secrets_store import SecretStore
        active = cfg.get("broker_active") or "groww"
        sec = SecretStore(data_dir).load(active)
        if active == "upstox":
            ok = bool(sec and sec["api_key"] and sec["api_secret"])
        else:
            ok = bool(sec and sec["api_key"])
        out.append((PASS if ok else FAIL, "broker secrets",
                    f"{active}: {'present' if ok else 'MISSING — open Settings, save keys'}"))
    except Exception as e:  # noqa: BLE001
        out.append((FAIL, "broker secrets", f"SecretStore error: {e}"))

    # 3. master cache freshness + lot/step sanity vs plan table
    #    (master is the SOURCE OF TRUTH; the plan table is a sanity check)
    try:
        from scapler.brokers.master_cache import load_disk
        active_b = cfg.get("broker_active") or "groww"
        m = load_disk(data_dir, active_b)          # same-day file only
        if m is None:
            out.append((WARN, "instrument master",
                        "no cache dated today — SCAPLER refreshes at login"))
        else:
            mism = []
            by_ix: dict[str, list] = {}
            for meta in m.options.values():
                by_ix.setdefault(meta.index_symbol, []).append(meta)
            for ix, lot in TABLE_LOTS.items():
                metas = by_ix.get(ix)
                if not metas:
                    continue
                got_lot = metas[0].lot_size
                if got_lot != lot:
                    mism.append(f"{ix} lot master={got_lot} vs table={lot}")
                strikes = sorted({mt.instrument.strike for mt in metas})
                steps = {round(b - a, 2) for a, b in zip(strikes, strikes[1:])}
                step = min(steps) if steps else 0.0
                if abs(step - TABLE_STEPS[ix]) > 1e-9:
                    mism.append(
                        f"{ix} step master={step:g} vs table={TABLE_STEPS[ix]:g}")
            detail = (f"cached today, {len(m.options)} options, "
                      f"{len(by_ix)} indices")
            if mism:
                detail += " — MISMATCH (master wins): " + "; ".join(mism)
            out.append((WARN if mism else PASS, "instrument master", detail))
    except Exception as e:  # noqa: BLE001
        out.append((WARN, "instrument master", f"master cache error: {e}"))

    # 4. journal DB: exists, WAL, sessions
    dbp = data_dir / "journal.sqlite"
    if dbp.exists():
        db = sqlite3.connect(str(dbp))
        mode = db.execute("PRAGMA journal_mode").fetchone()[0]
        out.append((PASS if mode == "wal" else WARN, "journal WAL mode",
                    f"journal_mode={mode}"))
        try:
            n = db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            out.append((PASS if n else WARN, "journal sessions", f"{n} recorded"))
            # export smoke test (same SQL the JournalAgent export uses)
            import csv
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False,
                                             mode="w") as tf:
                w = csv.writer(tf)
                rows = db.execute(
                    "SELECT seq, ts, topic, agent, detail FROM events "
                    "ORDER BY seq").fetchall()
                w.writerow(["seq", "ts", "topic", "agent", "detail"])
                w.writerows(rows)
                tmpname = tf.name
            ok = Path(tmpname).stat().st_size > 0
            Path(tmpname).unlink(missing_ok=True)
            out.append((PASS if ok else FAIL, "journal CSV export",
                        "export produced rows" if ok else "empty export"))
        except Exception as e:  # noqa: BLE001
            out.append((FAIL, "journal integrity", str(e)))
        db.close()
    else:
        out.append((WARN, "journal DB", "not created yet — run SCAPLER once"))

    # 5. shadow sessions clean (≥3) — plan §12 Phase 7 gate
    try:
        from shadow_report import summarize
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from shadow_report import summarize
    if dbp.exists():
        rep = summarize(dbp)
        n = rep["clean_sessions"]
        out.append((PASS if n >= 3 else FAIL, "clean shadow sessions",
                    f"{n}/3 clean (see tools/shadow_report.py)"))
    else:
        out.append((FAIL, "clean shadow sessions", "no journal — run shadow first"))

    # 6. windows-only power check
    if sys.platform == "win32":
        try:
            import subprocess
            r = subprocess.run(["powercfg", "/a"], capture_output=True,
                               text=True, timeout=10)
            standby_on = "Standby (S3)" in r.stdout or \
                "Standby (S1)" in r.stdout
            out.append((WARN if standby_on else PASS, "windows sleep states",
                        "sleep states available — disable sleep on AC"
                        if standby_on else "no active sleep states"))
        except Exception as e:  # noqa: BLE001
            out.append((SKIP, "windows sleep states", f"powercfg failed: {e}"))
    else:
        out.append((SKIP, "windows sleep states",
                    "run on the trading PC: powercfg /a; disable sleep + "
                    "screen-off on AC; UPS recommended"))

    # 7. manual sign-off items (always printed)
    for item, how in (
        ("feed latency < 50 ms", "watch the status-bar `tick→signal p95/p99` "
         "during a shadow session"),
        ("kill-switch drill", "press KILL mid-session (flat): position squares "
         "off, entries halt; then RE-ARM"),
        ("square-off at 15:20 IST", "confirmed by watchdog log during shadow"),
        ("1-lot first live policy", "go live with lot multiplier 1, single "
         "trade, then scale only after review"),
    ):
        out.append((SKIP, item, f"MANUAL: {how}"))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--settings")
    ap.add_argument("--data-dir")
    a = ap.parse_args()
    res = run_checks(a.settings, a.data_dir)
    fails = sum(1 for r in res if r[0] == FAIL)
    print("GO-LIVE CHECKLIST")
    for st, item, detail in res:
        print(f"  [{st:>4}] {item:<26} {detail}")
    skips = [r for r in res if r[0] == SKIP]
    if fails:
        print(f"\nVERDICT: NO-GO — {fails} failing item(s).")
        return 1
    print(f"\nVERDICT: automated checks PASS — {len(skips)} manual items "
          "need sign-off before the first LIVE 1-lot session.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
