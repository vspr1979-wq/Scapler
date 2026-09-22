"""SHADOW session report — read the journal DB, judge session cleanliness.

Usage:  python tools/shadow_report.py [path/to/journal.sqlite]

Summarizes every session: mode (SHADOW/LIVE/demo), signal counts, virtual
orders/fills, rejections (SideGuard SG_* are hard failures), veto histogram,
P&L by exit reason, fill-latency percentiles. "Clean session" (plan §12):
no SG_* rejections, no ORDER_REJECTED, at least one evaluated signal.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


_IST = timezone(timedelta(hours=5, minutes=30))
_END = "99:99:99.999"


def _ist_stamp(iso: str | None) -> str:
    """sessions.*_at are UTC ISO; events.ts are IST HH:MM:SS.mmm."""
    if not iso:
        return _END
    dt = datetime.fromisoformat(iso).astimezone(_IST)
    return f"{dt.strftime('%H:%M:%S.')}{dt.microsecond // 1000:03d}"


def summarize(db_path: str | Path) -> dict:
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    out: dict = {"db": str(db_path), "sessions": [], "clean_sessions": 0}
    sessions = db.execute("SELECT * FROM sessions ORDER BY id").fetchall()
    for s in sessions:
        sid = s["id"]
        # events belonging to this session = between its start and the next
        nxt = db.execute("SELECT MIN(started_at) m FROM sessions WHERE id>?",
                         (sid,)).fetchone()["m"]
        lo = _ist_stamp(s["started_at"])
        hi = _ist_stamp(nxt)

        def rows(topic: str, like: str | None = None) -> int:
            # Phase-7 rows carry sess; older rows fall back to the IST
            # wall-clock window [lo, hi)
            q = ("SELECT COUNT(*) n FROM events WHERE topic=? "
                 "AND (sess=? OR (sess IS NULL AND ts>=?")
            args = [topic, sid, lo]
            if nxt:
                q += " AND ts<?"
                args.append(hi)
            q += "))"
            if like:
                q += " AND detail LIKE ?"
                args.append(like)
            return db.execute(q, args).fetchone()["n"]

        sg = rows("order.rejected", "%SG_%")
        signals = rows("signal.new")
        sess = {
            "id": sid, "started": s["started_at"], "ended": s["ended_at"],
            "mode": s["mode"], "demo": bool(s["demo"]), "broker": s["broker"],
            "signals": signals, "orders": rows("order.req"),
            "fills": rows("order.fill"), "vetoes": rows("risk.veto"),
            "sideguard_rejects": sg,
            "clean": (sg == 0 and rows("order.rejected") == 0
                      and signals >= 1 and not s["demo"]),
        }
        out["sessions"].append(sess)
        if sess["clean"]:
            out["clean_sessions"] += 1
    # aggregate trade stats (whole DB)
    out["pnl_by_reason"] = {
        r["exit_reason"]: {"n": r["n"], "pnl": round(r["pnl"], 2)}
        for r in db.execute(
            "SELECT exit_reason, COUNT(*) n, SUM(realized_pnl) pnl "
            "FROM positions WHERE closed=1 GROUP BY exit_reason")}
    lat = [r["latency_ms"] for r in db.execute(
        "SELECT latency_ms FROM fills ORDER BY latency_ms")]
    if lat:
        out["fill_latency_ms"] = {
            "p50": round(lat[len(lat) // 2], 1),
            "p99": round(lat[min(len(lat) - 1, int(len(lat) * 0.99))], 1)}
    out["veto_histogram"] = {
        r["code"]: r["n"] for r in db.execute(
            "SELECT code, COUNT(*) n FROM risk_veto GROUP BY code")}
    db.close()
    out["go_live_ready"] = out["clean_sessions"] >= 3
    return out


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else \
        str(Path.home() / ".scapler" / "journal.sqlite")
    if not Path(path).exists():
        print(f"no journal at {path} — run SCAPLER first")
        return 2
    rep = summarize(path)
    print(f"SHADOW REPORT — {rep['db']}")
    for s in rep["sessions"]:
        tag = "CLEAN" if s["clean"] else ("demo" if s["demo"] else "dirty")
        print(f"  session {s['id']:>3} [{tag:>5}] {s['mode']:<14} "
              f"{s['started']} → {s['ended'] or 'open'}  "
              f"signals {s['signals']:>3} orders {s['orders']:>3} "
              f"fills {s['fills']:>3} vetoes {s['vetoes']:>2} "
              f"SG-rejects {s['sideguard_rejects']}")
    print(f"  clean live/shadow sessions: {rep['clean_sessions']} "
          f"(go-live needs ≥3) → "
          f"{'READY for go-live checklist' if rep['go_live_ready'] else 'NOT READY'}")
    print(f"  P&L by exit reason: {rep['pnl_by_reason']}")
    print(f"  fill latency: {rep.get('fill_latency_ms', '—')}")
    print(f"  vetoes: {rep['veto_histogram']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
