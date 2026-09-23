"""Instrument-master cache — memory + disk (user directive, Phase 6).

Instant index switching must never wait on a network master download:
  1. memory : adapter.master (already parsed this session)
  2. disk   : {data_dir}/master-{broker}-{yyyy-mm-dd}.json — same-day file
              is authoritative (exchange masters change daily)
  3. network: adapter.load_master() → parse → persist to disk

JSON keeps it dependency-free; a master is ~20-60k rows, loads in <1 s.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from ..core.messages import InstrumentKey, OptionType
from .base import InstrumentMeta, MasterTable

log = logging.getLogger(__name__)


def _master_to_json(m: MasterTable) -> dict:
    return {
        "source": m.source,
        "indices": dict(m.indices),
        "options": [
            {"exchange": ik.exchange, "token": ik.token, "symbol": ik.symbol,
             "strike": ik.strike, "expiry": ik.expiry,
             "option_type": ik.option_type.value if ik.option_type else None,
             "lot_size": meta.lot_size, "index_symbol": meta.index_symbol,
             "tick_size": getattr(meta, "tick_size", 0.05)}
            for ik, meta in ((v.instrument, v) for v in m.options.values())
        ],
    }


def _master_from_json(d: dict) -> MasterTable:
    options = {}
    for o in d["options"]:
        ik = InstrumentKey(
            exchange=o["exchange"], token=o["token"], symbol=o["symbol"],
            strike=o["strike"], expiry=o["expiry"],
            option_type=OptionType[o["option_type"]] if o["option_type"]
            else None)
        options[ik.feed_key] = InstrumentMeta(
            instrument=ik, lot_size=o["lot_size"],
            index_symbol=o["index_symbol"], expiry=o["expiry"])
    return MasterTable(options=options, indices=dict(d["indices"]),
                       source=d.get("source", "cache"))


def cache_path(data_dir: str | Path, broker: str,
               today: str | None = None) -> Path:
    today = today or date.today().isoformat()
    return Path(data_dir).expanduser() / f"master-{broker}-{today}.json"


def load_disk(data_dir: str | Path, broker: str,
              today: str | None = None) -> MasterTable | None:
    p = cache_path(data_dir, broker, today)
    if not p.exists():
        return None
    try:
        m = _master_from_json(json.loads(p.read_text(encoding="utf-8")))
        log.info("master cache: loaded %s (%d options) from disk",
                 p.name, len(m.options))
        return m
    except Exception as e:
        log.warning("master cache: %s unreadable (%s) — refetching", p, e)
        return None


def save_disk(m: MasterTable, data_dir: str | Path, broker: str,
              today: str | None = None) -> Path:
    p = cache_path(data_dir, broker, today)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(_master_to_json(m)), encoding="utf-8")
    # prune stale day-files (keep 3 most recent)
    for old in sorted(p.parent.glob(f"master-{broker}-*.json"))[:-3]:
        try:
            old.unlink()
        except OSError:
            pass
    return p


async def get_master(adapter, data_dir: str | Path,
                     today: str | None = None) -> MasterTable:
    """Memory → disk → network, in that order. Sets adapter.master."""
    today = today or date.today().isoformat()
    if getattr(adapter, "master", None) is not None:
        return adapter.master
    cached = load_disk(data_dir, getattr(adapter, "name", "broker"), today)
    if cached is not None:
        adapter.master = cached
        return cached
    m = await adapter.load_master(today)
    try:
        save_disk(m, data_dir, getattr(adapter, "name", "broker"), today)
    except OSError as e:
        log.warning("master cache: cannot persist (%s) — memory only", e)
    return m
