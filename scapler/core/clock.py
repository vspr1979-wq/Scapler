"""Clocks.

Hot path uses ``mono_ns`` only (no wall-clock syscalls, no formatting).
Wall/IST helpers are for the journal, UI and session guards (off hot path).
"""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

mono_ns = time.monotonic_ns


def utc_iso() -> str:
    return datetime.now(ZoneInfo("UTC")).isoformat(timespec="milliseconds")


def ist_now() -> datetime:
    return datetime.now(IST)


def ist_hhmm(dt: datetime | None = None) -> str:
    dt = dt or ist_now()
    return dt.strftime("%H:%M")


def in_window(hhmm: str, lo: str, hi: str) -> bool:
    """Inclusive HH:MM window check (IST strings, lexicographic-safe zero-padded)."""
    return lo <= hhmm <= hi
