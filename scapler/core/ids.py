"""Client order ids — double as Groww order_reference_id (8-20 alnum, ≤2 '-')."""
from __future__ import annotations

import itertools
from datetime import datetime

from .clock import ist_now

_seq = itertools.count(1)


def new_client_id(now: datetime | None = None) -> str:
    now = now or ist_now()
    return f"SCP-{now:%y%m%d}-{next(_seq):04d}"
