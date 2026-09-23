"""Upstox instrument-master CSV parsing → canonical MasterTable.

Columns tolerated (header names lowercased): token, segment, symbol,
instrument, name, expiry, strikeprice, lotsize. Option type is parsed from
the contract symbol suffix (…C24500 / …P24500). Expiry formats: DD-Mon-YYYY,
YYYY-MM-DD, DD-MM-YYYY. Expired rows are dropped against ``today``.
"""
from __future__ import annotations

import csv
import gzip
import io
import re
from datetime import datetime

from ...core.messages import InstrumentKey, OptionType
from ..base import InstrumentMeta, MasterTable

# canonical index → upstox index-master symbol
INDEX_SYMBOLS = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "Nifty Bank",
    "SENSEX": "SENSEX",
    "FINNIFTY": "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MIDCAP SELECT",
}
_INDEX_LOOKUP = {v.lower(): k for k, v in INDEX_SYMBOLS.items()}

_OPT_RE = re.compile(r"(C|P)(\d+(?:\.\d+)?)$")
_EXPIRY_FMTS = ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y")


def _parse_expiry(raw: str) -> str | None:
    raw = raw.strip()
    for fmt in _EXPIRY_FMTS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _unzip(body: bytes) -> str:
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body.decode("utf-8", errors="replace")


def parse_master(blobs: dict[str, bytes], today: str) -> MasterTable:
    options: dict[str, InstrumentMeta] = {}
    indices: dict[str, str] = {}
    for _ex, body in blobs.items():
        reader = csv.DictReader(io.StringIO(_unzip(body)))
        reader.fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]
        for row in reader:
            get = lambda k: (row.get(k) or "").strip()   # noqa: E731
            segment = get("segment")
            if segment in ("NSE_INDEX", "BSE_INDEX"):
                sym = get("symbol")
                canon = _INDEX_LOOKUP.get(sym.lower())
                if canon:
                    indices[canon] = f"{segment}|{get('token') or sym}"
                continue
            if segment not in ("NSE_FO", "BSE_FO") or get("instrument") != "OPTIDX":
                continue
            name = get("name").upper()
            if name not in INDEX_SYMBOLS:
                continue
            expiry = _parse_expiry(get("expiry"))
            if expiry is None or expiry < today:
                continue
            m = _OPT_RE.search(get("symbol"))
            if not m:
                continue
            opt = OptionType.CE if m.group(1) == "C" else OptionType.PE
            try:
                strike = float(get("strikeprice"))
                lot = int(float(get("lotsize")))
                token = get("token")
            except ValueError:
                continue
            if not token:
                continue
            ik = InstrumentKey(exchange=segment, token=token,
                               symbol=get("symbol"), strike=strike,
                               expiry=expiry, option_type=opt)
            options[ik.feed_key] = InstrumentMeta(
                instrument=ik, lot_size=lot, index_symbol=name, expiry=expiry)
    return MasterTable(options=options, indices=indices,
                       source="upstox-master")
