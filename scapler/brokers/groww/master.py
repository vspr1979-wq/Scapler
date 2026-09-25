"""Groww instrument.csv parsing → canonical MasterTable.

Documented columns: exchange, exchange_token, trading_symbol, groww_symbol,
name, instrument_type (CE/PE/…), segment (CASH/FNO), series, isin,
underlying_symbol, underlying_exchange_token, expiry_date (ISO), strike_price,
lot_size, tick_size, freeze_quantity, is_reserved, buy_allowed, sell_allowed.

Index rows: segment CASH with a canonical index trading symbol (feed docs use
exchange_token "NIFTY" / "1" for indices). Expired rows dropped vs ``today``.
"""
from __future__ import annotations

import csv
import io

from ...core.clock import ist_now
from ...core.messages import InstrumentKey, OptionType
from ..base import InstrumentMeta, MasterTable
from ..upstox.master import INDEX_SYMBOLS   # canonical names shared

_INDEX_SET = set(INDEX_SYMBOLS)


def parse_master(body: bytes, today: str) -> MasterTable:
    text = body.decode("utf-8", errors="replace")
    options: dict[str, InstrumentMeta] = {}
    indices: dict[str, str] = {}
    reader = csv.DictReader(io.StringIO(text))
    reader.fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]
    for row in reader:
        get = lambda k: (row.get(k) or "").strip()   # noqa: E731
        segment = get("segment")
        symbol = get("trading_symbol")
        if segment == "CASH":
            if symbol in _INDEX_SET:
                # canonical index key shape, same as Upstox: {EX}_INDEX|token
                indices[symbol] = f"{get('exchange')}_INDEX|{get('exchange_token') or symbol}"
            continue
        if segment != "FNO":
            continue
        underlying = get("underlying_symbol")
        itype = get("instrument_type")
        if underlying not in _INDEX_SET or itype not in ("CE", "PE"):
            continue
        expiry = get("expiry_date")                  # ISO yyyy-mm-dd
        if not expiry or expiry < today:
            continue
        # Drop same-day expiry after 15:30 IST (market closed)
        if expiry == today:
            now_ist = ist_now()
            if now_ist.hour >= 15 and now_ist.minute >= 30:
                continue
        try:
            strike = float(get("strike_price"))
            lot = int(float(get("lot_size")))
            token = get("exchange_token")
        except ValueError:
            continue
        if not token or get("buy_allowed") == "0":
            continue
        # canonical exchange carries the segment, exactly like Upstox
        # ("NSE_FO" / "BSE_FO") → feed_key shape is identical across brokers
        ik = InstrumentKey(exchange=f"{get('exchange')}_FO", token=token,
                           symbol=symbol, strike=strike, expiry=expiry,
                           option_type=OptionType.CE if itype == "CE"
                           else OptionType.PE)
        options[ik.feed_key] = InstrumentMeta(
            instrument=ik, lot_size=lot, index_symbol=underlying, expiry=expiry)
    return MasterTable(options=options, indices=indices, source="groww-master")
