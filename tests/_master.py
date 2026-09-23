"""Synthetic MasterTable helper for agent tests."""
from __future__ import annotations

from scapler.brokers.base import InstrumentMeta, MasterTable
from scapler.core.messages import InstrumentKey, OptionType


def make_master(index="BANKNIFTY", expiry="2026-09-29", lot=30,
                center=51200.0, step=100.0, exch="NSE_FO",
                radius=5) -> MasterTable:
    options = {}
    y, m, d = expiry[2:4], expiry[5:7], expiry[8:10]
    for i in range(-radius, radius + 1):
        strike = center + i * step
        for opt in OptionType:
            c = "C" if opt is OptionType.CE else "P"
            ik = InstrumentKey(exchange=exch, token=f"{int(strike)}{c}",
                               symbol=f"{index}{y}{m}{d}{c}{int(strike)}",
                               strike=strike, expiry=expiry, option_type=opt)
            options[ik.feed_key] = InstrumentMeta(
                instrument=ik, lot_size=lot, index_symbol=index,
                expiry=expiry)
    indices = {index: f"{exch.split('_')[0]}_INDEX|{index}"}
    return MasterTable(options=options, indices=indices, source="test")


def spot_key(index="BANKNIFTY", exch="NSE") -> str:
    return f"{exch}_INDEX|{index}"


def opt_key(strike: int, opt: str, exch="NSE_FO") -> str:
    return f"{exch}|{int(strike)}{opt}"
