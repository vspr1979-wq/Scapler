"""Groww instrument.csv parsing (documented column order)."""
from scapler.brokers.groww.master import parse_master
from scapler.core.messages import OptionType as OT

HDR = ("exchange,exchange_token,trading_symbol,groww_symbol,name,instrument_type,"
       "segment,series,isin,underlying_symbol,underlying_exchange_token,"
       "expiry_date,strike_price,lot_size,tick_size,freeze_quantity,"
       "is_reserved,buy_allowed,sell_allowed\n")


def opt(token, sym, underlying, itype, expiry, strike, lot=30, exch="NSE"):
    return (f"{exch},{token},{sym},{exch}-{underlying}-{itype},NaN,{itype},FNO,"
            f"NaN,NaN,{underlying},26009,{expiry},{strike},{lot},0.05,601,0,1,1\n")


CSV = (
    HDR
    + opt("1", "NIFTY24SEP26C24500", "NIFTY", "CE", "2026-09-24", 24500, 65)
    + opt("2", "NIFTY24SEP26P24500", "NIFTY", "PE", "2026-09-24", 24500, 65)
    + opt("3", "NIFTY17SEP26C24500", "NIFTY", "CE", "2026-09-17", 24500, 65)
    + opt("5", "BANKNIFTY29SEP26C51200", "BANKNIFTY", "CE", "2026-09-29", 51200)
    + opt("6", "BANKNIFTY29SEP26P51200", "BANKNIFTY", "PE", "2026-09-29", 51200)
    + opt("7", "BANKNIFTY29SEP26C51300", "BANKNIFTY", "CE", "2026-09-29", 51300)
    + opt("8", "SENSEX29SEP26C80100", "SENSEX", "CE", "2026-09-29", 80100, 20, "BSE")
    + opt("9", "RELIANCE29SEP26C1400", "RELIANCE", "CE", "2026-09-29", 1400)
    + "NSE,10,BANKNIFTY29SEP26C51400,x,NaN,CE,FNO,NaN,NaN,BANKNIFTY,26009,"
      "2026-09-29,51400,30,0.05,601,0,0,1\n"        # buy_allowed=0 → dropped
    + "NSE,NIFTY,NIFTY,NIFTY,NaN,INDEX,CASH,NaN,NaN,,,0,0,0,0.05,0,0,1,1\n"
    + "BSE,1,SENSEX,SENSEX,NaN,INDEX,CASH,NaN,NaN,,,0,0,0,0.05,0,0,1,1\n"
)
TODAY = "2026-09-22"


def test_options_parsed_and_expired_dropped():
    m = parse_master(CSV.encode(), TODAY)
    assert m.nearest_expiry("NIFTY", TODAY) == "2026-09-24"
    assert "2026-09-17" not in m.expiries("NIFTY")
    assert not any("RELIANCE" in k for k in m.options)
    assert not any("51400" in k for k in m.options)      # buy_allowed=0


def test_lots_strikes_sides():
    m = parse_master(CSV.encode(), TODAY)
    assert m.lot("NIFTY", "2026-09-24") == 65
    assert m.lot("BANKNIFTY", "2026-09-29") == 30
    assert m.lot("SENSEX", "2026-09-29") == 20
    assert m.strikes("BANKNIFTY", "2026-09-29", OT.CE) == [51200.0, 51300.0]
    meta = m.meta("BANKNIFTY", "2026-09-29", 51200.0, OT.CE)
    assert meta.instrument.symbol == "BANKNIFTY29SEP26C51200"
    assert meta.instrument.token == "5"
    assert meta.instrument.exchange == "NSE_FO"
    assert meta.instrument.feed_key == "NSE_FO|5"


def test_index_keys():
    m = parse_master(CSV.encode(), TODAY)
    assert m.indices["NIFTY"] == "NSE_INDEX|NIFTY"
    assert m.indices["SENSEX"] == "BSE_INDEX|1"
