"""Master CSV parsing: nearest expiry (never hardcoded weekday), lots,
OPTIDX filter, per-side contracts, index feed keys."""
import gzip

from scapler.brokers.upstox.master import INDEX_SYMBOLS, parse_master

HDR = "token,segment,symbol,instrument,name,expiry,strikeprice,lotsize\n"


def _fo(token, sym, name, expiry, strike, lot=30, seg="NSE_FO"):
    return f"{token},{seg},{sym},OPTIDX,{name},{expiry},{strike},{lot}\n"


def _idx(token, sym, seg="NSE_INDEX"):
    return f"{token},{seg},{sym},INDEX,{sym},,,\n"


CSV = (
    HDR
    + _fo("1", "NIFTY24SEP26C24500", "NIFTY", "24-Sep-2026", 24500, 65)
    + _fo("2", "NIFTY24SEP26P24500", "NIFTY", "24-Sep-2026", 24500, 65)
    + _fo("3", "NIFTY01OCT26C24500", "NIFTY", "01-Oct-2026", 24500, 65)
    + _fo("4", "NIFTY17SEP26C24500", "NIFTY", "17-Sep-2026", 24500, 65)   # expired
    + _fo("5", "BANKNIFTY29SEP26C51200", "BANKNIFTY", "29-Sep-2026", 51200)
    + _fo("6", "BANKNIFTY29SEP26P51200", "BANKNIFTY", "29-Sep-2026", 51200)
    + _fo("7", "BANKNIFTY29SEP26C51300", "BANKNIFTY", "29-Sep-2026", 51300)
    + _fo("8", "SENSEX29SEP26C80100", "SENSEX", "29-Sep-2026", 80100, 20, "BSE_FO")
    + _fo("9", "RELIANCE29SEP26C1400", "RELIANCE", "29-Sep-2026", 1400)   # stock opt
    + _idx("101", "NIFTY 50")
    + _idx("102", "Nifty Bank")
    + _idx("103", "SENSEX", "BSE_INDEX")
)
TODAY = "2026-09-22"


def test_parse_gz_and_plain():
    m = parse_master({"NSE_FO": gzip.compress(CSV.encode())}, TODAY)
    assert m.options
    m2 = parse_master({"NSE_FO": CSV.encode()}, TODAY)
    assert len(m2.options) == len(m.options)


def test_expired_rows_dropped_nearest_expiry_is_live():
    m = parse_master({"ALL": CSV.encode()}, TODAY)
    assert m.nearest_expiry("NIFTY", TODAY) == "2026-09-24"   # 17-Sep dropped
    assert "2026-09-17" not in m.expiries("NIFTY")


def test_stock_options_excluded_index_options_only():
    m = parse_master({"ALL": CSV.encode()}, TODAY)
    assert all(meta.index_symbol in INDEX_SYMBOLS for meta in m.options.values())
    assert not any("RELIANCE" in k for k in m.options)


def test_lots_and_strikes_and_sides():
    m = parse_master({"ALL": CSV.encode()}, TODAY)
    assert m.lot("NIFTY", "2026-09-24") == 65
    assert m.lot("BANKNIFTY", "2026-09-29") == 30
    assert m.lot("SENSEX", "2026-09-29") == 20
    from scapler.core.messages import OptionType as OT
    assert m.strikes("BANKNIFTY", "2026-09-29", OT.CE) == [51200.0, 51300.0]
    assert m.strikes("BANKNIFTY", "2026-09-29", OT.PE) == [51200.0]
    meta = m.meta("BANKNIFTY", "2026-09-29", 51200.0, OT.CE)
    assert meta.instrument.token == "5"
    assert meta.instrument.exchange == "NSE_FO"
    assert m.meta("BANKNIFTY", "2026-09-29", 51200.0, OT.PE).instrument.token == "6"


def test_index_feed_keys():
    m = parse_master({"ALL": CSV.encode()}, TODAY)
    assert m.indices["NIFTY"] == "NSE_INDEX|101"
    assert m.indices["BANKNIFTY"] == "NSE_INDEX|102"
    assert m.indices["SENSEX"] == "BSE_INDEX|103"


def test_expiry_format_tolerance():
    csv2 = HDR + _fo("1", "NIFTY24SEP26C24500", "NIFTY", "2026-09-24", 24500, 65)
    m = parse_master({"X": csv2.encode()}, TODAY)
    assert m.nearest_expiry("NIFTY", TODAY) == "2026-09-24"
